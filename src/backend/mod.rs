use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::backend::options::{
    MarkDoneOptions, MarkFailureOptions, MarkRetryOptions, MarkRevokeOptions, MarkStartOptions,
    StoreOptions,
};
use crate::config::BackendConfig;
use crate::error::{BackendError, TaskError, TraceError};
use crate::kombu_serde::SerializerKind;
use crate::states::State;
use crate::task::{Request, Task};

pub use self::disabled::{DisabledBackend, DisabledBackendBuilder};
pub use self::mongodb::{MongoDbBackend, MongoDbBackendBuilder};
pub use self::redis::{RedisBackend, RedisBackendBuilder};
pub use self::task_meta::TaskMeta;

mod disabled;
mod inner;
mod mongodb;
pub mod options;
mod redis;
mod task_meta;

type Exc = TraceError;
type TaskId = String;
type Traceback = ();

pub type TaskResult<D> = Result<D, Exc>;
pub type GetTaskResult<D> = Result<TaskResult<D>, BackendError>;

pub struct BackendBasic {
    pub url: String,
    pub result_serializer: SerializerKind,
    pub expiration_in_seconds: Option<u32>,
    pub cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

impl BackendBasic {
    pub fn new(backend_url: &str) -> BackendBasic {
        Self {
            url: backend_url.to_owned(),
            result_serializer: SerializerKind::JSON,
            expiration_in_seconds: None,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        }
    }
}

#[async_trait]
pub trait BackendBuilder: Sized {
    type Backend: Backend;

    fn new(backend_url: &str) -> Self;

    fn backend_basic(&mut self) -> &mut BackendBasic;

    fn config(mut self, config: BackendConfig) -> Self {
        self.backend_basic().result_serializer = config.result_serializer;
        self.backend_basic().expiration_in_seconds =
            config.result_expires.map(|d| d.num_seconds() as u32);
        self.backend_basic().url = config.url;
        self
    }

    async fn build(self) -> Result<Self::Backend, BackendError>;
}

#[async_trait]
pub trait Backend: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    fn safe_url(&self) -> String;

    async fn get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta;

    fn recover_result_by_meta<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>;

    async fn wait_for(
        &self,
        task_id: &TaskId,
        timeout: Option<std::time::Duration>,
        interval: Option<std::time::Duration>,
    ) -> Result<TaskMeta, BackendError> {
        let interval = interval.unwrap_or(std::time::Duration::from_millis(500));

        let timeout =
            timeout.map(|timeout| core::time::Duration::new(0, timeout.as_nanos() as u32));
        let interval = core::time::Duration::new(0, interval.as_nanos() as u32);

        self.ensure_not_eager();

        let start_timing = std::time::SystemTime::now();
        loop {
            let task_meta = self.get_task_meta(task_id, true).await;
            if task_meta.is_ready() {
                return Ok(task_meta);
            }

            tokio::time::sleep(interval).await;
            if let Some(timeout) = timeout {
                if std::time::SystemTime::now() > start_timing + timeout {
                    return Err(BackendError::TimeoutError);
                }
            }
        }
    }

    async fn mark_as_started<'s, T>(&'s self, task_id: &TaskId, option: MarkStartOptions<'s, T>)
    where
        T: Task,
    {
        let data = Ok(option.meta);
        self.store_result(task_id, data, option.status, &option.store)
            .await
    }

    async fn mark_as_done<'s, 'r, T>(&'s self, task_id: &TaskId, option: MarkDoneOptions<'s, 'r, T>)
    where
        T: Task,
    {
        let data = Ok(option.result);

        if option.store_result
            && !option
                .store
                .request
                .map(|r| r.ignore_result)
                .unwrap_or(false)
        {
            self.store_result(task_id, data.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, option.status, data)
                    .await
            }
        }
    }

    async fn mark_as_failure<'s, T>(&'s self, task_id: &TaskId, option: MarkFailureOptions<'s, T>)
    where
        T: Task,
    {
        let err = TaskResult::<()>::Err(option.exc);

        if option.store_result {
            self.store_result(task_id, err.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, option.status, err).await
            }

            // todo: chain elem ctx -> store_result
            if option.call_errbacks {
                // todo: call err callbacks
            }
        }
    }

    async fn mark_as_revoked<'s, T>(&'s self, task_id: &TaskId, option: MarkRevokeOptions<'s, T>)
    where
        T: Task,
    {
        let exc = TraceError::TaskError(TaskError::RevokedError(option.reason));
        let err = TaskResult::<()>::Err(exc);

        if option.store_result {
            self.store_result(task_id, err.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, option.status, err).await
            }
        }
    }

    async fn mark_as_retry<'s, T>(&'s self, task_id: &TaskId, option: MarkRetryOptions<'s, T>)
    where
        T: Task,
    {
        let err = TaskResult::<()>::Err(option.exc);

        self.store_result(task_id, err, option.status, &option.store)
            .await;
    }

    async fn forget(&self, task_id: &TaskId);

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    #[allow(unused)]
    async fn on_chord_part_return<T, D>(
        &self,
        request: &Request<T>,
        state: State,
        result: TaskResult<D>,
    ) where
        T: Task,
        D: Serialize + Send + Sync,
    {
        // no-op
    }

    fn ensure_not_eager(&self) {
        // no-op
    }

    async fn cleanup(&self) {
        // no-op
    }

    async fn process_cleanup(&self) {
        // no-op
    }

    // todo: chord related apis
    //   fn add_to_chord(&self, chord_id, result)
    //   fn on_chord_part_return(&self, request, state, result, kwargs)
    //   fn set_chord_size(&self, group_id, chord_size)
    //   fn fallback_chord_unlock(&self, header_result, body, countdown, kwargs)
    //   fn ensure_chords_allowed(&self)
    //   fn apply_chord(&self, header_result_args, body, kwargs)

    // todo: group related apis
    //   fn get_group_meta(&self, group_id, cache=True)
    //   fn restore_group(&self, group_id, cache=True)
    //   fn save_group(&self, group_id, result)
    //   fn delete_group(&self, group_id)
}
