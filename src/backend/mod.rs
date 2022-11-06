use std::collections::HashMap;

use crate::backend::common::basic_layer::BackendBasic;
use crate::config::BackendConfig;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::error::{BackendError, TaskError, TraceError};
use crate::states::State;
use crate::task::{Request, Task};

pub use self::common::basic_layer::BackendBasicLayer;
pub use self::common::protocol_layer::BackendProtocolLayer;
pub use self::common::serde_layer::BackendSerdeLayer;
pub use self::common::task_meta::TaskMeta;
pub use self::disabled::{DisabledBackend, DisabledBackendBuilder};
pub use self::mongodb::{MongoDbBackend, MongoDbBackendBuilder};
pub use self::redis::{RedisBackend, RedisBackendBuilder};

mod common;
mod disabled;
mod key_value_store;
mod mongodb;
mod redis;

type Exc = TraceError;
type TaskId = String;
type Traceback = ();

pub type TaskResult<D> = Result<D, Exc>;
pub type GetTaskResult<D> = Result<TaskResult<D>, BackendError>;

#[derive(Clone, Default)]
pub struct StoreOption<'request, T: Task> {
    traceback: Option<Traceback>,
    request: Option<&'request Request<T>>,
}

impl<'require, T: Task> StoreOption<'require, T> {
    pub fn with_request(request: &'require Request<T>) -> Self {
        Self {
            traceback: None,
            request: Some(request),
        }
    }
}

#[derive(TypedBuilder)]
pub struct MarkStartOption<'request, T: Task> {
    #[builder(default = State::STARTED)]
    status: State,
    meta: HashMap<String, String>,
    store: StoreOption<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkDoneOption<'returns, 'request, T: Task> {
    #[builder(default = State::SUCCESS)]
    status: State,
    result: &'returns T::Returns,
    #[builder(default = true)]
    store_result: bool,
    store: StoreOption<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkFailureOption<'request, T: Task> {
    #[builder(default = State::FAILURE)]
    status: State,
    exc: Exc,
    #[builder(default = false)]
    call_errbacks: bool,
    #[builder(default = true)]
    store_result: bool,
    store: StoreOption<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkRevokeOption<'request, T: Task> {
    #[builder(default = State::REVOKED)]
    status: State,
    reason: String,
    #[builder(default = true)]
    store_result: bool,
    store: StoreOption<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkRetryOption<'request, T: Task> {
    #[builder(default = State::RETRY)]
    status: State,
    exc: Exc,
    store: StoreOption<'request, T>,
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

    async fn mark_as_started<'s, T>(&'s self, task_id: &TaskId, option: MarkStartOption<'s, T>)
    where
        T: Task,
    {
        let data = Ok(option.meta);
        self.store_result(task_id, data, option.status, &option.store)
            .await
    }

    async fn mark_as_done<'s, 'r, T>(&'s self, task_id: &TaskId, option: MarkDoneOption<'s, 'r, T>)
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

    async fn mark_as_failure<'s, T>(&'s self, task_id: &TaskId, option: MarkFailureOption<'s, T>)
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

    async fn mark_as_revoked<'s, T>(&'s self, task_id: &TaskId, option: MarkRevokeOption<'s, T>)
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

    async fn mark_as_retry<'s, T>(&'s self, task_id: &TaskId, option: MarkRetryOption<'s, T>)
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
        store: &StoreOption<T>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kombu_serde::{AnyValue, SerializerKind};
    use chrono::{DateTime, Utc};

    #[test]
    fn test_serde_result_meta() {
        let serializer = SerializerKind::JSON;

        let result_meta = TaskMeta {
            task_id: "fake-id".to_owned(),
            status: State::SUCCESS,
            result: AnyValue::JSON(serde_json::to_value(11).unwrap()),
            traceback: None,
            children: vec![],
            date_done: Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339()),
            group_id: None,
            parent_id: None,
            name: None,
            args: None,
            kwargs: None,
            worker: Some("fake-hostname".to_owned()),
            retries: Some(10),
            queue: None,
        };

        let (_content_type, _encoding, data) = serializer.dump(&result_meta);
        let output_result_meta = serializer.load(&data);

        assert_eq!(result_meta, output_result_meta);
    }
}
