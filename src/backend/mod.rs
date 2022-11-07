use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::backend::options::{
    MarkDoneOptions, MarkFailureOptions, MarkRetryOptions, MarkRevokeOptions, MarkStartOptions,
    StoreOptions, WaitForOptions,
};
use crate::config::BackendConfig;
use crate::error::{BackendError, TraceError};
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
pub type BackendResult<D> = Result<D, BackendError>;
pub type GetTaskResult<D> = Result<TaskResult<D>, BackendError>;

pub struct BackendBasic {
    pub url: String,
    pub safe_url: String,
    pub result_serializer: SerializerKind,
    pub expiration_in_seconds: Option<u32>,
    pub cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

impl BackendBasic {
    pub fn new(backend_url: &str) -> BackendBasic {
        Self {
            url: backend_url.to_owned(),
            safe_url: backend_url.to_owned(),
            result_serializer: SerializerKind::JSON,
            expiration_in_seconds: None,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        }
    }
}

#[async_trait]
pub trait Backend: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    fn basic(&self) -> &BackendBasic;

    async fn wait_for(&self, task_id: &TaskId, option: WaitForOptions) -> BackendResult<TaskMeta>;

    async fn forget(&self, task_id: &TaskId);

    async fn mark_as_started<'s, T>(&self, task_id: &TaskId, option: MarkStartOptions<'s, T>)
    where
        T: Task;

    async fn mark_as_done<'s, 'r, T>(&self, task_id: &TaskId, option: MarkDoneOptions<'s, 'r, T>)
    where
        T: Task;

    async fn mark_as_failure<'s, T>(&self, task_id: &TaskId, option: MarkFailureOptions<'s, T>)
    where
        T: Task;

    async fn mark_as_revoked<'s, T>(&self, task_id: &TaskId, option: MarkRevokeOptions<'s, T>)
    where
        T: Task;

    async fn mark_as_retry<'s, T>(&self, task_id: &TaskId, option: MarkRetryOptions<'s, T>)
    where
        T: Task;

    async fn get_task_meta_by(&self, task_id: &TaskId, cache: bool) -> TaskMeta;

    fn recover_result<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>;

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    async fn on_chord_part_return<T, D>(
        &self,
        result: TaskResult<D>,
        status: State,
        request: &Request<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    fn ensure_not_eager(&self);

    async fn cleanup(&self);

    async fn process_cleanup(&self);

    // todo: chord related apis
    // fn add_to_chord(&self, chord_id, result)
    // fn on_chord_part_return(&self, request, state, result, kwargs)
    // fn set_chord_size(&self, group_id, chord_size)
    // fn fallback_chord_unlock(&self, header_result, body, countdown, kwargs)
    // fn ensure_chords_allowed(&self)
    // fn apply_chord(&self, header_result_args, body, kwargs)

    // todo: group related apis
    // fn get_group_meta(&self, group_id, cache=True)
    // fn restore_group(&self, group_id, cache=True)
    // fn save_group(&self, group_id, result)
    // fn delete_group(&self, group_id)
}

#[async_trait]
pub trait BackendBuilder: Sized {
    type Backend: Backend;

    fn new(backend_url: &str) -> Self;

    fn backend_basic(&mut self) -> &mut BackendBasic;

    fn parse_url(&self) -> Option<url::Url>;

    fn config(mut self, config: BackendConfig) -> Self {
        let safe_url = match self.parse_url() {
            Some(url) => format!(
                "{}://{}:***@{}:{}/{}",
                url.scheme(),
                url.username(),
                url.host_str().unwrap(),
                url.port().unwrap(),
                url.path(),
            ),
            None => {
                log::error!("Invalid redis url.");
                String::from("")
            }
        };

        self.backend_basic().url = config.url;
        self.backend_basic().safe_url = safe_url;
        self.backend_basic().result_serializer = config.result_serializer;
        self.backend_basic().expiration_in_seconds =
            config.result_expires.map(|d| d.num_seconds() as u32);
        self
    }

    async fn build(self) -> Result<Self::Backend, BackendError>;
}
