use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::backend::options::{
    MarkDoneOptions, MarkFailureOptions, MarkRetryOptions, MarkRevokeOptions, MarkStartOptions,
    StoreOptions, WaitOptions,
};
use crate::config::BackendConfig;
use crate::error::BackendError;
use crate::protocol::{ContentType, GroupMeta};
use crate::protocol::{ExecResult, State, TaskId, TaskMeta};
use crate::result::ResultStructure;
use crate::task::{Request, Task};

pub use self::disabled::{DisabledBackend, DisabledBackendBuilder};
pub use self::mongodb::{MongoDbBackend, MongoDbBackendBuilder};
pub use self::redis::{RedisBackend, RedisBackendBuilder};

mod disabled;
mod inner;
mod mongodb;
pub mod options;
mod redis;

pub type BackendResult<D> = Result<D, BackendError>;
pub type GetTaskResult<D> = BackendResult<ExecResult<D>>;

/// The basic part of a backend that is common for all backend implementations.
pub struct BackendBasic {
    /// The full url of the backend.
    pub url: String,

    /// A safe version url that hides user and password.
    pub safe_url: String,

    /// The [SerializerKind] that used to persistent [TaskMeta].
    pub result_serializer: ContentType,

    /// Expiration for a task result.
    pub result_expires: Option<chrono::Duration>,

    /// A table that caches only ready [TaskMeta].
    pub cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

impl BackendBasic {
    pub fn new(backend_url: &str) -> BackendBasic {
        Self {
            url: backend_url.to_owned(),
            safe_url: backend_url.to_owned(),
            result_serializer: ContentType::Json,
            result_expires: None,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        }
    }
}

/// A result [Backend] is used to tracing task status and the returned results.
#[async_trait]
pub trait Backend: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    fn basic(&self) -> &BackendBasic;

    /// Wait until the task is ready, and return the [TaskMeta].
    async fn wait(&self, task_id: &TaskId, option: WaitOptions) -> BackendResult<TaskMeta>;

    /// Forget the result for the given task.
    async fn forget(&self, task_id: &TaskId);

    /// Mark a task as started.
    async fn mark_as_started<'s, T>(&self, task_id: &TaskId, option: MarkStartOptions<'s, T>)
    where
        T: Task;

    /// Mark a task as successfully executed.
    async fn mark_as_done<'s, 'r, T>(&self, task_id: &TaskId, option: MarkDoneOptions<'s, 'r, T>)
    where
        T: Task;

    /// Mark a task as executed with failure.
    async fn mark_as_failure<'s, T>(&self, task_id: &TaskId, option: MarkFailureOptions<'s, T>)
    where
        T: Task;

    /// Mark a task as revoked.
    async fn mark_as_revoked<'s, T>(&self, task_id: &TaskId, option: MarkRevokeOptions<'s, T>)
    where
        T: Task;

    /// Mark a task as being retried.
    async fn mark_as_retry<'s, T>(&self, task_id: &TaskId, option: MarkRetryOptions<'s, T>)
    where
        T: Task;

    /// Get [TaskMeta] by task id.
    async fn get_task_meta_by(&self, task_id: &TaskId, cache: bool) -> TaskMeta;

    async fn save_group(&self, group_id: &str, group_structure: ResultStructure);

    async fn forget_group(&self, group_id: &str);

    async fn get_group_meta_by(&self, group_id: &str) -> GroupMeta;

    /// Store the task result to the [crate::backend::Backend].
    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: ExecResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    async fn on_chord_part_return<T, D>(
        &self,
        result: ExecResult<D>,
        status: State,
        request: &Request<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    fn ensure_not_eager(&self);

    /// Clean up the [Backend].
    ///
    /// If there were expired [TaskMeta]s, clean them up.
    async fn cleanup(&self);

    /// Clean up at the end of a task worker process.
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

    /// Return a mutable instance of [BackendBasic].
    fn backend_basic(&mut self) -> &mut BackendBasic;

    /// Parse valid backend url to [url::Url].
    fn parse_url(&self) -> Option<url::Url>;

    /// Configure according to a [BackendConfig].
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
        self.backend_basic().result_expires = config.result_expires;
        self
    }

    async fn build(self) -> Result<Self::Backend, BackendError>;
}
