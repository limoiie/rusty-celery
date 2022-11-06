use std::cell::RefCell;
use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::MutexGuard;

use crate::error::{BackendError, TaskError, TraceError};
use crate::kombu_serde::{AnyValue, SerializerKind};
use crate::states::State;
use crate::task::{Request, Task};

pub use self::disabled::{DisabledBackend, DisabledBackendBuilder};
pub use self::mongodb::{MongoDbBackend, MongoDbBackendBuilder};
pub use self::redis::{RedisBackend, RedisBackendBuilder};

mod disabled;
mod key_value_store;
mod mongodb;
mod redis;

type Exc = TraceError;
type TaskId = String;
type Traceback = ();
type Key = String;

pub type TaskResult<D> = Result<D, Exc>;
pub type GetTaskResult<D> = Result<TaskResult<D>, BackendError>;

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

    async fn mark_as_started<T: Task>(
        &self,
        task_id: &TaskId,
        meta: HashMap<String, String>,
        request: Option<&Request<T>>,
    ) {
        let data = Ok(meta);
        self.store_result_wrapped_as_task_meta(task_id, data, State::STARTED, None, request)
            .await
    }

    async fn mark_as_done<T: Task>(
        &self,
        task_id: &TaskId,
        result: &T::Returns,
        request: Option<&Request<T>>,
        store_result: bool,
        state: Option<State>,
    ) {
        let state = state.unwrap_or(State::SUCCESS);
        let data = Ok(result);

        if store_result && !request.map(|r| r.ignore_result).unwrap_or(false) {
            self.store_result_wrapped_as_task_meta(task_id, data.clone(), state, None, request)
                .await;
        }

        if let Some(request) = request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, state, data).await
            }
        }
    }

    async fn mark_as_failure<T: Task>(
        &self,
        task_id: &TaskId,
        exc: Exc,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
        store_result: bool,
        call_errbacks: bool,
        state: Option<State>,
    ) {
        let state = state.unwrap_or(State::FAILURE);
        let err = TaskResult::<()>::Err(exc);

        if store_result {
            self.store_result_wrapped_as_task_meta(task_id, err.clone(), state, traceback, request)
                .await;
        }

        if let Some(request) = request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, state, err).await
            }

            // todo: chain elem ctx -> store_result
            if call_errbacks {
                // todo: call err callbacks
            }
        }
    }

    async fn mark_as_revoked<T: Task>(
        &self,
        task_id: &TaskId,
        reason: String,
        request: Option<&Request<T>>,
        store_result: bool,
        state: Option<State>,
    ) {
        let exc = TraceError::TaskError(TaskError::RevokedError(reason));
        let err = TaskResult::<()>::Err(exc);
        let state = state.unwrap_or(State::REVOKED);

        if store_result {
            self.store_result_wrapped_as_task_meta(task_id, err.clone(), state, None, request)
                .await;
        }

        if let Some(request) = request {
            if request.chord.is_some() {
                self.on_chord_part_return(request, state, err).await
            }
        }
    }

    async fn mark_as_retry<T: Task>(
        &self,
        task_id: &TaskId,
        exc: Exc,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
        state: Option<State>,
    ) {
        let err = TaskResult::<()>::Err(exc);
        let state = state.unwrap_or(State::RETRY);

        self.store_result_wrapped_as_task_meta(task_id, err, state, traceback, request)
            .await;
    }

    async fn forget(&self, task_id: &TaskId);

    async fn store_result_wrapped_as_task_meta<D: Serialize + Send + Sync, T: Task>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        state: State,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
    );

    async fn on_chord_part_return<T: Task, D: Serialize + Send + Sync>(
        &self,
        _request: &Request<T>,
        _state: State,
        _result: TaskResult<D>,
    ) {
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
pub trait BackendBuilder {
    type Backend: Backend;

    fn new(backend_url: &str) -> Self;

    fn result_serializer(self, kind: SerializerKind) -> Self;

    fn result_expires(self, expiration: Option<Duration>) -> Self;

    async fn build(&self) -> Result<Self::Backend, BackendError>;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct GeneralError {
    exc_type: String,
    exc_message: String,
    exc_module: String,
}

pub trait BaseTranslator: Send + Sync + Sized {
    fn serializer(&self) -> SerializerKind;

    fn encode<D: Serialize>(&self, data: &D) -> String {
        self.serializer().dump(data).2
    }

    fn decode<D: for<'de> Deserialize<'de>>(&self, payload: String) -> D {
        self.serializer().load(&payload)
    }

    fn prepare_result<D: Serialize>(&self, result: TaskResult<D>, state: State) -> AnyValue {
        match result {
            Ok(ref data) => self.prepare_value(data),
            Err(ref err) if state.is_exception() => self.prepare_exception(err),
            Err(_) => todo!(),
        }
    }

    fn prepare_value<D: Serialize>(&self, result: &D) -> AnyValue {
        // todo
        //   if result is ResultBase {
        //       result.as_tuple()
        //   }
        self.serializer().data_to_value(result)
    }

    fn prepare_exception(&self, exc: &Exc) -> AnyValue {
        let (typ, msg, module) = match exc {
            Exc::TaskError(err) => match err {
                TaskError::ExpectedError(err) => ("TaskError", err.as_str(), "celery.exceptions"),
                TaskError::UnexpectedError(err) => ("TaskError", err.as_str(), "celery.exceptions"),
                TaskError::TimeoutError => ("TimeoutError", "", "celery.exceptions"),
                TaskError::Retry(_time) => {
                    ("RetryTaskError", "todo!" /*todo*/, "celery.exceptions")
                }
                TaskError::RevokedError(err) => ("RevokedError", err.as_str(), "celery.exceptions"),
            },
            Exc::ExpirationError => ("TaskError", "", "celery.exceptions"),
            Exc::Retry(_time) => ("RetryTaskError", "todo!", "celery.exceptions"),
        };

        let exc_struct = GeneralError {
            exc_type: typ.into(),
            exc_message: msg.into(),
            exc_module: module.into(),
        };

        self.serializer().data_to_value(&exc_struct)
    }

    fn recover_result<D>(&self, data: AnyValue, state: State) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        match state {
            _ if state.is_exception() => Some(Err(self.recover_exception(data))),
            _ if state.is_successful() => Some(Ok(self.recover_value(data))),
            _ => None,
        }
    }

    fn recover_value<D>(&self, data: AnyValue) -> D
    where
        D: for<'de> Deserialize<'de>,
    {
        self.serializer().value_to_data(data).unwrap()
    }

    fn recover_exception(&self, data: AnyValue) -> Exc {
        let err: GeneralError = self.serializer().value_to_data(data).unwrap();
        match err.exc_type.as_str() {
            "TaskError" => Exc::TaskError(TaskError::ExpectedError(err.exc_message)),
            "TimeoutError" => Exc::TaskError(TaskError::TimeoutError),
            "RetryTaskError" => Exc::TaskError(TaskError::Retry(None /*todo*/)),
            "RevokedError" => Exc::TaskError(TaskError::RevokedError(err.exc_message)),
            _ => Exc::TaskError(TaskError::UnexpectedError(err.exc_message)), // todo
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMeta {
    pub task_id: TaskId,
    pub status: State,
    pub result: AnyValue,
    pub traceback: Option<Traceback>,
    pub children: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    date_done: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,

    // extend request meta
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kwargs: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
}

impl TaskMeta {
    pub fn is_ready(&self) -> bool {
        self.status.is_ready()
    }

    pub fn is_exception(&self) -> bool {
        self.status.is_exception()
    }
}

#[async_trait]
pub trait BaseCached: Send + Sync + Sized {
    fn _safe_url(&self) -> String {
        match self.__parse_url() {
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
        }
    }

    fn __parse_url(&self) -> Option<url::Url>;

    fn expires_in_seconds(&self) -> Option<u32>;

    async fn cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>>;

    async fn get_cached(&self, key: &Key) -> Option<TaskMeta> {
        let guard = self.cached().await;
        let cached = guard.borrow();
        cached.get(key).map(Clone::clone)
    }

    async fn set_cached(&self, key: Key, val: TaskMeta) -> Option<TaskMeta> {
        let mut guard = self.cached().await;
        guard.get_mut().insert(key, val)
    }

    async fn del_cached(&self, key: &Key) -> Option<TaskMeta> {
        let mut guard = self.cached().await;
        guard.get_mut().remove(key)
    }

    async fn is_cached(&self, key: &Key) -> bool {
        let guard = self.cached().await;
        let cached = guard.borrow();
        cached.contains_key(key)
    }
}

#[async_trait]
pub trait BaseBackendProtocol: BaseCached + BaseTranslator {
    type Builder: BackendBuilder<Backend = Self>;

    async fn _store_result_wrapped_as_task_meta<T: Task>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        state: State,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
    );

    async fn __forget_task_meta_by(&self, task_id: &TaskId);

    async fn __fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta;

    fn __decode_task_meta(&self, payload: String) -> TaskMeta;

    async fn __get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.ensure_not_eager();
        if cache {
            if let Some(meta) = self.get_cached(task_id).await {
                return meta;
            }
        }

        let meta = self.__fetch_task_meta_by(task_id).await;
        if cache && meta.status == State::SUCCESS {
            self.set_cached(task_id.clone(), meta.clone()).await;
        }

        return meta;
    }

    async fn __make_task_meta<T: Task>(
        task_id: TaskId,
        result: AnyValue,
        status: State,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
    ) -> TaskMeta {
        let date_done = if status.is_ready() {
            Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339())
        } else {
            None
        };

        TaskMeta {
            task_id,
            status,
            result,
            traceback,
            children: vec![/* todo */],
            date_done,
            group_id: request.and_then(|r| r.group.clone()),
            parent_id: None,
            // todo: assign request properties to following fields
            name: None,
            args: None,
            kwargs: None,
            worker: request.and_then(|r| r.hostname.clone()),
            retries: request.map(|r| r.retries),
            queue: request.and_then(|r| r.reply_to.clone()),
        }
    }

    async fn __reload_task_meta(&mut self, task_id: &TaskId) -> Option<TaskMeta> {
        let meta = self.__get_task_meta(task_id, false).await;
        self.set_cached(task_id.clone(), meta).await
    }
}

#[async_trait]
impl<B: BaseBackendProtocol> Backend for B {
    type Builder = B::Builder;

    fn safe_url(&self) -> String {
        self._safe_url()
    }

    async fn get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.__get_task_meta(task_id, cache).await
    }

    fn recover_result_by_meta<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        self.recover_result(task_meta.result, task_meta.status)
    }

    async fn forget(&self, task_id: &TaskId) {
        self.del_cached(task_id).await;
        self.__forget_task_meta_by(task_id).await
    }

    async fn store_result_wrapped_as_task_meta<D: Serialize + Send + Sync, T: Task>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        state: State,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
    ) {
        let data = self.prepare_result(result, state);
        self._store_result_wrapped_as_task_meta(task_id, data, state, traceback, request)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
