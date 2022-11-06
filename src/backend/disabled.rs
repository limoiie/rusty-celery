use async_trait::async_trait;
use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, BackendBuilder, TaskId, TaskMeta, TaskResult, Traceback};
use crate::error::BackendError;
use crate::kombu_serde::SerializerKind;
use crate::prelude::Task;
use crate::states::State;
use crate::task::Request;

pub struct DisabledBackend {}

pub struct DisabledBackendBuilder {}

#[async_trait]
impl BackendBuilder for DisabledBackendBuilder {
    type Backend = DisabledBackend;

    fn new(_backend_url: &str) -> Self {
        Self {}
    }

    fn result_serializer(self, _kind: SerializerKind) -> Self {
        self
    }

    fn result_expires(self, _expiration: Option<Duration>) -> Self {
        self
    }

    async fn build(&self) -> Result<Self::Backend, BackendError> {
        Ok(Self::Backend {})
    }
}

#[async_trait]
impl Backend for DisabledBackend {
    type Builder = DisabledBackendBuilder;

    fn safe_url(&self) -> String {
        "#[disabled!]#".to_owned()
    }

    async fn get_task_meta(&self, _task_id: &TaskId, _cache: bool) -> TaskMeta {
        unreachable!("Backend is disabled!")
    }

    fn recover_result_by_meta<D>(&self, _task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        unreachable!("Backend is disabled!")
    }

    async fn forget(&self, _task_id: &TaskId) {
        unreachable!("Backend is disabled!")
    }

    async fn store_result_wrapped_as_task_meta<D: Serialize + Send + Sync, T: Task>(
        &self,
        _task_id: &TaskId,
        _result: TaskResult<D>,
        _state: State,
        _traceback: Option<Traceback>,
        _request: Option<&Request<T>>,
    ) {
        // nothing to do
    }
}
