use async_trait::async_trait;
use chrono::Duration;
use serde::Serialize;

use crate::backend::{Backend, BackendBuilder, TaskId, TaskResult, Traceback};
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

    async fn forget(&mut self, _task_id: &TaskId) {
        panic!("Backend is disabled!")
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
