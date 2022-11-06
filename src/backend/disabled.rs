use async_trait::async_trait;
use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, BackendBuilder, StoreOption, TaskId, TaskMeta, TaskResult};
use crate::error::BackendError;
use crate::kombu_serde::SerializerKind;
use crate::prelude::Task;
use crate::states::State;

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

    #[allow(unused)]
    async fn get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        unreachable!("Backend is disabled!")
    }

    #[allow(unused)]
    fn recover_result_by_meta<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        unreachable!("Backend is disabled!")
    }

    #[allow(unused)]
    async fn forget(&self, task_id: &TaskId) {
        unreachable!("Backend is disabled!")
    }

    #[allow(unused)]
    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        state: State,
        store: &StoreOption<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        // nothing to do
    }
}
