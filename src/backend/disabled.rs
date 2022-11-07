use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::backend::{
    Backend, BackendBasic, BackendBuilder, TaskId, TaskMeta, TaskResult,
};
use crate::backend::options::StoreOptions;
use crate::error::BackendError;
use crate::prelude::Task;
use crate::states::State;

pub struct DisabledBackend {}

pub struct DisabledBackendBuilder {
    backend_basic: BackendBasic,
}

#[async_trait]
impl BackendBuilder for DisabledBackendBuilder {
    type Backend = DisabledBackend;

    fn new(_backend_url: &str) -> Self {
        Self {
            backend_basic: BackendBasic::new(_backend_url),
        }
    }

    fn backend_basic(&mut self) -> &mut BackendBasic {
        &mut self.backend_basic
    }

    async fn build(self) -> Result<Self::Backend, BackendError> {
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
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        // nothing to do
    }
}
