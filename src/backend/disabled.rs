use async_trait::async_trait;
use serde::Serialize;

use crate::backend::inner::impl_layer::ImplLayer;
use crate::backend::options::StoreOptions;
use crate::backend::{BackendBasic, BackendBuilder};
use crate::error::BackendError;
use crate::prelude::Task;
use crate::protocol::{ExecResult, State, TaskId, TaskMeta};

pub struct DisabledBackend {
    backend_basic: BackendBasic,
}

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

    fn parse_url(&self) -> Option<url::Url> {
        url::Url::parse("disabled://localhost/").ok()
    }

    async fn build(self) -> Result<Self::Backend, BackendError> {
        Ok(Self::Backend {
            backend_basic: self.backend_basic,
        })
    }
}

#[async_trait]
impl ImplLayer for DisabledBackend {
    type Builder = DisabledBackendBuilder;

    fn basic_(&self) -> &BackendBasic {
        &self.backend_basic
    }

    #[allow(unused)]
    async fn forget_(&self, task_id: &TaskId) {
        unreachable!("Backend is disabled!")
    }

    #[allow(unused)]
    async fn get_task_meta_by_(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        unreachable!("Backend is disabled!")
    }

    #[allow(unused)]
    async fn store_result_<D, T>(
        &self,
        task_id: &TaskId,
        result: ExecResult<D>,
        state: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        // nothing to do
    }
}
