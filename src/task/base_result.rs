use async_trait::async_trait;
use serde::Deserialize;

use crate::backend::GetTaskResult;
use crate::protocol::{ExecResult, TaskMeta};

pub type CachedTaskMeta<R> = Option<TaskMeta<ExecResult<R>>>;

#[allow(unused)]
pub struct RevokeOptions {
    pub(crate) terminate: bool,
    pub(crate) wait: bool,
    pub(crate) timeout: Option<u64>,
}

#[derive(Default)]
pub struct GetOptions {
    pub(crate) timeout: Option<chrono::Duration>,
    pub(crate) interval: Option<chrono::Duration>,
}

#[async_trait]
pub trait BaseResult<R>
where
    R: for<'de> Deserialize<'de> + Send + Sync + Clone,
{
    async fn is_successful(&self) -> bool;

    async fn is_failure(&self) -> bool;

    async fn is_waiting(&self) -> bool;

    async fn is_ready(&self) -> bool;

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R>;

    async fn wait(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        self.get(options).await
    }
}

#[async_trait]
pub trait BaseResultInfluenceParent<R>: BaseResult<R>
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    async fn forget_iteratively(&self);

    #[allow(unused)]
    async fn revoke_iteratively(&self, options: Option<RevokeOptions>) {
        unimplemented!()
    }
}

/// Represents a non-existing result.
pub struct VoidResult;

#[async_trait]
impl<R> BaseResult<R> for VoidResult
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    async fn is_successful(&self) -> bool {
        unreachable!()
    }

    async fn is_failure(&self) -> bool {
        unreachable!()
    }

    async fn is_waiting(&self) -> bool {
        unreachable!()
    }

    async fn is_ready(&self) -> bool {
        unreachable!()
    }

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        unreachable!()
    }
}

#[async_trait]
impl<R> BaseResultInfluenceParent<R> for VoidResult
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    async fn forget_iteratively(&self) {
        unreachable!()
    }
}
