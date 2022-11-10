use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use serde::Deserialize;

use crate::backend::GetTaskResult;
use crate::kombu_serde::AnyValue;
use crate::protocol::{ExecResult, TaskMeta};

pub type CachedTaskMeta<R> = Option<TaskMeta<ExecResult<R>>>;

#[allow(unused)]
#[derive(Clone, Default)]
pub struct RevokeOptions {
    pub(crate) terminate: bool,
    pub(crate) wait: bool,
    pub(crate) timeout: Option<u64>,
}

#[derive(Clone, Default)]
pub struct GetOptions {
    pub(crate) timeout: Option<chrono::Duration>,
    pub(crate) interval: Option<chrono::Duration>,
}

#[async_trait]
pub trait BaseResult<R>: Send + Sync
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
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
pub trait BaseResultInfluenceParent {
    async fn forget_iteratively(&self);

    // #[allow(unused)]
    // async fn revoke_iteratively(&self, options: Option<RevokeOptions>) {
    //     unimplemented!()
    // }

    fn to_any(self) -> Box<dyn FullResult<AnyValue>>;
}

pub trait FullResult<R>: BaseResult<R> + BaseResultInfluenceParent
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<T, R> FullResult<R> for T
where
    T: BaseResult<R> + BaseResultInfluenceParent,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<R> Debug for dyn FullResult<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "$<impl FullResult<{}>>", std::any::type_name::<R>())
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

    #[allow(unused)]
    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        unreachable!()
    }
}

#[async_trait]
impl BaseResultInfluenceParent for VoidResult {
    async fn forget_iteratively(&self) {
        unreachable!()
    }

    fn to_any(self) -> Box<dyn FullResult<AnyValue>> {
        Box::new(VoidResult {})
    }
}
