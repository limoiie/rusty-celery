use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use async_result::AsyncResult;
pub use group_result::{GroupAnyResult, GroupTupleResult};
pub use void_result::VoidResult;

use crate::backend::GetTaskResult;
use crate::kombu_serde::AnyValue;
use crate::protocol::{ExecResult, TaskMeta};

mod async_result;
mod group_result;
mod void_result;

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

    async fn forget_iteratively(&self);

    // #[allow(unused)]
    // async fn revoke_iteratively(&self, options: Option<RevokeOptions>) {
    //     unimplemented!()
    // }

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>>;

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R>;

    async fn wait(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        self.get(options).await
    }
}

impl<R> Debug for dyn BaseResult<R> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "$<impl BaseResult<{}>>", std::any::type_name::<R>())
    }
}

#[async_trait]
pub trait BaseResultRequireP<R, P, PR>
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

pub trait FullResult<R, P, PR>: BaseResult<R> + BaseResultRequireP<R, P, PR>
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<T, R, P, PR> FullResult<R, P, PR> for T
where
    T: BaseResult<R> + BaseResultRequireP<R, P, PR>,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<R, PR, P> Debug for dyn FullResult<R, PR, P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "$<impl FullResult<{}>>", std::any::type_name::<R>())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GroupStructure((String, Option<Box<GroupStructure>>), Vec<GroupStructure>);
