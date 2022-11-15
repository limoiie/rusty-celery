use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use async_result::AsyncResult;
pub use group_result::{GroupAnyResult, GroupTupleResult};
pub use void_result::VoidResult;

use crate::backend::{Backend, GetTaskResult};
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
    fn id(&self) -> String;

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

    fn to_structure(&self) -> Box<ResultStructure>;

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

/// A more compact serializable format that represents the structure of the task results.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResultStructure((String, Option<Box<ResultStructure>>), Vec<ResultStructure>);

pub enum ResultWrap<B: Backend> {
    Item(AsyncResult<B, AnyValue>),
    Tuple(GroupAnyResult<B>),
}

impl<B: Backend + 'static> ResultWrap<B> {
    pub fn cast_into(self) -> Box<dyn BaseResult<AnyValue>> {
        match self {
            ResultWrap::Item(r) => Box::new(r),
            ResultWrap::Tuple(r) => Box::new(r),
        }
    }
}

impl ResultStructure {
    pub fn restore<B: Backend + 'static>(self, backend: Arc<B>) -> ResultWrap<B>
    where
        B: Backend + 'static,
    {
        let ResultStructure((group_id, parent), children) = self;
        if children.is_empty() {
            ResultWrap::Item(
                AsyncResult::new(group_id, backend.clone()).with_parent(
                    parent
                        .map(|p| *p)
                        .map(|p| Arc::new(p.restore(backend).cast_into())),
                ),
            )
        } else {
            ResultWrap::Tuple(GroupAnyResult {
                group_id,
                parent: parent
                    .map(|p| *p)
                    .map(|p| Arc::new(p.restore(backend.clone()).cast_into())),
                children: children
                    .into_iter()
                    .map(|c| Arc::new(c.restore(backend.clone()).cast_into()))
                    .collect(),
                backend,
            })
        }
    }

    pub fn restore_group<B>(self, backend: Arc<B>) -> Option<GroupAnyResult<B>>
    where
        B: Backend + 'static,
    {
        match self.restore(backend) {
            ResultWrap::Tuple(group) => Some(group),
            ResultWrap::Item(_) => None,
        }
    }

    pub fn restore_single<B>(self, backend: Arc<B>) -> Option<AsyncResult<B, AnyValue>>
    where
        B: Backend + 'static,
    {
        match self.restore(backend) {
            ResultWrap::Tuple(_) => None,
            ResultWrap::Item(single) => Some(single),
        }
    }
}
