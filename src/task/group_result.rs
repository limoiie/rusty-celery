use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, GetTaskResult};
use crate::kombu_serde::AnyValue;
use crate::task::base_result::{
    BaseResult, BaseResultInfluenceParent, FullResult, GetOptions, VoidResult,
};

#[derive(Debug, Clone)]
pub struct GroupAnyResult<B, P = VoidResult>
where
    B: Backend,
{
    pub group_id: String,
    parent: Option<Arc<P>>,
    children: Vec<Arc<Box<dyn FullResult<AnyValue>>>>,
    #[allow(unused)]
    backend: Arc<B>,
}

#[async_trait]
impl<B, P> BaseResult<Vec<AnyValue>> for GroupAnyResult<B, P>
where
    B: Backend,
    P: Send + Sync,
{
    async fn is_successful(&self) -> bool {
        future::try_join_all(
            self.iter_children()
                .map(BaseResult::is_successful)
                .map(|f| f.then(bool_to_future_result)),
        )
        .await
        .is_ok()
    }

    async fn is_failure(&self) -> bool {
        future::try_join_all(
            self.iter_children()
                .map(BaseResult::is_failure)
                .map(|f| f.then(not_bool_to_future_result)),
        )
        .await
        .is_err()
    }

    async fn is_waiting(&self) -> bool {
        !self.is_ready().await
    }

    async fn is_ready(&self) -> bool {
        future::try_join_all(
            self.iter_children()
                .map(BaseResult::is_ready)
                .map(|f| f.then(bool_to_future_result)),
        )
        .await
        .is_ok()
    }

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<Vec<AnyValue>> {
        future::try_join_all(
            self.children
                .iter()
                .map(|result| result.get(options.clone())),
        )
        .await
        .map(|r| r.into_iter().collect())
    }
}

#[async_trait]
impl<B, P> BaseResultInfluenceParent for GroupAnyResult<B, P>
where
    B: Backend,
    P: BaseResultInfluenceParent + Send + Sync,
{
    async fn forget_iteratively(&self) {
        future::join_all(self.children.iter().map(|r| r.forget_iteratively())).await;

        if let Some(parent) = &self.parent {
            parent.forget_iteratively().await;
        }
    }
}

impl<B, P> GroupAnyResult<B, P>
where
    B: Backend,
{
    pub fn new<RS>(group_id: String, backend: Arc<B>, results: RS) -> Self
    where
        RS: IntoIterator<Item = Arc<Box<dyn FullResult<AnyValue>>>>,
    {
        GroupAnyResult {
            group_id,
            parent: None,
            children: results.into_iter().collect(),
            backend,
        }
    }

    fn iter_children(&self) -> impl Iterator<Item = &dyn FullResult<AnyValue>> {
        self.children.iter().map(AsRef::as_ref).map(AsRef::as_ref)
    }
}

fn bool_to_future_result(b: bool) -> future::Ready<Result<bool, bool>> {
    (b).then(|| future::ok(b)).unwrap_or_else(|| future::err(b))
}

fn not_bool_to_future_result(b: bool) -> future::Ready<Result<bool, bool>> {
    (!b).then(|| future::ok(!b))
        .unwrap_or_else(|| future::err(!b))
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GroupStructure(
    (String, Option<Box<GroupStructure>>),
    Vec<GroupStructure>,
);
