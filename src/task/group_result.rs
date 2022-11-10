use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, GetTaskResult};
use crate::error::BackendError;
use crate::kombu_serde::{AnyValue, FromVec};
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

    fn to_any(self) -> Box<dyn FullResult<AnyValue>> {
        unimplemented!()
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
pub struct GroupStructure((String, Option<Box<GroupStructure>>), Vec<GroupStructure>);

#[derive(Debug, Clone)]
pub struct GroupTupleResult<B, R, P = VoidResult>
where
    B: Backend,
{
    proxy: GroupAnyResult<B, P>,
    pha: PhantomData<R>,
}

#[async_trait]
impl<B, R, P> BaseResult<R> for GroupTupleResult<B, R, P>
where
    B: Backend,
    R: Send + Sync + Clone + FromVec + for<'de> Deserialize<'de>,
    P: Send + Sync,
{
    async fn is_successful(&self) -> bool {
        self.proxy.is_successful().await
    }

    async fn is_failure(&self) -> bool {
        self.proxy.is_failure().await
    }

    async fn is_waiting(&self) -> bool {
        self.proxy.is_waiting().await
    }

    async fn is_ready(&self) -> bool {
        self.proxy.is_ready().await
    }

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        self.proxy
            .get(options)
            .await?
            .map(R::from_vec)
            .transpose_result()
            .map_err(BackendError::DeserializeError)
    }
}

#[async_trait]
impl<B, R, P> BaseResultInfluenceParent for GroupTupleResult<B, R, P>
where
    B: Backend,
    R: Send + Sync,
    P: BaseResultInfluenceParent + Send + Sync,
{
    async fn forget_iteratively(&self) {
        self.proxy.forget_iteratively().await
    }

    fn to_any(self) -> Box<dyn FullResult<AnyValue>> {
        unimplemented!()
    }
}

impl<B, R, P> From<GroupAnyResult<B, P>> for GroupTupleResult<B, R, P>
where
    B: Backend,
    R: Send + Sync,
    P: BaseResultInfluenceParent + Send + Sync,
{
    fn from(other: GroupAnyResult<B, P>) -> Self {
        Self {
            proxy: other,
            pha: Default::default(),
        }
    }
}

macro_rules! impl_group_tuple_result_new {
    ( $( $result_type:ident )+ ) => {
        impl<Bd, $( $result_type, )* Parent> GroupTupleResult<Bd, ( $( $result_type, )* ), Parent>
        where
            Bd: Backend,
            $( $result_type: Clone + Send + Sync + for<'de> Deserialize<'de>, )*
            Parent: BaseResultInfluenceParent + Send + Sync,
        {
            #[allow(non_snake_case)]
            pub fn new(
                group_id: String,
                backend: Arc<Bd>,
                $( $result_type: impl FullResult<$result_type> ),*
            ) -> Self {
                GroupTupleResult {
                    proxy: GroupAnyResult::new(group_id, backend, vec![
                        $( Arc::new($result_type.to_any()) ),*
                    ]),
                    pha: Default::default(),
                }
            }
        }
    };
}

impl_group_tuple_result_new! { A }
impl_group_tuple_result_new! { A B }
impl_group_tuple_result_new! { A B C }
impl_group_tuple_result_new! { A B C D }
impl_group_tuple_result_new! { A B C D E }

trait Transpose<D, EI, EO> {
    fn transpose_result(self) -> Result<Result<D, EO>, EI>;
}

impl<D, EI, EO> Transpose<D, EI, EO> for Result<Result<D, EI>, EO> {
    fn transpose_result(self) -> Result<Result<D, EO>, EI> {
        match self {
            Ok(d) => Ok(Ok(d?)),
            Err(e) => Ok(Err(e)),
        }
    }
}

