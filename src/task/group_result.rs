use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::backend::{Backend, GetTaskResult};
use crate::error::BackendError;
use crate::kombu_serde::{AnyValue, FromVec};
use crate::task::base_result::{BaseResult, BaseResultRequireP, GetOptions, VoidResult};

#[derive(Debug, Clone)]
pub struct GroupAnyResult<B, P = VoidResult, PR = ()>
where
    B: Backend,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    pub group_id: String,
    parent: Option<Arc<P>>,
    children: Vec<Arc<Box<dyn BaseResult<AnyValue>>>>,
    #[allow(unused)]
    backend: Arc<B>,
    pha: PhantomData<PR>,
}

#[async_trait]
impl<B, P, PR> BaseResult<Vec<AnyValue>> for GroupAnyResult<B, P, PR>
where
    B: Backend,
    P: BaseResult<PR> + Send + Sync,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
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

    async fn forget_iteratively(&self) {
        future::join_all(self.children.iter().map(|r| r.forget_iteratively())).await;

        if let Some(parent) = &self.parent {
            parent.forget_iteratively().await;
        }
    }

    fn to_any(self) -> Box<dyn BaseResult<AnyValue>> {
        unimplemented!()
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
impl<B, R, P, PR> BaseResultRequireP<R, P, PR> for GroupAnyResult<B, P, PR>
where
    B: Backend,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<B, P, PR> GroupAnyResult<B, P, PR>
where
    B: Backend,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    pub fn new<RS>(group_id: String, backend: Arc<B>, results: RS) -> Self
    where
        RS: IntoIterator<Item = Arc<Box<dyn BaseResult<AnyValue>>>>,
    {
        GroupAnyResult {
            group_id,
            parent: None,
            pha: PhantomData::default(),
            children: results.into_iter().collect(),
            backend,
        }
    }

    fn iter_children(&self) -> impl Iterator<Item = &dyn BaseResult<AnyValue>> {
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
pub struct GroupTupleResult<B, R, P = VoidResult, PR = ()>
where
    B: Backend,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    proxy: GroupAnyResult<B, P, PR>,
    pha: PhantomData<(R, PR)>,
}

#[async_trait]
impl<B, R, P, PR> BaseResult<R> for GroupTupleResult<B, R, P, PR>
where
    B: Backend,
    R: Send + Sync + Clone + FromVec + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
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

    async fn forget_iteratively(&self) {
        self.proxy.forget_iteratively().await
    }

    fn to_any(self) -> Box<dyn BaseResult<AnyValue>> {
        unimplemented!()
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
impl<B, R, P, PR> BaseResultRequireP<R, P, PR> for GroupTupleResult<B, R, P, PR>
where
    B: Backend,
    R: Send + Sync + Clone + FromVec + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Send + Sync + Clone + FromVec + for<'de> Deserialize<'de>,
{
}

impl<B, R, P, PR> From<GroupAnyResult<B, P, PR>> for GroupTupleResult<B, R, P, PR>
where
    B: Backend,
    R: Send + Sync,
    P: BaseResult<PR> + Send + Sync,
    PR: Clone + FromVec + Send + Sync + for<'de> Deserialize<'de>,
{
    fn from(other: GroupAnyResult<B, P, PR>) -> Self {
        Self {
            proxy: other,
            pha: Default::default(),
        }
    }
}

macro_rules! impl_group_tuple_result_new {
    ( $( $result_type:ident )+ ) => {
        impl<Bd, $( $result_type, )* Parent, ParentR> GroupTupleResult<Bd, ( $( $result_type, )* ), Parent, ParentR>
        where
            Bd: Backend,
            $( $result_type: Clone + Send + Sync + for<'de> Deserialize<'de>, )*
            Parent: BaseResult<ParentR> + Send + Sync,
            ParentR: Clone + FromVec + Send + Sync + for<'de> Deserialize<'de>,
        {
            #[allow(non_snake_case)]
            pub fn new(
                group_id: String,
                backend: Arc<Bd>,
                $( $result_type: impl BaseResult<$result_type> ),*
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
