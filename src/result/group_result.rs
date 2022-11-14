use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use futures::FutureExt;
use serde::Deserialize;

use crate::backend::{Backend, GetTaskResult};
use crate::error::BackendError;
use crate::kombu_serde::{AnyValue, FromVec};
use crate::result::{BaseResult, BaseResultRequireP, GetOptions, ResultStructure, VoidResult};

#[derive(Debug, Clone)]
pub struct GroupAnyResult<B, P = VoidResult, PR = ()>
where
    B: Backend,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    pub group_id: String,
    pub parent: Option<Arc<P>>,
    pub children: Vec<Arc<Box<dyn BaseResult<AnyValue>>>>,
    pub backend: Arc<B>,
    pub pha: PhantomData<PR>,
}

#[async_trait]
impl<B, P, PR> BaseResult<AnyValue> for GroupAnyResult<B, P, PR>
where
    B: Backend + 'static,
    P: BaseResult<PR> + Send + Sync + 'static,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    fn id(&self) -> String {
        self.group_id.clone()
    }

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

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>> {
        Box::new(self)
    }

    fn to_structure(&self) -> Box<ResultStructure> {
        Box::new(ResultStructure(
            (
                self.id(),
                self.parent.as_ref().map(|p| p.as_ref().to_structure()),
            ),
            self.iter_children().map(|c| *c.to_structure()).collect(),
        ))
    }

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<AnyValue> {
        self.get_as_vec(options).await.map(|r| {
            r.map(|vec| {
                AnyValue::bury_vec(vec)
                    .unwrap_or_else(|| self.backend.basic().result_serializer.to_value(&None::<()>))
            })
        })
    }
}

#[async_trait]
impl<B, P, PR> BaseResultRequireP<Vec<AnyValue>, P, PR> for GroupAnyResult<B, P, PR>
where
    B: Backend,
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

    pub async fn wait_as_vec(&self, options: Option<GetOptions>) -> GetTaskResult<Vec<AnyValue>> {
        self.get_as_vec(options).await
    }

    pub async fn get_as_vec(&self, options: Option<GetOptions>) -> GetTaskResult<Vec<AnyValue>> {
        future::try_join_all(
            self.children
                .iter()
                .map(|result| result.get(options.clone())),
        )
        .await
        .map(|r| r.into_iter().collect())
    }
}

impl<B, P, PR> GroupAnyResult<B, P, PR>
where
    B: Backend + 'static,
    P: BaseResult<PR> + 'static,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    pub async fn save(&self) {
        self.backend
            .save_group(self.group_id.as_str(), *self.to_structure())
            .await
    }
}

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
    B: Backend + 'static,
    R: Send + Sync + Clone + FromVec + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync + 'static,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    fn id(&self) -> String {
        self.proxy.id()
    }

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

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>> {
        Box::new(self.proxy)
    }

    fn to_structure(&self) -> Box<ResultStructure> {
        self.proxy.to_structure()
    }

    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        self.proxy
            .get_as_vec(options)
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

impl<B, R, P, PR> GroupTupleResult<B, R, P, PR>
where
    B: Backend + 'static,
    R: Send + Sync + 'static ,
    P: BaseResult<PR> + Send + Sync + 'static,
    PR: Clone + FromVec + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    pub async fn save(&self) {
        self.proxy.save().await
    }
}

macro_rules! impl_group_tuple_result_new {
    ( $( $result_type:ident )+ ) => {
        impl<Bd, $( $result_type, )* Parent, ParentR> GroupTupleResult<Bd, ( $( $result_type, )* ), Parent, ParentR>
        where
            Bd: Backend + 'static,
            $( $result_type: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static, )*
            Parent: BaseResult<ParentR> + Send + Sync + 'static,
            ParentR: Clone + FromVec + Send + Sync + for<'de> Deserialize<'de> + 'static,
        {
            #[allow(non_snake_case)]
            pub fn new(
                group_id: String,
                backend: Arc<Bd>,
                $( $result_type: impl BaseResult<$result_type> ),*
            ) -> Self {
                GroupTupleResult {
                    proxy: GroupAnyResult::new(group_id, backend, vec![
                        $( Arc::new($result_type.into_any()) ),*
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

fn bool_to_future_result(b: bool) -> future::Ready<Result<bool, bool>> {
    (b).then(|| future::ok(b)).unwrap_or_else(|| future::err(b))
}

fn not_bool_to_future_result(b: bool) -> future::Ready<Result<bool, bool>> {
    (!b).then(|| future::ok(!b))
        .unwrap_or_else(|| future::err(!b))
}

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

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use crate::backend::{BackendBuilder, DisabledBackendBuilder};

    use super::*;

    #[tokio::test]
    async fn test_to_structure() {
        let backend = Arc::new(
            DisabledBackendBuilder::new("disabled://localhost")
                .build()
                .await
                .unwrap(),
        );

        let group_result = GroupAnyResult::<_, VoidResult, ()>::new(
            "group#1".to_owned(),
            backend,
            vec![
                Arc::new(Box::new(VoidResult("void#1".to_owned())) as Box<dyn BaseResult<AnyValue>>),
                Arc::new(Box::new(VoidResult("void#2".to_owned())) as Box<dyn BaseResult<AnyValue>>),
            ],
        );

        let structure = group_result.to_structure();

        let structure_str = serde_json::to_string(structure.deref()).unwrap();
        let expected_str = r#"[["group#1",null],[[["void#1",null],[]],[["void#2",null],[]]]]"#;

        assert_eq!(structure_str, expected_str)
    }
}
