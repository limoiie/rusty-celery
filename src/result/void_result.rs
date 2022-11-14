use async_trait::async_trait;
use serde::Deserialize;

use crate::backend::GetTaskResult;
use crate::export::Serialize;
use crate::kombu_serde::AnyValue;
use crate::prelude::BaseResult;
use crate::result::{BaseResultRequireP, GetOptions, ResultStructure};

/// Represents a non-existing result.
#[derive(Clone, Serialize, Deserialize)]
pub struct VoidResult(pub String);

#[async_trait]
impl<R> BaseResult<R> for VoidResult
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    fn id(&self) -> String {
        self.0.clone()
    }

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

    async fn forget_iteratively(&self) {
        unreachable!()
    }

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>> {
        Box::new(self)
    }

    fn to_structure(&self) -> Box<ResultStructure> {
        Box::new(ResultStructure((self.0.clone(), None), vec![]))
    }

    #[allow(unused)]
    async fn get(&self, options: Option<GetOptions>) -> GetTaskResult<R> {
        unreachable!()
    }
}

#[async_trait]
impl<R, P, PR> BaseResultRequireP<R, P, PR> for VoidResult
where
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}
