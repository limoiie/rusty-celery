use async_trait::async_trait;
use serde::Deserialize;
use crate::backend::GetTaskResult;
use crate::kombu_serde::AnyValue;
use crate::prelude::BaseResult;
use crate::result::{BaseResultRequireP, GetOptions};

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

    async fn forget_iteratively(&self) {
        unreachable!()
    }

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>> {
        Box::new(VoidResult {})
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
