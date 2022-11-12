use std::cell::RefCell;
use std::convert::TryInto;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::backend::options::WaitOptions;
use crate::backend::{Backend, GetTaskResult};
use crate::kombu_serde::AnyValue;
use crate::protocol::{ExecResult, State, TaskMeta, TaskMetaInfo};
use crate::result::{BaseResult, BaseResultRequireP, CachedTaskMeta, GetOptions};
use crate::result::void_result::VoidResult;

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult<B, R = (), P = VoidResult, PR = ()>
where
    B: Backend,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR>,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    pub task_id: String,
    parent: Option<Arc<P>>,
    backend: Arc<B>,
    cache: Arc<Mutex<RefCell<CachedTaskMeta<R>>>>,
    pha: PhantomData<PR>,
}

#[async_trait]
impl<B, R, P, PR> BaseResult<R> for AsyncResult<B, R, P, PR>
where
    B: Backend + 'static,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync + 'static,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de> + 'static,
{
    async fn is_successful(&self) -> bool {
        self.get_task_meta_info().await.status.is_successful()
    }

    async fn is_failure(&self) -> bool {
        self.get_task_meta_info().await.status.is_exception()
    }

    async fn is_waiting(&self) -> bool {
        !self.get_task_meta_info().await.status.is_ready()
    }

    async fn is_ready(&self) -> bool {
        self.get_task_meta_info().await.status.is_ready()
    }

    async fn forget_iteratively(&self) {
        self.cache.lock().await.replace(None);
        if let Some(parent) = &self.parent {
            parent.forget_iteratively().await;
        }
        self.backend.forget(&self.task_id).await
    }

    fn into_any(self) -> Box<dyn BaseResult<AnyValue>> {
        Box::new(AsyncResult {
            task_id: self.task_id,
            parent: self.parent,
            backend: self.backend,
            cache: Arc::new(Mutex::new(RefCell::new(None))),
            pha: Default::default(),
        })
    }

    async fn get(&self, option: Option<GetOptions>) -> GetTaskResult<R> {
        let option = option.unwrap_or_default();

        let meta = self
            .backend
            .as_ref()
            .wait(
                &self.task_id,
                WaitOptions::builder()
                    .timeout(option.timeout)
                    .interval(option.interval)
                    .build(),
            )
            .await?
            .try_into()?;

        self.set_cache(meta).await;

        let mut guard = self.cache.lock().await;

        let result = guard
            .get_mut()
            .as_ref()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .clone();

        Ok(result)
    }
}

#[async_trait]
impl<B, R, P, PR> BaseResultRequireP<R, P, PR> for AsyncResult<B, R, P, PR>
where
    B: Backend,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync + 'static,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
}

impl<B, R, P, PR> AsyncResult<B, R, P, PR>
where
    B: Backend,
    R: Clone + Send + Sync + for<'de> Deserialize<'de>,
    P: BaseResult<PR> + Send + Sync,
    PR: Clone + Send + Sync + for<'de> Deserialize<'de>,
{
    pub fn new(task_id: String, backend: Arc<B>) -> Self {
        Self {
            task_id,
            parent: None,
            backend,
            cache: Arc::new(Mutex::new(RefCell::new(None))),
            pha: Default::default(),
        }
    }

    pub async fn status(&self) -> State {
        self.get_task_meta_info().await.status
    }

    async fn get_task_meta_info(&self) -> TaskMetaInfo {
        if self.cache.lock().await.get_mut().is_none() {
            let task_meta = self
                .backend
                .as_ref()
                .get_task_meta_by(&self.task_id, true)
                .await;
            self.set_cache_if_ready(task_meta).await
        } else {
            self.cache
                .lock()
                .await
                .get_mut()
                .as_ref()
                .unwrap()
                .info
                .clone()
        }
    }

    async fn set_cache(&self, task_meta: TaskMeta<ExecResult<R>>) {
        // todo: task_meta.children = ..
        self.cache.lock().await.replace(Some(task_meta));
    }

    async fn set_cache_if_ready(&self, task_meta: TaskMeta<AnyValue>) -> TaskMetaInfo {
        if task_meta.is_ready() {
            let task_meta_info = task_meta.info.clone();
            self.set_cache(task_meta.try_into().unwrap()).await;
            task_meta_info
        } else {
            task_meta.info
        }
    }
}
