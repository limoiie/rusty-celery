use std::cell::RefCell;
use std::convert::TryInto;
use std::sync::Arc;

use async_recursion::async_recursion;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::backend::options::WaitOptions;
use crate::backend::{Backend, GetTaskResult};
use crate::kombu_serde::AnyValue;
use crate::protocol::{ExecResult, State, TaskMeta, TaskMetaInfo};

type CacheResult<R> = Option<TaskMeta<ExecResult<R>>>;

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult<B, R, PR = AnyValue>
where
    B: Backend,
    R: for<'de> Deserialize<'de> + Send + Sync,
    PR: for<'de> Deserialize<'de> + Send + Sync,
{
    pub task_id: String,
    parent: Option<Box<AsyncResult<B, PR, AnyValue>>>,
    backend: Arc<B>,
    cache: Arc<Mutex<RefCell<CacheResult<R>>>>,
}

#[allow(unused)]
pub struct RevokeOption {
    terminate: bool,
    wait: bool,
    timeout: Option<u64>,
}

#[derive(Default)]
pub struct GetOption {
    timeout: Option<chrono::Duration>,
    interval: Option<chrono::Duration>,
}

impl<B, R, PR> AsyncResult<B, R, PR>
where
    B: Backend,
    R: for<'de> Deserialize<'de> + Send + Sync + Clone,
    PR: for<'de> Deserialize<'de> + Send + Sync + Clone,
{
    pub fn new(task_id: &str, backend: Arc<B>) -> Self {
        Self {
            task_id: task_id.into(),
            parent: None,
            backend,
            cache: Arc::new(Mutex::new(RefCell::new(None))),
        }
    }

    #[async_recursion]
    pub async fn forget(&self) {
        self.cache.lock().await.replace(None);
        if let Some(parent) = &self.parent {
            parent.forget().await;
        }
        self.backend.forget(&self.task_id).await
    }

    #[allow(unused)]
    pub async fn revoke(&self, option: Option<RevokeOption>) {
        unimplemented!()
    }

    pub async fn get(&self, option: Option<GetOption>) -> GetTaskResult<R> {
        self.wait(option).await
    }

    pub async fn wait(&self, option: Option<GetOption>) -> GetTaskResult<R> {
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
