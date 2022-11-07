use std::cell::RefCell;
use std::sync::Arc;

use async_recursion::async_recursion;
use serde::Deserialize;
use tokio::sync::Mutex;

use crate::backend::options::WaitForOptions;
use crate::backend::{Backend, GetTaskResult};
use crate::protocol::{State, TaskMeta};

/// An [`AsyncResult`] is a handle for the result of a task.
#[derive(Debug, Clone)]
pub struct AsyncResult<B: Backend> {
    pub task_id: String,
    parent: Option<Box<AsyncResult<B>>>,
    backend: Arc<B>,
    cache: Arc<Mutex<RefCell<Option<TaskMeta>>>>,
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

impl<B> AsyncResult<B>
where
    B: Backend,
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

    pub async fn get<D>(&self, option: Option<GetOption>) -> GetTaskResult<D>
    where
        D: for<'de> Deserialize<'de>,
    {
        self.wait(option).await
    }

    pub async fn wait<D>(&self, option: Option<GetOption>) -> GetTaskResult<D>
    where
        D: for<'de> Deserialize<'de>,
    {
        let option = option.unwrap_or_default();
        let meta = self
            .backend
            .as_ref()
            .wait_for(
                &self.task_id,
                WaitForOptions::builder()
                    .timeout(option.timeout)
                    .interval(option.interval)
                    .build(),
            )
            .await?;

        let meta = self.set_cache_if_ready(meta).await;
        Ok(self.backend.as_ref().restore_result::<D>(meta).unwrap())
    }

    pub async fn status(&self) -> State {
        self.get_task_meta().await.status
    }

    async fn get_task_meta(&self) -> TaskMeta {
        if self.cache.lock().await.get_mut().is_none() {
            let task_meta = self
                .backend
                .as_ref()
                .get_task_meta_by(&self.task_id, true)
                .await;
            self.set_cache_if_ready(task_meta).await
        } else {
            self.cache.lock().await.get_mut().as_ref().unwrap().clone()
        }
    }

    async fn set_cache(&self, task_meta: TaskMeta) -> TaskMeta {
        // todo: task_meta.children = ..
        self.cache.lock().await.replace(Some(task_meta.clone()));
        task_meta
    }

    async fn set_cache_if_ready(&self, task_meta: TaskMeta) -> TaskMeta {
        if task_meta.is_ready() {
            self.set_cache(task_meta).await
        } else {
            task_meta
        }
    }
}
