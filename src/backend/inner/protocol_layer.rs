use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::backend::inner::{BackendBasicLayer, BackendSerdeLayer};
use crate::backend::options::StoreOptions;
use crate::backend::{Backend, BackendBuilder, TaskId, TaskMeta, TaskResult};
use crate::kombu_serde::AnyValue;
use crate::prelude::Task;
use crate::states::State;

#[async_trait]
pub trait BackendProtocolLayer: BackendBasicLayer + BackendSerdeLayer {
    type Builder: BackendBuilder<Backend = Self>;

    async fn _store_result<T: Task>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        status: State,
        option: &StoreOptions<T>,
    );

    async fn _forget_task_meta_by(&self, task_id: &TaskId);

    async fn _fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta;

    fn _decode_task_meta(&self, payload: String) -> TaskMeta;

    async fn _get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.ensure_not_eager();
        if cache {
            if let Some(meta) = self._get_cached(task_id).await {
                return meta;
            }
        }

        let meta = self._fetch_task_meta_by(task_id).await;
        if cache && meta.status == State::SUCCESS {
            self._set_cached(task_id.clone(), meta.clone()).await;
        }

        return meta;
    }

    async fn _make_task_meta<T>(
        task_id: TaskId,
        result: AnyValue,
        status: State,
        option: &StoreOptions<T>,
    ) -> TaskMeta
    where
        T: Task,
    {
        let date_done = if status.is_ready() {
            Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339())
        } else {
            None
        };

        TaskMeta {
            task_id,
            status,
            result,
            traceback: option.traceback,
            children: vec![/* todo */],
            date_done,
            group_id: option.request.and_then(|r| r.group.clone()),
            parent_id: None,
            // todo: assign request properties to following fields
            name: None,
            args: None,
            kwargs: None,
            worker: option.request.and_then(|r| r.hostname.clone()),
            retries: option.request.map(|r| r.retries),
            queue: option.request.and_then(|r| r.reply_to.clone()),
        }
    }

    async fn _reload_task_meta(&mut self, task_id: &TaskId) -> Option<TaskMeta> {
        let meta = self._get_task_meta(task_id, false).await;
        self._set_cached(task_id.clone(), meta).await
    }
}

#[async_trait]
impl<B: BackendProtocolLayer> Backend for B {
    type Builder = B::Builder;

    fn safe_url(&self) -> String {
        self._safe_url()
    }

    async fn get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self._get_task_meta(task_id, cache).await
    }

    fn recover_result_by_meta<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        self._recover_result(task_meta.result, task_meta.status)
    }

    async fn forget(&self, task_id: &TaskId) {
        self._del_cached(task_id).await;
        self._forget_task_meta_by(task_id).await
    }

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        option: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        let data = self._prepare_result(result, status);
        self._store_result(task_id, data, status, option).await;
    }
}
