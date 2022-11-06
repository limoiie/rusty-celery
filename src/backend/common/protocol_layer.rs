use crate::backend::{
    Backend, BackendBasicLayer, BackendBuilder, BackendSerdeLayer, StoreOption, TaskId, TaskMeta,
    TaskResult,
};
use crate::kombu_serde::AnyValue;
use crate::prelude::Task;
use crate::states::State;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait BackendProtocolLayer: BackendBasicLayer + BackendSerdeLayer {
    type Builder: BackendBuilder<Backend = Self>;

    async fn _store_result<T: Task>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        status: State,
        option: &StoreOption<T>,
    );

    async fn __forget_task_meta_by(&self, task_id: &TaskId);

    async fn __fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta;

    fn __decode_task_meta(&self, payload: String) -> TaskMeta;

    async fn __get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.ensure_not_eager();
        if cache {
            if let Some(meta) = self.get_cached(task_id).await {
                return meta;
            }
        }

        let meta = self.__fetch_task_meta_by(task_id).await;
        if cache && meta.status == State::SUCCESS {
            self.set_cached(task_id.clone(), meta.clone()).await;
        }

        return meta;
    }

    async fn __make_task_meta<T>(
        task_id: TaskId,
        result: AnyValue,
        status: State,
        option: &StoreOption<T>,
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

    async fn __reload_task_meta(&mut self, task_id: &TaskId) -> Option<TaskMeta> {
        let meta = self.__get_task_meta(task_id, false).await;
        self.set_cached(task_id.clone(), meta).await
    }
}

#[async_trait]
impl<B: BackendProtocolLayer> Backend for B {
    type Builder = B::Builder;

    fn safe_url(&self) -> String {
        self._safe_url()
    }

    async fn get_task_meta(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.__get_task_meta(task_id, cache).await
    }

    fn recover_result_by_meta<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        self.recover_result(task_meta.result, task_meta.status)
    }

    async fn forget(&self, task_id: &TaskId) {
        self.del_cached(task_id).await;
        self.__forget_task_meta_by(task_id).await
    }

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        option: &StoreOption<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        let data = self.prepare_result(result, status);
        self._store_result(task_id, data, status, option).await;
    }
}
