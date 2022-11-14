use std::convert::TryInto;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::backend::inner::{BackendBasicLayer, BackendSerdeLayer, ImplLayer};
use crate::backend::options::StoreOptions;
use crate::backend::{Backend, BackendBasic, BackendBuilder};
use crate::kombu_serde::AnyValue;
use crate::prelude::Task;
use crate::protocol::{
    ExecResult, GroupMeta, GroupMetaInfo, State, TaskId, TaskMeta, TaskMetaInfo,
};
use crate::result::ResultStructure;

#[async_trait]
pub trait BackendProtocolLayer: BackendBasicLayer + BackendSerdeLayer {
    type Builder: BackendBuilder<Backend = Self>;

    async fn _store_task_meta(&self, task_id: &TaskId, task_meta: TaskMeta);

    async fn _forget_task_meta_by(&self, task_id: &TaskId);

    async fn _fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta;

    async fn _get_task_meta_by(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.ensure_not_eager();
        if cache {
            if let Some(meta) = self._get_cached(task_id).await {
                return meta;
            }
        }

        let meta = self._fetch_task_meta_by(task_id).await;
        if cache && meta.info.status == State::SUCCESS {
            self._set_cached(task_id.clone(), meta.clone()).await;
        }

        return meta;
    }

    async fn _make_task_meta<T, D>(
        &self,
        task_id: TaskId,
        result: ExecResult<D>,
        status: State,
        option: &StoreOptions<T>,
    ) -> TaskMeta<ExecResult<D>>
    where
        T: Task,
        D: Serialize + Send + Sync,
    {
        let date_done = if status.is_ready() {
            Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339())
        } else {
            None
        };

        TaskMeta {
            info: TaskMetaInfo {
                task_id,
                status,
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
                content_type: self._serializer(),
            },
            result: Some(result),
        }
    }

    async fn _reload_task_meta(&mut self, task_id: &TaskId) -> Option<TaskMeta> {
        let meta = self._get_task_meta_by(task_id, false).await;
        self._set_cached(task_id.clone(), meta).await
    }

    async fn _make_group_meta<D>(&self, group_id: String, result: D) -> GroupMeta<D>
    where
        D: Serialize + Send + Sync,
    {
        GroupMeta {
            info: GroupMetaInfo {
                task_id: group_id,
                date_done: Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339()),
                content_type: self._serializer(),
            },
            result: Some(result),
        }
    }

    async fn _store_group_meta(&self, group_id: &str, group_meta: GroupMeta);

    async fn _forget_group_meta_by(&self, group_id: &str);

    async fn _fetch_group_meta_by(&self, group_id: &str) -> GroupMeta;
}

#[async_trait]
impl<B: BackendProtocolLayer> ImplLayer for B {
    type Builder = B::Builder;

    fn basic_(&self) -> &BackendBasic {
        self._backend_basic()
    }

    async fn forget_(&self, task_id: &TaskId) {
        self._del_cached(task_id).await;
        self._forget_task_meta_by(task_id).await
    }

    async fn get_task_meta_by_(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self._get_task_meta_by(task_id, cache).await
    }

    async fn store_result_<D, T>(
        &self,
        task_id: &TaskId,
        result: ExecResult<D>,
        status: State,
        option: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        let task_meta = self
            ._make_task_meta(task_id.clone(), result, status, option)
            .await
            .try_into()
            .unwrap();

        self._store_task_meta(task_id, task_meta).await;
    }

    async fn store_group_result_(&self, group_id: &str, structure: ResultStructure) {
        let group_meta = self
            ._make_group_meta(
                group_id.to_owned(),
                self._serializer().try_to_value(&structure).unwrap(),
            )
            .await;

        self._store_group_meta(group_id, group_meta).await;
    }

    #[allow(unused)]
    async fn restore_group_result_(&self, group_id: &str) -> Option<ResultStructure> {
        self._fetch_group_meta_by(group_id).await.result
            .map(AnyValue::into)
            .transpose()
            .unwrap()
    }

    async fn forget_group_(&self, group_id: &str) {
        self._forget_group_meta_by(group_id).await;
    }

    async fn get_group_meta_by_(&self, group_id: &str) -> GroupMeta {
        self._fetch_group_meta_by(group_id).await
    }
}
