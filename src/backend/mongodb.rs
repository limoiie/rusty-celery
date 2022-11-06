use crate::backend::{
    BackendBuilder, BaseBackendProtocol, BaseCached, BaseTranslator, StoreOption, TaskId, TaskMeta,
    Traceback,
};
use crate::error::BackendError;
use crate::kombu_serde::{AnyValue, SerializerKind};
use crate::prelude::Task;
use crate::states::State;
use async_trait::async_trait;
use chrono::Duration;
use mongodb::bson::{doc, DateTime};
use mongodb::options::FindOneAndReplaceOptions;
use mongodb::{Client, Collection, Database};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MongoTaskMeta {
    _id: String,
    status: State,
    result: String,
    traceback: Option<Traceback>,
    children: Vec<String>,

    date_done: Option<DateTime>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_id: Option<String>,

    // extend request meta
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kwargs: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    worker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<String>,
}

impl MongoTaskMeta {
    fn into_task_meta(self, serializer: SerializerKind) -> TaskMeta {
        TaskMeta {
            task_id: self._id.to_string(),
            status: self.status,
            result: serializer.load(&self.result),
            traceback: self.traceback,
            children: self.children,
            date_done: self
                .date_done
                .map(DateTime::try_to_rfc3339_string)
                .map(Result::unwrap),
            group_id: self.group_id,
            parent_id: self.parent_id,
            name: self.name,
            args: self.args,
            kwargs: self.kwargs,
            worker: self.worker,
            retries: self.retries,
            queue: self.queue,
        }
    }

    fn from_task_meta(task_meta: TaskMeta, serializer: SerializerKind) -> Self {
        MongoTaskMeta {
            _id: task_meta.task_id.clone(),
            status: task_meta.status,
            result: serializer.dump(&task_meta.result).2,
            traceback: task_meta.traceback,
            children: task_meta.children,
            date_done: task_meta
                .date_done
                .map(DateTime::parse_rfc3339_str)
                .map(Result::unwrap),
            group_id: task_meta.group_id,
            parent_id: task_meta.parent_id,
            name: task_meta.name,
            args: task_meta.args,
            kwargs: task_meta.kwargs,
            worker: task_meta.worker,
            retries: task_meta.retries,
            queue: task_meta.queue,
        }
    }
}

pub struct MongoDbBackendBuilder {
    url: String,
    result_serializer: SerializerKind,
    expiration_in_seconds: Option<u32>,
}

#[async_trait]
impl BackendBuilder for MongoDbBackendBuilder {
    type Backend = MongoDbBackend;

    fn new(backend_url: &str) -> Self {
        Self {
            url: backend_url.to_string(),
            result_serializer: SerializerKind::JSON,
            expiration_in_seconds: None,
        }
    }

    fn result_serializer(mut self, kind: SerializerKind) -> Self {
        self.result_serializer = kind;
        self
    }

    fn result_expires(mut self, expiration: Option<Duration>) -> Self {
        self.expiration_in_seconds = expiration.map(|d| d.num_seconds() as u32);
        self
    }

    async fn build(&self) -> Result<Self::Backend, BackendError> {
        let client = Client::with_uri_str(self.url.as_str()).await?;

        Ok(MongoDbBackend {
            url: self.url.clone(),
            result_serializer: self.result_serializer,
            expiration_in_seconds: self.expiration_in_seconds,
            connection: client,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        })
    }
}

pub struct MongoDbBackend {
    url: String,
    result_serializer: SerializerKind,
    expiration_in_seconds: Option<u32>,
    connection: Client,
    cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

impl MongoDbBackend {
    const DATABASE_NAME: &'static str = "celery";
    const TASK_META_COL: &'static str = "celery_taskmeta";
    // const GROUP_META_COL: &'static str = "celery_groupmeta";

    fn database(&self) -> Database {
        self.connection.database(Self::DATABASE_NAME)
    }

    // todo: cache?
    fn collection(&self) -> Collection<MongoTaskMeta> {
        // todo: create index on field date_done?
        self.database()
            .collection::<MongoTaskMeta>(Self::TASK_META_COL)
    }
}

#[async_trait]
impl BaseCached for MongoDbBackend {
    fn __parse_url(&self) -> Option<url::Url> {
        url::Url::parse(self.url.as_str()).ok().and_then(|url| {
            if url.scheme().contains("mongo") {
                Some(url)
            } else {
                None
            }
        })
    }

    fn expires_in_seconds(&self) -> Option<u32> {
        self.expiration_in_seconds
    }

    async fn cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>> {
        self.cache.lock().await
    }
}

impl BaseTranslator for MongoDbBackend {
    fn serializer(&self) -> SerializerKind {
        self.result_serializer
    }
}

#[async_trait]
impl BaseBackendProtocol for MongoDbBackend {
    type Builder = MongoDbBackendBuilder;

    async fn _store_result<T>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        status: State,
        option: &StoreOption<T>,
    ) where
        T: Task,
    {
        let task_meta = MongoTaskMeta::from_task_meta(
            Self::__make_task_meta(task_id.clone(), data, status, option).await,
            self.serializer(),
        );

        let opt = FindOneAndReplaceOptions::builder().upsert(true).build();

        self.collection()
            .find_one_and_replace(doc! {"_id": task_id}, task_meta, opt)
            .await
            .unwrap();
    }

    async fn __forget_task_meta_by(&self, task_id: &TaskId) {
        self.collection()
            .delete_one(doc! {"_id": task_id}, None)
            .await
            .unwrap();
    }

    async fn __fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta {
        let mut cursor = self
            .collection()
            .find(doc! {"_id": task_id}, None)
            .await
            .unwrap();

        if cursor.advance().await.unwrap() {
            return cursor
                .deserialize_current()
                .unwrap()
                .into_task_meta(self.serializer());
        }

        TaskMeta {
            status: State::PENDING,
            result: self.serializer().data_to_value(&None::<()>),
            ..TaskMeta::default()
        }
    }

    fn __decode_task_meta(&self, _payload: String) -> TaskMeta {
        unimplemented!(
            "TaskMeta is not stored as serialized in MongoDbBackend, \
hence never need to decode it as a whole."
        )
    }
}
