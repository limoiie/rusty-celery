use async_trait::async_trait;
use mongodb::bson::{doc, DateTime};
use mongodb::options::FindOneAndReplaceOptions;
use mongodb::{Client, Collection, Database};
use serde::{Deserialize, Serialize};

use crate::backend::inner::{BackendBasicLayer, BackendProtocolLayer, BackendSerdeLayer};
use crate::backend::options::StoreOptions;
use crate::backend::{BackendBasic, BackendBuilder, TaskId, TaskMeta, Traceback};
use crate::error::BackendError;
use crate::kombu_serde::{AnyValue, SerializerKind};
use crate::prelude::Task;
use crate::states::State;

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
    backend_basic: BackendBasic,
}

#[async_trait]
impl BackendBuilder for MongoDbBackendBuilder {
    type Backend = MongoDbBackend;

    fn new(backend_url: &str) -> Self {
        Self {
            backend_basic: BackendBasic::new(backend_url),
        }
    }

    fn backend_basic(&mut self) -> &mut BackendBasic {
        &mut self.backend_basic
    }

    async fn build(self) -> Result<Self::Backend, BackendError> {
        let client = Client::with_uri_str(self.backend_basic.url.as_str()).await?;

        Ok(MongoDbBackend {
            backend_basic: self.backend_basic,
            connection: client,
        })
    }
}

pub struct MongoDbBackend {
    backend_basic: BackendBasic,
    connection: Client,
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
impl BackendBasicLayer for MongoDbBackend {
    fn backend_basic(&self) -> &BackendBasic {
        &self.backend_basic
    }

    fn _parse_url(&self) -> Option<url::Url> {
        url::Url::parse(self.backend_basic.url.as_str())
            .ok()
            .and_then(|url| {
                if url.scheme().contains("mongo") {
                    Some(url)
                } else {
                    None
                }
            })
    }
}

impl BackendSerdeLayer for MongoDbBackend {
    fn _serializer(&self) -> SerializerKind {
        self.backend_basic.result_serializer
    }
}

#[async_trait]
impl BackendProtocolLayer for MongoDbBackend {
    type Builder = MongoDbBackendBuilder;

    async fn _store_result<T>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        status: State,
        option: &StoreOptions<T>,
    ) where
        T: Task,
    {
        let task_meta = MongoTaskMeta::from_task_meta(
            Self::_make_task_meta(task_id.clone(), data, status, option).await,
            self._serializer(),
        );

        let opt = FindOneAndReplaceOptions::builder().upsert(true).build();

        self.collection()
            .find_one_and_replace(doc! {"_id": task_id}, task_meta, opt)
            .await
            .unwrap();
    }

    async fn _forget_task_meta_by(&self, task_id: &TaskId) {
        self.collection()
            .delete_one(doc! {"_id": task_id}, None)
            .await
            .unwrap();
    }

    async fn _fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta {
        let mut cursor = self
            .collection()
            .find(doc! {"_id": task_id}, None)
            .await
            .unwrap();

        if cursor.advance().await.unwrap() {
            return cursor
                .deserialize_current()
                .unwrap()
                .into_task_meta(self._serializer());
        }

        TaskMeta {
            status: State::PENDING,
            result: self._serializer().data_to_value(&None::<()>),
            ..TaskMeta::default()
        }
    }

    fn _decode_task_meta(&self, _payload: String) -> TaskMeta {
        unimplemented!(
            "TaskMeta is not stored as serialized in MongoDbBackend, \
hence never need to decode it as a whole."
        )
    }
}
