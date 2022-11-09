use async_trait::async_trait;
use mongodb::bson::doc;
use mongodb::options::FindOneAndReplaceOptions;
use mongodb::{Client, Collection, Database};
use serde::{Deserialize, Serialize};

use crate::backend::inner::{BackendBasicLayer, BackendProtocolLayer, BackendSerdeLayer};
use crate::backend::{BackendBasic, BackendBuilder};
use crate::error::BackendError;
use crate::protocol::ContentType;
use crate::protocol::{TaskId, TaskMeta, TaskMetaInfo};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MongoTaskMeta {
    _id: String,
    #[serde(flatten)]
    info: TaskMetaInfo,
    result: String,
}

impl MongoTaskMeta {
    fn into_task_meta(self, content_type: ContentType) -> TaskMeta {
        TaskMeta {
            info: TaskMetaInfo {
                task_id: self._id.to_string(),
                date_done: self.info.date_done,
                ..self.info
            },
            result: content_type.load(&self.result),
        }
    }

    fn from_task_meta(task_meta: TaskMeta, content_type: ContentType) -> Self {
        MongoTaskMeta {
            _id: task_meta.info.task_id.clone(),
            info: TaskMetaInfo {
                date_done: task_meta.info.date_done,
                ..task_meta.info
            },
            result: content_type.dump(&task_meta.result),
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

    fn parse_url(&self) -> Option<url::Url> {
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
    fn _backend_basic(&self) -> &BackendBasic {
        &self.backend_basic
    }
}

impl BackendSerdeLayer for MongoDbBackend {
    fn _serializer(&self) -> ContentType {
        self.backend_basic.result_serializer
    }
}

#[async_trait]
impl BackendProtocolLayer for MongoDbBackend {
    type Builder = MongoDbBackendBuilder;

    async fn _store_task_meta(&self, task_id: &TaskId, task_meta: TaskMeta) {
        let task_meta = MongoTaskMeta::from_task_meta(task_meta, self._serializer());

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

        TaskMeta::default()
    }

    fn _decode_task_meta(&self, _payload: String) -> TaskMeta {
        unimplemented!(
            "TaskMeta is not stored as serialized in MongoDbBackend, \
hence never need to decode it as a whole."
        )
    }
}
