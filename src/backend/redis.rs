use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Duration;
use redis::aio::ConnectionManager;
use redis::Client;
use tokio::sync::{Mutex, MutexGuard};

use crate::backend::key_value_store::BaseKeyValueStore;
use crate::backend::{BackendBuilder, BaseCached, BaseTranslator, Key, TaskMeta};
use crate::error::BackendError;
use crate::kombu_serde::SerializerKind;

pub struct RedisBackendBuilder {
    url: String,
    result_serializer: SerializerKind,
    expiration_in_seconds: Option<u32>,
}

pub struct RedisBackend {
    url: String,
    result_serializer: SerializerKind,
    expiration_in_seconds: Option<u32>,
    manager: ConnectionManager,
    cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    type Backend = RedisBackend;

    fn new(backend_url: &str) -> Self {
        Self {
            url: backend_url.to_owned(),
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
        let client = Client::open(self.url.as_str())
            .map_err(|_| BackendError::InvalidBackendUrl(self.url.clone()))?;

        let manager = client.get_tokio_connection_manager().await?;

        Ok(RedisBackend {
            url: self.url.clone(),
            result_serializer: self.result_serializer,
            expiration_in_seconds: self.expiration_in_seconds,
            manager,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        })
    }
}

impl BaseTranslator for RedisBackend {
    fn serializer(&self) -> SerializerKind {
        self.result_serializer
    }
}

#[async_trait]
impl BaseCached for RedisBackend {
    fn __parse_url(&self) -> Option<url::Url> {
        redis::parse_redis_url(&self.url[..])
    }

    fn expires_in_seconds(&self) -> Option<u32> {
        self.expiration_in_seconds
    }

    async fn cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>> {
        self.cache.lock().await
    }
}

const MAX_BYTES: usize = 536870912;

#[async_trait]
impl BaseKeyValueStore for RedisBackend {
    type Builder = RedisBackendBuilder;

    async fn get(&self, key: Key) -> Option<Vec<u8>> {
        redis::cmd("GET")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn mget(&self, keys: &[Key]) -> Option<Vec<Vec<u8>>> {
        redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn set(&self, key: Key, value: &[u8]) -> Option<()> {
        if value.len() > MAX_BYTES {
            panic!("Value too long for Redis Backend.")
        }

        let mut pipe = redis::pipe();
        let pipe = if let Some(expiration_in_seconds) = self.expiration_in_seconds {
            pipe.cmd("SETEX")
                .arg(&key)
                .arg(expiration_in_seconds)
                .arg(value)
                .ignore()
        } else {
            pipe.cmd("SET").arg(&key).arg(value).ignore()
        };

        pipe.cmd("PUBLISH")
            .arg(&key)
            .arg(value)
            .ignore()
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn delete(&self, key: Key) -> Option<u32> {
        redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn incr(&self, key: Key) -> Option<i32> {
        redis::cmd("INCR")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn expire(&self, key: Key, value: u32) -> Option<bool> {
        redis::cmd("EXPIRE")
            .arg(&key)
            .arg(value)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }
}
