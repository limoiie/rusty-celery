use async_trait::async_trait;
use redis::aio::ConnectionManager;
use redis::Client;

use crate::backend::inner::key_value_store_layer::Key;
use crate::backend::inner::{BackendBasicLayer, BackendSerdeLayer, KeyValueStoreLayer};
use crate::backend::{BackendBasic, BackendBuilder};
use crate::error::BackendError;
use crate::kombu_serde::SerializerKind;

pub struct RedisBackendBuilder {
    backend_basic: BackendBasic,
}

pub struct RedisBackend {
    backend_basic: BackendBasic,
    manager: ConnectionManager,
}

#[async_trait]
impl BackendBuilder for RedisBackendBuilder {
    type Backend = RedisBackend;

    fn new(backend_url: &str) -> Self {
        Self {
            backend_basic: BackendBasic::new(backend_url),
        }
    }

    fn backend_basic(&mut self) -> &mut BackendBasic {
        &mut self.backend_basic
    }

    async fn build(self) -> Result<Self::Backend, BackendError> {
        let client = Client::open(self.backend_basic.url.as_str())
            .map_err(|_| BackendError::InvalidBackendUrl(self.backend_basic.url.clone()))?;

        let manager = client.get_tokio_connection_manager().await?;

        Ok(RedisBackend {
            backend_basic: self.backend_basic,
            manager,
        })
    }
}

impl BackendSerdeLayer for RedisBackend {
    fn _serializer(&self) -> SerializerKind {
        self.backend_basic.result_serializer
    }
}

#[async_trait]
impl BackendBasicLayer for RedisBackend {
    fn backend_basic(&self) -> &BackendBasic {
        &self.backend_basic
    }

    fn _parse_url(&self) -> Option<url::Url> {
        redis::parse_redis_url(&self.backend_basic.url[..])
    }
}

const MAX_BYTES: usize = 536870912;

#[async_trait]
impl KeyValueStoreLayer for RedisBackend {
    type Builder = RedisBackendBuilder;

    async fn _get(&self, key: Key) -> Option<Vec<u8>> {
        redis::cmd("GET")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn _mget(&self, keys: &[Key]) -> Option<Vec<Vec<u8>>> {
        redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn _set(&self, key: Key, value: &[u8]) -> Option<()> {
        if value.len() > MAX_BYTES {
            panic!("Value too long for Redis Backend.")
        }

        let mut pipe = redis::pipe();
        let pipe = if let Some(expiration_in_seconds) = self.backend_basic.expiration_in_seconds {
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

    async fn _delete(&self, key: Key) -> Option<u32> {
        redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn _incr(&self, key: Key) -> Option<i32> {
        redis::cmd("INCR")
            .arg(&key)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }

    async fn _expire(&self, key: Key, value: u32) -> Option<bool> {
        redis::cmd("EXPIRE")
            .arg(&key)
            .arg(value)
            .query_async(&mut self.manager.clone())
            .await
            .ok()
    }
}
