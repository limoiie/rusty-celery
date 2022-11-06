use crate::backend::TaskMeta;
use crate::kombu_serde::SerializerKind;
use async_trait::async_trait;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};
use crate::backend::key_value_store::Key;

pub struct BackendBasic {
    pub url: String,
    pub result_serializer: SerializerKind,
    pub expiration_in_seconds: Option<u32>,
    pub cache: Arc<Mutex<RefCell<HashMap<String, TaskMeta>>>>,
}

impl BackendBasic {
    pub fn new(backend_url: &str) -> BackendBasic {
        Self {
            url: backend_url.to_owned(),
            result_serializer: SerializerKind::JSON,
            expiration_in_seconds: None,
            cache: Arc::new(Mutex::new(RefCell::new(HashMap::new()))),
        }
    }
}

#[async_trait]
pub trait BackendBasicLayer: Send + Sync + Sized {
    fn backend_basic(&self) -> &BackendBasic;

    fn _safe_url(&self) -> String {
        match self.parse_url() {
            Some(url) => format!(
                "{}://{}:***@{}:{}/{}",
                url.scheme(),
                url.username(),
                url.host_str().unwrap(),
                url.port().unwrap(),
                url.path(),
            ),
            None => {
                log::error!("Invalid redis url.");
                String::from("")
            }
        }
    }

    fn parse_url(&self) -> Option<url::Url>;

    fn expires_in_seconds(&self) -> Option<u32> {
        self.backend_basic().expiration_in_seconds
    }

    async fn cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>> {
        self.backend_basic().cache.lock().await
    }

    async fn get_cached(&self, key: &Key) -> Option<TaskMeta> {
        let guard = self.cached().await;
        let cached = guard.borrow();
        cached.get(key).map(Clone::clone)
    }

    async fn set_cached(&self, key: Key, val: TaskMeta) -> Option<TaskMeta> {
        let mut guard = self.cached().await;
        guard.get_mut().insert(key, val)
    }

    async fn del_cached(&self, key: &Key) -> Option<TaskMeta> {
        let mut guard = self.cached().await;
        guard.get_mut().remove(key)
    }

    async fn is_cached(&self, key: &Key) -> bool {
        let guard = self.cached().await;
        let cached = guard.borrow();
        cached.contains_key(key)
    }
}
