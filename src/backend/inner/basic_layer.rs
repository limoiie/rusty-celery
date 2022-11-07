use crate::backend::{TaskId, TaskMeta};
use crate::kombu_serde::SerializerKind;
use async_trait::async_trait;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

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
        match self._parse_url() {
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

    fn _parse_url(&self) -> Option<url::Url>;

    fn _expires_in_seconds(&self) -> Option<u32> {
        self.backend_basic().expiration_in_seconds
    }

    async fn _cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>> {
        self.backend_basic().cache.lock().await
    }

    async fn _get_cached(&self, task_id: &TaskId) -> Option<TaskMeta> {
        let guard = self._cached().await;
        let cached = guard.borrow();
        cached.get(task_id).map(Clone::clone)
    }

    async fn _set_cached(&self, task_id: TaskId, val: TaskMeta) -> Option<TaskMeta> {
        let mut guard = self._cached().await;
        guard.get_mut().insert(task_id, val)
    }

    async fn _del_cached(&self, task_id: &TaskId) -> Option<TaskMeta> {
        let mut guard = self._cached().await;
        guard.get_mut().remove(task_id)
    }

    async fn _is_cached(&self, task_id: &TaskId) -> bool {
        let guard = self._cached().await;
        let cached = guard.borrow();
        cached.contains_key(task_id)
    }
}
