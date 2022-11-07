use std::cell::RefCell;
use std::collections::HashMap;

use async_trait::async_trait;
use tokio::sync::MutexGuard;

use crate::backend::BackendBasic;
use crate::protocol::{TaskId, TaskMeta};

#[async_trait]
pub trait BackendBasicLayer: Send + Sync + Sized {
    fn _backend_basic(&self) -> &BackendBasic;

    fn _expires_in_seconds(&self) -> Option<u32> {
        self._backend_basic().expiration_in_seconds
    }

    async fn _cached(&self) -> MutexGuard<RefCell<HashMap<String, TaskMeta>>> {
        self._backend_basic().cache.lock().await
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
