use async_trait::async_trait;
use bstr::ByteVec;

use crate::backend::inner::{BackendBasicLayer, BackendProtocolLayer, BackendSerdeLayer};
use crate::backend::BackendBuilder;
use crate::protocol::TaskId;
use crate::protocol::{State, TaskMeta};

pub type Key = String;

#[async_trait]
pub trait KeyValueStoreLayer: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    const KEY_PREFIX_TASK: &'static str = "celery-task-meta-";

    const KEY_PREFIX_GROUP: &'static str = "celery-taskset-meta-";

    const KEY_PREFIX_CHORD: &'static str = "chord-unlock-";

    async fn _get(&self, key: Key) -> Option<Vec<u8>>;

    async fn _mget(&self, keys: &[Key]) -> Option<Vec<Vec<u8>>>;

    async fn _set(&self, key: Key, value: &[u8]) -> Option<()>;

    async fn _delete(&self, key: Key) -> Option<u32>;

    async fn _incr(&self, key: Key) -> Option<i32>;

    async fn _expire(&self, key: Key, value: u32) -> Option<bool>;

    async fn _set_with_state(&self, key: Key, value: &[u8], _state: State) -> Option<()> {
        self._set(key, value).await
    }

    fn _get_key_for_task(&self, task_id: &TaskId, key: Option<Key>) -> String {
        self._get_key_for(Self::KEY_PREFIX_TASK, task_id, key)
    }

    fn _get_key_for_group(&self, group_id: &TaskId, key: Option<Key>) -> String {
        self._get_key_for(Self::KEY_PREFIX_GROUP, group_id, key)
    }

    fn _get_key_for_chord(&self, group_id: &TaskId, key: Option<Key>) -> String {
        self._get_key_for(Self::KEY_PREFIX_CHORD, group_id, key)
    }

    fn _get_key_for(&self, prefix: &str, id: &TaskId, key: Option<Key>) -> String {
        [prefix, id, key.unwrap_or_default().as_str()].join("")
    }

    fn _strip_prefix(&self, key: Key) -> String {
        for prefix in [
            Self::KEY_PREFIX_TASK,
            Self::KEY_PREFIX_GROUP,
            Self::KEY_PREFIX_CHORD,
        ] {
            if let Some(origin_key) = key.strip_prefix(prefix) {
                return origin_key.to_owned();
            }
        }
        key
    }
}

#[async_trait]
impl<B> BackendProtocolLayer for B
where
    B: KeyValueStoreLayer + BackendBasicLayer + BackendSerdeLayer,
{
    type Builder = B::Builder;

    async fn _store_task_meta(&self, task_id: &TaskId, task_meta: TaskMeta) {
        let remote_task_meta = self._fetch_task_meta_by(task_id).await;
        if !remote_task_meta.status.is_successful() {
            let data = self._encode(&task_meta);
            log::debug!("Store task meta: {}", data);

            self._set_with_state(
                self._get_key_for_task(task_id, None),
                data.as_bytes(),
                task_meta.status,
            )
            .await;
        }
    }

    async fn _forget_task_meta_by(&self, task_id: &TaskId) {
        self._delete(self._get_key_for_task(task_id, None))
            .await
            .unwrap();
    }

    async fn _fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta {
        if let Some(meta) = self._get(self._get_key_for_task(task_id, None)).await {
            if !meta.is_empty() {
                return self._decode_task_meta(meta.into_string_lossy());
            }
        }

        TaskMeta {
            status: State::PENDING,
            result: self._serializer().data_to_value(&None::<()>),
            ..TaskMeta::default()
        }
    }

    fn _decode_task_meta(&self, payload: String) -> TaskMeta {
        // todo:
        //   convert exception to rust exception
        self._decode::<TaskMeta>(payload)
    }
}
