use async_trait::async_trait;
use bstr::ByteVec;

use crate::backend::{
    BaseBackendProtocol, BaseCached, BaseTranslator, Key, TaskId, TaskMeta, Traceback,
};
use crate::kombu_serde::AnyValue;
use crate::states::State;
use crate::task::{Request, Task};

use super::BackendBuilder;

#[async_trait]
pub trait BaseKeyValueStore: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    const KEY_PREFIX_TASK: &'static str = "celery-task-meta-";

    const KEY_PREFIX_GROUP: &'static str = "celery-taskset-meta-";

    const KEY_PREFIX_CHORD: &'static str = "chord-unlock-";

    async fn get(&self, key: Key) -> Option<Vec<u8>>;

    async fn mget(&self, keys: &[Key]) -> Option<Vec<Vec<u8>>>;

    async fn set(&self, key: Key, value: &[u8]) -> Option<()>;

    async fn delete(&self, key: Key) -> Option<u32>;

    async fn incr(&self, key: Key) -> Option<i32>;

    async fn expire(&self, key: Key, value: u32) -> Option<bool>;

    async fn _set_with_state(&self, key: Key, value: &[u8], _state: State) -> Option<()> {
        self.set(key, value).await
    }

    fn get_key_for_task(&self, task_id: &TaskId, key: Option<Key>) -> String {
        self._get_key_for(Self::KEY_PREFIX_TASK, task_id, key)
    }

    fn get_key_for_group(&self, group_id: &TaskId, key: Option<Key>) -> String {
        self._get_key_for(Self::KEY_PREFIX_GROUP, group_id, key)
    }

    fn get_key_for_chord(&self, group_id: &TaskId, key: Option<Key>) -> String {
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
impl<B> BaseBackendProtocol for B
where
    B: BaseKeyValueStore + BaseCached + BaseTranslator,
{
    type Builder = B::Builder;

    async fn _store_result_wrapped_as_task_meta<T: Task>(
        &self,
        task_id: &TaskId,
        data: AnyValue,
        state: State,
        traceback: Option<Traceback>,
        request: Option<&Request<T>>,
    ) {
        let task_meta =
            Self::__make_task_meta(task_id.clone(), data, state, traceback, request).await;

        let remote_task_meta = self.__fetch_task_meta_by(task_id).await;
        if remote_task_meta.status != State::SUCCESS {
            let data = self.encode(&task_meta);
            log::debug!("Store task meta: {}", data);

            self._set_with_state(self.get_key_for_task(task_id, None), data.as_bytes(), state)
                .await;
        }
    }

    async fn __forget_task_meta_by(&mut self, task_id: &TaskId) {
        self.delete(self.get_key_for_task(task_id, None))
            .await
            .unwrap();
    }

    async fn __fetch_task_meta_by(&self, task_id: &TaskId) -> TaskMeta {
        if let Some(meta) = self.get(self.get_key_for_task(task_id, None)).await {
            if !meta.is_empty() {
                return self.__decode_task_meta(meta.into_string_lossy());
            }
        }

        TaskMeta {
            status: State::PENDING,
            result: self.serializer().to_value(&None::<()>),
            ..TaskMeta::default()
        }
    }

    fn __decode_task_meta(&self, payload: String) -> TaskMeta {
        // todo:
        //   convert exception to rust exception
        self.decode::<TaskMeta>(payload)
    }
}
