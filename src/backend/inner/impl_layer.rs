use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::backend::options::{
    MarkDoneOptions, MarkFailureOptions, MarkRetryOptions, MarkRevokeOptions, MarkStartOptions,
    StoreOptions, WaitForOptions,
};
use crate::backend::{
    Backend, BackendBasic, BackendBuilder, BackendResult, TaskId, TaskMeta, TaskResult,
};
use crate::error::{BackendError, TaskError, TraceError};
use crate::states::State;
use crate::task::{Request, Task};

#[async_trait]
pub trait ImplLayer: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    fn basic_(&self) -> &BackendBasic;

    fn safe_url_(&self) -> String;

    async fn wait_for_(&self, task_id: &TaskId, option: WaitForOptions) -> BackendResult<TaskMeta> {
        let interval = option
            .interval
            .or(Some(std::time::Duration::from_millis(500)))
            .map(|interval| core::time::Duration::new(0, interval.as_nanos() as u32))
            .unwrap();

        let timeout = option
            .timeout
            .map(|timeout| core::time::Duration::new(0, timeout.as_nanos() as u32));

        self.ensure_not_eager_();

        let start_timing = std::time::SystemTime::now();
        loop {
            let task_meta = self.get_task_meta_by_(task_id, true).await;
            if task_meta.is_ready() {
                return Ok(task_meta);
            }

            tokio::time::sleep(interval).await;
            if let Some(timeout) = timeout {
                if std::time::SystemTime::now() > start_timing + timeout {
                    return Err(BackendError::TimeoutError);
                }
            }
        }
    }

    async fn forget_(&self, task_id: &TaskId);

    async fn mark_as_started_<'s, T>(&'s self, task_id: &TaskId, option: MarkStartOptions<'s, T>)
    where
        T: Task,
    {
        let data = Ok(option.meta);
        self.store_result_(task_id, data, option.status, &option.store)
            .await
    }

    async fn mark_as_done_<'s, 'r, T>(
        &'s self,
        task_id: &TaskId,
        option: MarkDoneOptions<'s, 'r, T>,
    ) where
        T: Task,
    {
        let data = Ok(option.result);

        if option.store_result
            && !option
                .store
                .request
                .map(|r| r.ignore_result)
                .unwrap_or(false)
        {
            self.store_result_(task_id, data.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return_(data, option.status, request)
                    .await
            }
        }
    }

    async fn mark_as_failure_<'s, T>(&'s self, task_id: &TaskId, option: MarkFailureOptions<'s, T>)
    where
        T: Task,
    {
        let err = TaskResult::<()>::Err(option.exc);

        if option.store_result {
            self.store_result_(task_id, err.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return_(err, option.status, request)
                    .await
            }

            // todo: chain elem ctx -> store_result
            if option.call_errbacks {
                // todo: call err callbacks
            }
        }
    }

    async fn mark_as_revoked_<'s, T>(&'s self, task_id: &TaskId, option: MarkRevokeOptions<'s, T>)
    where
        T: Task,
    {
        let exc = TraceError::TaskError(TaskError::RevokedError(option.reason));
        let err = TaskResult::<()>::Err(exc);

        if option.store_result {
            self.store_result_(task_id, err.clone(), option.status, &option.store)
                .await;
        }

        if let Some(request) = option.store.request {
            if request.chord.is_some() {
                self.on_chord_part_return_(err, option.status, request)
                    .await
            }
        }
    }

    async fn mark_as_retry_<'s, T>(&'s self, task_id: &TaskId, option: MarkRetryOptions<'s, T>)
    where
        T: Task,
    {
        let err = TaskResult::<()>::Err(option.exc);

        self.store_result_(task_id, err, option.status, &option.store)
            .await;
    }

    async fn get_task_meta_by_(&self, task_id: &TaskId, cache: bool) -> TaskMeta;

    fn recover_result_<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>;

    async fn store_result_<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    #[allow(unused)]
    async fn on_chord_part_return_<T, D>(
        &self,
        result: TaskResult<D>,
        state: State,
        request: &Request<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        // no-op
    }

    fn ensure_not_eager_(&self) {
        // no-op
    }

    async fn cleanup_(&self) {
        // no-op
    }

    async fn process_cleanup_(&self) {
        // no-op
    }

    // todo: chord related apis
    // fn add_to_chord(&self, chord_id, result)
    // fn on_chord_part_return(&self, request, state, result, kwargs)
    // fn set_chord_size(&self, group_id, chord_size)
    // fn fallback_chord_unlock(&self, header_result, body, countdown, kwargs)
    // fn ensure_chords_allowed(&self)
    // fn apply_chord(&self, header_result_args, body, kwargs)

    // todo: group related apis
    // fn get_group_meta(&self, group_id, cache=True)
    // fn restore_group(&self, group_id, cache=True)
    // fn save_group(&self, group_id, result)
    // fn delete_group(&self, group_id)
}

#[async_trait]
impl<L: ImplLayer> Backend for L {
    type Builder = L::Builder;

    fn basic(&self) -> &BackendBasic {
        self.basic_()
    }

    fn safe_url(&self) -> String {
        self.safe_url_()
    }

    async fn wait_for(&self, task_id: &TaskId, option: WaitForOptions) -> BackendResult<TaskMeta> {
        self.wait_for_(task_id, option).await
    }

    async fn forget(&self, task_id: &TaskId) {
        self.forget_(task_id).await
    }

    async fn mark_as_started<'s, T>(&self, task_id: &TaskId, option: MarkStartOptions<'s, T>)
    where
        T: Task,
    {
        self.mark_as_started_(task_id, option).await
    }

    async fn mark_as_done<'s, 'r, T>(&self, task_id: &TaskId, option: MarkDoneOptions<'s, 'r, T>)
    where
        T: Task,
    {
        self.mark_as_done_(task_id, option).await
    }

    async fn mark_as_failure<'s, T>(&self, task_id: &TaskId, option: MarkFailureOptions<'s, T>)
    where
        T: Task,
    {
        self.mark_as_failure_(task_id, option).await
    }

    async fn mark_as_revoked<'s, T>(&self, task_id: &TaskId, option: MarkRevokeOptions<'s, T>)
    where
        T: Task,
    {
        self.mark_as_revoked_(task_id, option).await
    }

    async fn mark_as_retry<'s, T>(&self, task_id: &TaskId, option: MarkRetryOptions<'s, T>)
    where
        T: Task,
    {
        self.mark_as_retry_(task_id, option).await
    }

    async fn get_task_meta_by(&self, task_id: &TaskId, cache: bool) -> TaskMeta {
        self.get_task_meta_by_(task_id, cache).await
    }

    fn recover_result<D>(&self, task_meta: TaskMeta) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        self.recover_result_(task_meta)
    }

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: TaskResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        self.store_result_(task_id, result, status, store).await
    }

    async fn on_chord_part_return<T, D>(
        &self,
        result: TaskResult<D>,
        status: State,
        request: &Request<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task,
    {
        self.on_chord_part_return_(result, status, request).await
    }

    fn ensure_not_eager(&self) {
        self.ensure_not_eager_()
    }

    async fn cleanup(&self) {
        self.cleanup_().await
    }

    async fn process_cleanup(&self) {
        self.process_cleanup_().await
    }
}
