use async_trait::async_trait;
use serde::Serialize;

use crate::backend::options::{
    MarkDoneOptions, MarkFailureOptions, MarkRetryOptions, MarkRevokeOptions, MarkStartOptions,
    StoreOptions, WaitOptions,
};
use crate::backend::{Backend, BackendBasic, BackendBuilder, BackendResult};
use crate::error::{BackendError, TaskError, TraceError};
use crate::protocol::{ExecResult, State, TaskId, TaskMeta, GroupMeta};
use crate::result::ResultStructure;
use crate::task::{Request, Task};

#[async_trait]
pub trait ImplLayer: Send + Sync + Sized {
    type Builder: BackendBuilder<Backend = Self>;

    fn basic_(&self) -> &BackendBasic;

    async fn wait_(&self, task_id: &TaskId, option: WaitOptions) -> BackendResult<TaskMeta> {
        let interval = option
            .interval
            .or_else(|| Some(chrono::Duration::milliseconds(500)))
            .as_ref()
            .map(chrono::Duration::to_std)
            .map(Result::unwrap)
            .unwrap();

        self.ensure_not_eager_();

        let start_timing = chrono::Local::now();
        loop {
            let task_meta = self.get_task_meta_by_(task_id, true).await;
            if task_meta.is_ready() {
                return Ok(task_meta);
            }

            tokio::time::sleep(interval).await;
            if let Some(timeout) = option.timeout {
                if chrono::Local::now() > start_timing + timeout {
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
        let err = ExecResult::<()>::Err(option.exc);

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
        let err = ExecResult::<()>::Err(exc);

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
        let err = ExecResult::<()>::Err(option.exc);

        self.store_result_(task_id, err, option.status, &option.store)
            .await;
    }

    async fn get_task_meta_by_(&self, task_id: &TaskId, cache: bool) -> TaskMeta;

    async fn store_result_<D, T>(
        &self,
        task_id: &TaskId,
        result: ExecResult<D>,
        status: State,
        store: &StoreOptions<T>,
    ) where
        D: Serialize + Send + Sync,
        T: Task;

    async fn store_group_result_(&self, group_id: &str, structure: ResultStructure);

    async fn restore_group_result_(&self, group_id: &str) -> Option<ResultStructure>;

    async fn forget_group_(&self, group_id: &str);

    async fn get_group_meta_by_(&self, group_id: &str) -> GroupMeta;

    #[allow(unused)]
    async fn on_chord_part_return_<T, D>(
        &self,
        result: ExecResult<D>,
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
}

#[async_trait]
impl<L: ImplLayer> Backend for L {
    type Builder = L::Builder;

    fn basic(&self) -> &BackendBasic {
        self.basic_()
    }

    async fn wait(&self, task_id: &TaskId, option: WaitOptions) -> BackendResult<TaskMeta> {
        self.wait_(task_id, option).await
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

    async fn save_group(&self, group_id: &str, group_structure: ResultStructure) {
        self.store_group_result_(group_id, group_structure).await
    }

    async fn forget_group(&self, group_id: &str) {
        self.forget_group_(group_id).await
    }

    async fn get_group(&self, group_id: &str) -> Option<ResultStructure> {
        self.restore_group_result_(group_id).await
    }

    async fn get_group_meta_by(&self, group_id: &str) -> GroupMeta {
        self.get_group_meta_by_(group_id).await
    }

    async fn store_result<D, T>(
        &self,
        task_id: &TaskId,
        result: ExecResult<D>,
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
        result: ExecResult<D>,
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
