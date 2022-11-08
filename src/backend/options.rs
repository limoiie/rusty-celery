use std::collections::HashMap;

use typed_builder::TypedBuilder;

use crate::protocol::Exc;
use crate::prelude::Task;
use crate::protocol::{State, Traceback};
use crate::task::Request;

#[derive(Clone, Default)]
pub struct StoreOptions<'request, T: Task> {
    pub(crate) traceback: Option<Traceback>,
    pub(crate) request: Option<&'request Request<T>>,
}

impl<'require, T: Task> StoreOptions<'require, T> {
    pub fn with_request(request: &'require Request<T>) -> Self {
        Self {
            traceback: None,
            request: Some(request),
        }
    }
}

#[derive(TypedBuilder)]
pub struct WaitOptions {
    pub(crate) timeout: Option<chrono::Duration>,
    pub(crate) interval: Option<chrono::Duration>,
}

#[derive(TypedBuilder)]
pub struct MarkStartOptions<'request, T: Task> {
    #[builder(default = State::STARTED)]
    pub(crate) status: State,
    pub(crate) meta: HashMap<String, String>,
    pub(crate) store: StoreOptions<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkDoneOptions<'returns, 'request, T: Task> {
    #[builder(default = State::SUCCESS)]
    pub(crate) status: State,
    pub(crate) result: &'returns T::Returns,
    #[builder(default = true)]
    pub(crate) store_result: bool,
    pub(crate) store: StoreOptions<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkFailureOptions<'request, T: Task> {
    #[builder(default = State::FAILURE)]
    pub(crate) status: State,
    pub(crate) exc: Exc,
    #[builder(default = false)]
    pub(crate) call_errbacks: bool,
    #[builder(default = true)]
    pub(crate) store_result: bool,
    pub(crate) store: StoreOptions<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkRevokeOptions<'request, T: Task> {
    #[builder(default = State::REVOKED)]
    pub(crate) status: State,
    pub(crate) reason: String,
    #[builder(default = true)]
    pub(crate) store_result: bool,
    pub(crate) store: StoreOptions<'request, T>,
}

#[derive(TypedBuilder)]
pub struct MarkRetryOptions<'request, T: Task> {
    #[builder(default = State::RETRY)]
    pub(crate) status: State,
    pub(crate) exc: Exc,
    pub(crate) store: StoreOptions<'request, T>,
}
