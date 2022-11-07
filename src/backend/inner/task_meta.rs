use crate::backend::{TaskId, Traceback};
use crate::kombu_serde::AnyValue;
use crate::states::State;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMeta {
    pub task_id: TaskId,
    pub status: State,
    pub result: AnyValue,
    pub traceback: Option<Traceback>,
    pub children: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_done: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,

    // extend request meta
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

impl TaskMeta {
    pub fn is_ready(&self) -> bool {
        self.status.is_ready()
    }

    pub fn is_exception(&self) -> bool {
        self.status.is_exception()
    }

    pub fn is_successful(&self) -> bool {
        self.status.is_successful()
    }
}
