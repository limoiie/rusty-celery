use std::collections::HashSet;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum State {
    SUCCESS,
    FAILURE,
    IGNORED,
    REVOKED,
    STARTED,
    RECEIVED,
    REJECTED,
    RETRY,
    #[default]
    PENDING,
}

pub static READY_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::SUCCESS, State::FAILURE, State::REVOKED]));

pub static EXCEPTION_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::RETRY, State::FAILURE, State::REVOKED]));

pub static PROPAGATE_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::FAILURE, State::REVOKED]));

impl State {
    pub fn is_ready(&self) -> bool {
        READY_STATES.contains(self)
    }

    pub fn is_exception(&self) -> bool {
        EXCEPTION_STATES.contains(self)
    }

    pub fn is_successful(&self) -> bool {
        matches!(self, State::SUCCESS)
    }
}
