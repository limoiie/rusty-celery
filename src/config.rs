use core::marker::Sized;
use core::option::Option;
use core::option::Option::Some;

use crate::kombu_serde::SerializerKind;
use crate::protocol::MessageContentType;
use crate::task::TaskOptions;

pub struct TaskConfig {
    pub options: TaskOptions,
    pub routes: Vec<(String, String)>,
}

pub trait ConfigTask: Sized {
    fn get_task_config(&mut self) -> &mut TaskConfig;

    /// Set an app-level time limit for tasks (see [`TaskOptions::time_limit`]).
    fn task_time_limit(mut self, task_time_limit: u32) -> Self {
        self.get_task_config().options.time_limit = Some(task_time_limit);
        self
    }

    /// Set an app-level hard time limit for tasks (see [`TaskOptions::hard_time_limit`]).
    ///
    /// *Note that this is really only for compatability with Python workers*.
    /// `time_limit` and `hard_time_limit` are treated the same by Rust workers, and if both
    /// are set, the minimum of the two will be used.
    fn task_hard_time_limit(mut self, task_hard_time_limit: u32) -> Self {
        self.get_task_config().options.hard_time_limit = Some(task_hard_time_limit);
        self
    }

    /// Set an app-level maximum number of retries for tasks (see [`TaskOptions::max_retries`]).
    fn task_max_retries(mut self, task_max_retries: u32) -> Self {
        self.get_task_config().options.max_retries = Some(task_max_retries);
        self
    }

    /// Set an app-level minimum retry delay for tasks (see [`TaskOptions::min_retry_delay`]).
    fn task_min_retry_delay(mut self, task_min_retry_delay: u32) -> Self {
        self.get_task_config().options.min_retry_delay = Some(task_min_retry_delay);
        self
    }

    /// Set an app-level maximum retry delay for tasks (see [`TaskOptions::max_retry_delay`]).
    fn task_max_retry_delay(mut self, task_max_retry_delay: u32) -> Self {
        self.get_task_config().options.max_retry_delay = Some(task_max_retry_delay);
        self
    }

    /// Set whether by default `UnexpectedError`s should be retried for (see
    /// [`TaskOptions::retry_for_unexpected`]).
    fn task_retry_for_unexpected(mut self, retry_for_unexpected: bool) -> Self {
        self.get_task_config().options.retry_for_unexpected = Some(retry_for_unexpected);
        self
    }

    /// Set whether by default a task is acknowledged before or after execution (see
    /// [`TaskOptions::acks_late`]).
    fn acks_late(mut self, acks_late: bool) -> Self {
        self.get_task_config().options.acks_late = Some(acks_late);
        self
    }

    /// Set default serialization format a task will have (see [`TaskOptions::content_type`]).
    fn task_content_type(mut self, content_type: MessageContentType) -> Self {
        self.get_task_config().options.content_type = Some(content_type);
        self
    }

    /// Add a routing rule.
    fn task_route(mut self, pattern: &str, queue: &str) -> Self {
        self.get_task_config()
            .routes
            .push((pattern.into(), queue.into()));
        self
    }
}

pub struct BrokerConfig {
    pub url: String,
    pub default_queue: String,
    pub prefetch_count: Option<u16>,
    pub heartbeat: Option<Option<u16>>,
    pub connection_timeout: u32,
    pub connection_retry: bool,
    pub connection_max_retries: u32,
    pub connection_retry_delay: u32,
}

pub trait ConfigBroker: Sized {
    fn get_broker_config(&mut self) -> &mut BrokerConfig;

    /// Set url for connecting to the broker.
    fn broker_url(mut self, url: String) -> Self {
        self.get_broker_config().url = url;
        self
    }

    /// Set the name of the default queue to something other than "celery".
    fn default_queue(mut self, queue_name: &str) -> Self {
        self.get_broker_config().default_queue = queue_name.into();
        self
    }

    /// Set the prefetch count. The default value depends on the broker implementation,
    /// but it's recommended that you always set this to a value that works best
    /// for your application.
    ///
    /// This may take some tuning, as it depends on a lot of factors, such
    /// as whether your tasks are IO bound (higher prefetch count is better) or CPU bound (lower
    /// prefetch count is better).
    fn prefetch_count(mut self, prefetch_count: u16) -> Self {
        self.get_broker_config().prefetch_count = Some(prefetch_count);
        self
    }

    /// Set the broker heartbeat. The default value depends on the broker implementation.
    fn heartbeat(mut self, heartbeat: Option<u16>) -> Self {
        self.get_broker_config().heartbeat = Some(heartbeat);
        self
    }

    /// Set a timeout in seconds before giving up establishing a connection to a broker.
    fn broker_connection_timeout(mut self, timeout: u32) -> Self {
        self.get_broker_config().connection_timeout = timeout;
        self
    }

    /// Set whether or not to automatically try to re-establish connection to the AMQP broker.
    fn broker_connection_retry(mut self, retry: bool) -> Self {
        self.get_broker_config().connection_retry = retry;
        self
    }

    /// Set the maximum number of retries before we give up trying to re-establish connection
    /// to the AMQP broker.
    fn broker_connection_max_retries(mut self, max_retries: u32) -> Self {
        self.get_broker_config().connection_max_retries = max_retries;
        self
    }

    /// Set the number of seconds to wait before re-trying the connection with the broker.
    fn broker_connection_retry_delay(mut self, retry_delay: u32) -> Self {
        self.get_broker_config().connection_retry_delay = retry_delay;
        self
    }
}

pub struct BackendConfig {
    pub url: String,
    pub result_serializer: SerializerKind,
    pub result_expires: Option<chrono::Duration>,
}

pub trait ConfigBackend: Sized {
    fn get_backend_config(&mut self) -> &mut BackendConfig;

    /// Set url for connecting to the backend.
    fn backend_url(mut self, url: String) -> Self {
        self.get_backend_config().url = url;
        self
    }

    /// Set the serializer for storing results into the backend.
    fn result_serializer(mut self, serializer: SerializerKind) -> Self {
        self.get_backend_config().result_serializer = serializer;
        self
    }

    /// Set a expiration for results in the backend.
    fn result_expires(mut self, expiration: chrono::Duration) -> Self {
        self.get_backend_config().result_expires = Some(expiration);
        self
    }
}
