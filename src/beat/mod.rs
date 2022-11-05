//! Celery [`Beat`] is an app that can automatically produce tasks at scheduled times.
//!
//! ### Terminology
//!
//! This is the terminology used in this module (with references to the corresponding names
//! in the Python implementation):
//! - schedule: the strategy used to decide when a task must be executed (each scheduled
//!   task has its own schedule);
//! - scheduled task: a task together with its schedule (it more or less corresponds to
//!   a *schedule entry* in Python);
//! - scheduler: the component in charge of keeping track of tasks to execute;
//! - scheduler backend: the component that updates the internal state of the scheduler according to
//!   to an external source of truth (e.g., a database); there is no equivalent in Python,
//!   due to the fact that another pattern is used (see below);
//! - beat: the service that drives the execution, calling the appropriate
//!   methods of the scheduler in an infinite loop (called just *service* in Python).
//!
//! The main difference with the architecture used in Python is that in Python
//! there is a base scheduler class which contains the scheduling logic, then different
//! implementations use different strategies to synchronize the scheduler.
//! Here instead we have only one scheduler struct, and the different backends
//! correspond to the different scheduler implementations in Python.

use std::marker::PhantomData;
use std::time::SystemTime;

use log::{debug, error, info};
use tokio::time::{self, Duration};

pub use backend::{LocalSchedulerBackend, SchedulerBackend, SchedulerBackendBuilder};
pub use schedule::{CronSchedule, DeltaSchedule, Schedule};
pub use scheduled_task::ScheduledTask;
pub use scheduler::Scheduler;

use crate::broker::{Broker, BrokerBuilder};
use crate::config::{BrokerConfig, ConfigBroker, ConfigTask, TaskConfig};
use crate::routing::{self, Rule};
use crate::task::TaskOptions;
use crate::{
    error::{BeatError, BrokerError},
    task::{Signature, Task},
};

mod backend;
mod schedule;
mod scheduled_task;
mod scheduler;

pub(crate) struct SchedulerConfig {
    max_sleep_duration: Option<Duration>,
}

struct BeatConfig {
    task: TaskConfig,
    broker: BrokerConfig,
    scheduler: SchedulerConfig,
}

impl BeatConfig {
    fn default(url: String) -> Self {
        Self {
            task: TaskConfig {
                options: TaskOptions::default(),
                routes: vec![],
            },
            broker: BrokerConfig {
                url,
                default_queue: "celery".into(),
                prefetch_count: None,
                heartbeat: None,
                connection_timeout: 2,
                connection_retry: true,
                connection_max_retries: 5,
                connection_retry_delay: 5,
            },
            scheduler: SchedulerConfig {
                max_sleep_duration: None,
            },
        }
    }
}

/// Used to create a [`Beat`] app with a custom configuration.
pub struct BeatBuilder<B, Sb = LocalSchedulerBackend>
where
    B: Broker,
    Sb: SchedulerBackend,
{
    name: String,
    config: BeatConfig,
    _modules_type: PhantomData<(B, Sb)>,
}

impl<B, Sb> ConfigTask for BeatBuilder<B, Sb>
where
    B: Broker,
    Sb: SchedulerBackend,
{
    fn get_task_config(&mut self) -> &mut TaskConfig {
        &mut self.config.task
    }
}

impl<B, Sb> ConfigBroker for BeatBuilder<B, Sb>
where
    B: Broker,
    Sb: SchedulerBackend,
{
    fn get_broker_config(&mut self) -> &mut BrokerConfig {
        &mut self.config.broker
    }
}

impl<B, Sb> BeatBuilder<B, Sb>
where
    B: Broker,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom scheduler backend and
    /// a custom configuration.
    pub fn new(name: &str, broker_url: &str) -> Self {
        Self {
            name: name.into(),
            config: BeatConfig::default(broker_url.to_string()),
            _modules_type: PhantomData::default(),
        }
    }

    /// Set a maximum sleep duration, which limits the amount of time that
    /// can pass between ticks. This is useful to ensure that the scheduler backend
    /// implementation is called regularly.
    pub fn max_sleep_duration(mut self, max_sleep_duration: Duration) -> Self {
        self.config.scheduler.max_sleep_duration = Some(max_sleep_duration);
        self
    }

    /// Construct a `Beat` app with the current configuration.
    pub async fn build(self) -> Result<Beat<B, Sb>, BeatError> {
        let mut task_routes = Vec::new();

        // Declare default queue to broker.
        let broker = B::Builder::new(self.config.broker.url.as_str())
            .heartbeat(self.config.broker.heartbeat.unwrap_or(Some(60)))
            .prefetch_count(self.config.broker.prefetch_count.unwrap_or(10))
            .declare_queue(&self.config.broker.default_queue)
            .task_routes(&self.config.task.routes, &mut task_routes)?
            .build_and_connect(
                self.config.broker.connection_timeout,
                if self.config.broker.connection_retry {
                    self.config.broker.connection_max_retries
                } else {
                    0
                },
                self.config.broker.connection_retry_delay,
            )
            .await?;

        let scheduler_backend = Sb::Builder::new().build();

        Ok(Beat {
            name: self.name,
            scheduler: Scheduler::new(broker),
            scheduler_backend,
            task_routes,
            config: self.config,
        })
    }
}

/// A [`Beat`] app is used to send out scheduled tasks. This is the struct that is
/// created with the [`beat!`] macro.
///
/// It drives execution by making the internal scheduler "tick", and updates the list of scheduled
/// tasks through a customizable scheduler backend.
pub struct Beat<Br, Sb = LocalSchedulerBackend>
where
    Br: Broker,
    Sb: SchedulerBackend,
{
    pub name: String,
    pub scheduler: Scheduler<Br>,
    pub scheduler_backend: Sb,
    config: BeatConfig,
    task_routes: Vec<Rule>,
}

impl<Br, Sb> Beat<Br, Sb>
where
    Br: Broker,
    Sb: SchedulerBackend,
{
    /// Get a `BeatBuilder` for creating a `Beat` app with a custom configuration and
    /// a custom scheduler backend.
    pub fn builder(name: &str, broker_url: &str) -> BeatBuilder<Br, Sb> {
        BeatBuilder::<Br, Sb>::new(name, broker_url)
    }

    /// Schedule the execution of a task.
    pub fn schedule_task<T, S>(&mut self, signature: Signature<T>, schedule: S)
    where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        self.schedule_named_task(Signature::<T>::task_name().to_string(), signature, schedule);
    }

    /// Schedule the execution of a task with the given `name`.
    pub fn schedule_named_task<T, S>(
        &mut self,
        name: String,
        mut signature: Signature<T>,
        schedule: S,
    ) where
        T: Task + Clone + 'static,
        S: Schedule + 'static,
    {
        signature.options.update(&self.config.task.options);
        let queue = match &signature.queue {
            Some(queue) => queue.to_string(),
            None => routing::route(T::NAME, &self.task_routes)
                .unwrap_or(&self.config.broker.default_queue)
                .to_string(),
        };
        let message_factory = Box::new(signature);

        self.scheduler
            .schedule_task(name, message_factory, queue, schedule);
    }

    /// Start the *beat*.
    pub async fn start(&mut self) -> Result<(), BeatError> {
        info!("Starting beat service");
        loop {
            let result = self.beat_loop().await;
            if !self.config.broker.connection_retry {
                return result;
            }

            if let Err(err) = result {
                match err {
                    BeatError::BrokerError(broker_err) => {
                        if broker_err.is_connection_error() {
                            error!("Broker connection failed");
                        } else {
                            return Err(BeatError::BrokerError(broker_err));
                        }
                    }
                    _ => return Err(err),
                };
            } else {
                return result;
            }

            let mut reconnect_successful: bool = false;
            for _ in 0..self.config.broker.connection_max_retries {
                info!("Trying to re-establish connection with broker");
                time::sleep(Duration::from_secs(
                    self.config.broker.connection_retry_delay as u64,
                ))
                .await;

                match self
                    .scheduler
                    .broker
                    .reconnect(self.config.broker.connection_timeout)
                    .await
                {
                    Err(err) => {
                        if err.is_connection_error() {
                            continue;
                        }
                        return Err(BeatError::BrokerError(err));
                    }
                    Ok(_) => {
                        info!("Successfully reconnected with broker");
                        reconnect_successful = true;
                        break;
                    }
                };
            }

            if !reconnect_successful {
                return Err(BeatError::BrokerError(BrokerError::NotConnected));
            }
        }
    }

    async fn beat_loop(&mut self) -> Result<(), BeatError> {
        loop {
            let next_tick_at = self.scheduler.tick().await?;

            if self.scheduler_backend.should_sync() {
                self.scheduler_backend
                    .sync(self.scheduler.get_scheduled_tasks())?;
            }

            let now = SystemTime::now();
            if now < next_tick_at {
                let sleep_interval = next_tick_at.duration_since(now).expect(
                    "Unexpected error when unwrapping a SystemTime comparison that is not supposed to fail",
                );
                let sleep_interval = match &self.config.scheduler.max_sleep_duration {
                    Some(max_sleep_duration) => std::cmp::min(sleep_interval, *max_sleep_duration),
                    None => sleep_interval,
                };
                debug!("Now sleeping for {:?}", sleep_interval);
                time::sleep(sleep_interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests;
