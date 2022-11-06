use std::collections::HashMap;
use std::convert::TryFrom;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

use colored::Colorize;
use futures::stream::StreamExt;
use log::{debug, error, info, warn};
use tokio::select;
#[cfg(unix)]
use tokio::signal::unix::{signal, Signal, SignalKind};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::RwLock;
use tokio::time::{self, Duration};
use tokio_stream::StreamMap;

use trace::{build_tracer, TraceBuilder, TracerTrait};

use crate::app::trace::TraceBuilderResult;
use crate::backend::DisabledBackend;
use crate::backend::{Backend, BackendBuilder};
use crate::broker::{Broker, BrokerBuilder};
use crate::config::{
    BackendConfig, BrokerConfig, ConfigBackend, ConfigBroker, ConfigTask, TaskConfig,
};
use crate::error::{BrokerError, CeleryError, TraceError};
use crate::kombu_serde::SerializerKind;
use crate::protocol::{Message, TryDeserializeMessage};
use crate::routing::Rule;
use crate::task::{AsyncResult, Signature, Task, TaskEvent, TaskOptions, TaskStatus};

mod trace;

pub(crate) struct CeleryConfig {
    pub(crate) task: TaskConfig,
    pub(crate) broker: BrokerConfig,
    pub(crate) backend: BackendConfig,
}

impl CeleryConfig {
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
            backend: BackendConfig {
                url: String::new(),
                result_serializer: SerializerKind::JSON,
                result_expires: Some(chrono::Duration::days(1)),
            },
        }
    }
}

/// Used to create a [`Celery`] app with a custom configuration.
pub struct CeleryBuilder<B, D = DisabledBackend>
where
    B: Broker,
    D: Backend,
{
    name: String,
    hostname: String,
    config: CeleryConfig,
    _builder_type: PhantomData<(B, D)>,
}

impl<B, D> ConfigTask for CeleryBuilder<B, D>
where
    B: Broker,
    D: Backend,
{
    fn get_task_config(&mut self) -> &mut TaskConfig {
        &mut self.config.task
    }
}

impl<B, D> ConfigBroker for CeleryBuilder<B, D>
where
    B: Broker,
    D: Backend,
{
    fn get_broker_config(&mut self) -> &mut BrokerConfig {
        &mut self.config.broker
    }
}

impl<B, D> ConfigBackend for CeleryBuilder<B, D>
where
    B: Broker,
    D: Backend,
{
    fn get_backend_config(&mut self) -> &mut BackendConfig {
        &mut self.config.backend
    }
}

impl<B, D> CeleryBuilder<B, D>
where
    B: Broker + Send,
    D: Backend,
{
    /// Get a [`CeleryBuilder`] for creating a [`Celery`] app with a custom configuration.
    pub fn new(name: &str, broker_url: &str) -> Self {
        Self {
            name: name.into(),
            hostname: format!(
                "{}@{}",
                name,
                hostname::get()
                    .ok()
                    .and_then(|sys_hostname| sys_hostname.into_string().ok())
                    .unwrap_or_else(|| "unknown".into())
            ),
            config: CeleryConfig::default(broker_url.to_string()),
            _builder_type: PhantomData::default(),
        }
    }

    /// Set the node name of the app. Defaults to `"{name}@{sys hostname}"`.
    ///
    /// *This field should probably be named "nodename" to avoid confusion with the
    /// system hostname, but we're trying to be consistent with Python Celery.*
    pub fn hostname(mut self, hostname: &str) -> Self {
        self.hostname = hostname.into();
        self
    }

    /// Construct a [`Celery`] app with the current configuration.
    pub async fn build(self) -> Result<Celery<B, D>, CeleryError> {
        let mut task_routes = Vec::new();

        // Declare default queue to broker.
        let broker = B::Builder::new(self.config.broker.url.as_str())
            .prefetch_count(self.config.broker.prefetch_count.unwrap_or(10))
            .heartbeat(self.config.broker.heartbeat.unwrap_or(Some(60)))
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

        let backend = D::Builder::new(self.config.backend.url.as_str())
            .result_serializer(self.config.backend.result_serializer)
            .result_expires(self.config.backend.result_expires)
            .build()
            .await?;

        Ok(Celery {
            name: self.name,
            hostname: self.hostname,
            broker,
            backend: Arc::new(backend),
            task_routes,
            task_trace_builders: RwLock::new(HashMap::new()),
            config: self.config,
        })
    }
}

/// A [`Celery`] app is used to produce or consume tasks asynchronously. This is the struct that is
/// created with the [`app!`] macro.
pub struct Celery<B: Broker, D: Backend = DisabledBackend> {
    /// An arbitrary, human-readable name for the app.
    pub name: String,

    /// Node name of the app.
    pub hostname: String,

    /// The app's broker.
    pub broker: B,

    /// The app's backend.
    pub backend: Arc<D>,

    /// A vector of routing rules in the order of their importance.
    task_routes: Vec<Rule>,

    /// Mapping of task name to task tracer factory. Used to create a task tracer
    /// from an incoming message.
    task_trace_builders: RwLock<HashMap<String, TraceBuilder<D>>>,

    /// Celery common configuration
    config: CeleryConfig,
}

impl<B, D> Celery<B, D>
where
    B: Broker + 'static,
    D: Backend + 'static,
{
    /// Get a [`CeleryBuilder`] for creating a [`Celery`] app with a custom configuration.
    pub fn builder(name: &str, broker_url: &str) -> CeleryBuilder<B, D> {
        CeleryBuilder::<B, D>::new(name, broker_url)
    }

    /// Print a pretty ASCII art logo and configuration settings.
    ///
    /// This is useful and fun to print from a worker application right after
    /// the [`Celery`] app is initialized.
    pub async fn display_pretty(&self) {
        // Cool ASCII logo with hostname.
        let banner = format!(
            r#"
  _________________          >_<
 /  ______________ \         | |
/  /              \_\  ,---. | | ,---. ,--.--.,--. ,--.
| /   .<      >.      | .-. :| || .-. :|  .--' \  '  /
| |   (        )      \   --.| |\   --.|  |     \   /
| |    --o--o--        `----'`-' `----'`--'   .-'  /
| |  _/        \_   __                         `--'
| | / \________/ \ / /
| \    |      |   / /
 \ \_____________/ /    {}
  \_______________/
"#,
            self.hostname
        );
        println!("{}", banner.truecolor(255, 102, 0));

        // Broker.
        println!("{}", "[broker]".bold());
        println!(" {}", self.broker.safe_url());
        println!("{}", "[backend]".bold());
        println!(" {}", self.backend.safe_url());
        println!();

        // Registered tasks.
        println!("{}", "[tasks]".bold());
        for task in self.task_trace_builders.read().await.keys() {
            println!(" . {}", task);
        }
        println!();
    }

    /// Send a task to a remote worker. Returns an [`AsyncResult`] with the task ID of the task
    /// if it was successfully sent.
    pub async fn send_task<T: Task>(
        &self,
        mut task_sig: Signature<T>,
    ) -> Result<AsyncResult<D>, CeleryError> {
        task_sig.options.update(&self.config.task.options);
        let maybe_queue = task_sig.queue.take();
        let queue = maybe_queue.as_deref().unwrap_or_else(|| {
            crate::routing::route(T::NAME, &self.task_routes)
                .unwrap_or(&self.config.broker.default_queue)
        });
        let message = Message::try_from(task_sig)?;
        info!(
            "Sending task {}[{}] to {}",
            T::NAME,
            message.task_id(),
            queue,
        );
        self.broker.send(&message, queue).await?;
        Ok(AsyncResult::new(message.task_id(), self.backend.clone()))
    }

    /// Register a task.
    pub async fn register_task<T: Task + 'static>(&self) -> Result<(), CeleryError> {
        let mut task_trace_builders = self.task_trace_builders.write().await;
        if task_trace_builders.contains_key(T::NAME) {
            Err(CeleryError::TaskRegistrationError(T::NAME.into()))
        } else {
            task_trace_builders.insert(T::NAME.into(), Box::new(build_tracer::<T, D>));
            debug!("Registered task {}", T::NAME);
            Ok(())
        }
    }

    async fn get_task_tracer(
        &self,
        message: Message,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<Box<dyn TracerTrait<D>>, Box<dyn Error + Send + Sync + 'static>> {
        let task_trace_builders = self.task_trace_builders.read().await;
        if let Some(build_tracer) = task_trace_builders.get(&message.headers.task) {
            (build_tracer(
                message,
                self.config.task.options,
                self.backend.clone(),
                event_tx,
                self.hostname.clone(),
            ) as TraceBuilderResult<D>)
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)
        } else {
            Err(
                Box::new(CeleryError::UnregisteredTaskError(message.headers.task))
                    as Box<dyn Error + Send + Sync + 'static>,
            )
        }
    }

    /// Tries converting a delivery into a `Message`, executing the corresponding task,
    /// and communicating with the broker.
    async fn try_handle_delivery(
        &self,
        delivery: B::Delivery,
        event_tx: UnboundedSender<TaskEvent>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        // Coerce the delivery into a protocol message.
        let message = match delivery.try_deserialize_message() {
            Ok(message) => message,
            Err(e) => {
                // This is a naughty message that we can't handle, so we'll ack it with
                // the broker so it gets deleted.
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(Box::new(e));
            }
        };

        // Try deserializing the message to create a task wrapped in a task tracer.
        // (The tracer handles all of the logic of directly interacting with the task
        // to execute it and run the post-execution functions).
        let mut tracer = match self.get_task_tracer(message, event_tx).await {
            Ok(tracer) => tracer,
            Err(e) => {
                // Even though the message meta data was okay, we failed to deserialize
                // the body of the message for some reason, so ack it with the broker
                // to delete it and return an error.
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(e);
            }
        };

        if tracer.is_delayed() {
            // Task has an ETA, so we need to increment the prefetch count so that
            // we can receive other tasks while we wait for the ETA.
            if let Err(e) = self.broker.increase_prefetch_count().await {
                // If for some reason this operation fails, we should stop tracing
                // this task and send it back to the broker to retry.
                // Otherwise we could reach the prefetch_count and end up blocking
                // other deliveries if there are a high number of messages with a
                // future ETA.
                self.broker
                    .retry(&delivery, None)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                self.broker
                    .ack(&delivery)
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
                return Err(Box::new(e));
            };

            // Then wait for the task to be ready.
            tracer.wait().await;
        }

        // If acks_late is false, we acknowledge the message before tracing it.
        if !tracer.acks_late() {
            self.broker
                .ack(&delivery)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // Try tracing the task now.
        // NOTE: we don't need to log errors from the trace here since the tracer
        // handles all errors at it's own level or the task level. In this function
        // we only log errors at the broker and delivery level.
        if let Err(TraceError::Retry(retry_eta)) = tracer.trace().await {
            // If retry error -> retry the task.
            self.broker
                .retry(&delivery, retry_eta)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // If we have not done it before, we have to acknowledge the message now.
        if tracer.acks_late() {
            self.broker
                .ack(&delivery)
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        // If we had increased the prefetch count above due to a future ETA, we have
        // to decrease it back down to restore balance to the universe.
        if tracer.is_delayed() {
            self.broker
                .decrease_prefetch_count()
                .await
                .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync + 'static>)?;
        }

        Ok(())
    }

    /// Wraps `try_handle_delivery` to catch any and all errors that might occur.
    async fn handle_delivery(
        self: Arc<Self>,
        delivery: B::Delivery,
        event_tx: UnboundedSender<TaskEvent>,
    ) {
        if let Err(e) = self.try_handle_delivery(delivery, event_tx).await {
            error!("{}", e);
        }
    }

    /// Close channels and connections.
    pub async fn close(&self) -> Result<(), CeleryError> {
        Ok(self.broker.close().await?)
    }

    /// Consume tasks from the default queue.
    pub async fn consume(self: &Arc<Self>) -> Result<(), CeleryError> {
        let queues = &[&self.config.broker.default_queue.clone()[..]];
        Self::consume_from(self, queues).await
    }

    /// Consume tasks from any number of queues.
    pub async fn consume_from(self: &Arc<Self>, queues: &[&str]) -> Result<(), CeleryError> {
        loop {
            let result = self.clone()._consume_from(queues).await;
            if !self.config.broker.connection_retry {
                return result;
            }

            if let Err(err) = result {
                match err {
                    CeleryError::BrokerError(broker_err) => {
                        if broker_err.is_connection_error() {
                            error!("Broker connection failed");
                        } else {
                            return Err(CeleryError::BrokerError(broker_err));
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
                    .broker
                    .reconnect(self.config.broker.connection_timeout)
                    .await
                {
                    Err(err) => {
                        if err.is_connection_error() {
                            continue;
                        }
                        return Err(CeleryError::BrokerError(err));
                    }
                    Ok(_) => {
                        info!("Successfully reconnected with broker");
                        reconnect_successful = true;
                        break;
                    }
                };
            }

            if !reconnect_successful {
                return Err(CeleryError::BrokerError(BrokerError::NotConnected));
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    async fn _consume_from(self: Arc<Self>, queues: &[&str]) -> Result<(), CeleryError> {
        if queues.is_empty() {
            return Err(CeleryError::NoQueueToConsume);
        }

        info!("Consuming from {:?}", queues);

        // Stream of errors from broker. The capacity here is arbitrary because a single
        // error from the broker should trigger this method to return early.
        let (broker_error_tx, mut broker_error_rx) = mpsc::channel::<BrokerError>(100);

        // Stream of deliveries from the queue.
        let mut stream_map = StreamMap::new();
        let mut consumer_tags = vec![];
        for queue in queues {
            let broker_error_tx = broker_error_tx.clone();

            let (consumer_tag, consumer) = self
                .broker
                .consume(
                    queue,
                    Box::new(move |e| {
                        broker_error_tx.clone().try_send(e).ok();
                    }),
                )
                .await?;
            stream_map.insert(queue, Box::pin(consumer));
            consumer_tags.push(consumer_tag);
        }

        // Stream of OS signals.
        let mut ender = Ender::new()?;

        // A sender and receiver for task related events.
        // NOTE: we can use an unbounded channel since we already have backpressure
        // from the `prefetch_count` setting.
        let (task_event_tx, mut task_event_rx) = mpsc::unbounded_channel::<TaskEvent>();
        let mut pending_tasks = 0;

        // This is the main loop where we receive deliveries and pass them off
        // to be handled by spawning `self.handle_delivery`.
        // At the same time we are also listening for a SIGINT (Ctrl+C) or SIGTERM interruption.
        // If that occurs we break from this loop and move to the warm shutdown loop
        // if there are still any pending tasks (tasks being executed, not including
        // tasks being delayed due to a future ETA).
        loop {
            select! {
                maybe_delivery_result = stream_map.next() => {
                    if let Some((queue, delivery_result)) = maybe_delivery_result {
                        match delivery_result {
                            Ok(delivery) => {
                                let task_event_tx = task_event_tx.clone();
                                debug!("Received delivery from {}: {:?}", queue, delivery);
                                tokio::spawn(self.clone().handle_delivery(delivery, task_event_tx));
                            }
                            Err(e) => {
                                error!("Deliver failed: {}", e);
                            }
                        }
                    }
                },
                ending = ender.wait() => {
                    if let Ok(SigType::Interrupt) = ending {
                        warn!("Ope! Hitting Ctrl+C again will terminate all running tasks!");
                    }
                    info!("Warm shutdown...");
                    break;
                },
                maybe_task_event = task_event_rx.recv() => {
                    if let Some(event) = maybe_task_event {
                        debug!("Received task event {:?}", event);
                        match event {
                            TaskEvent::StatusChange(TaskStatus::Pending) => pending_tasks += 1,
                            TaskEvent::StatusChange(TaskStatus::Finished) => pending_tasks -= 1,
                        };
                    }
                },
                maybe_broker_error = broker_error_rx.recv() => {
                    if let Some(broker_error) = maybe_broker_error {
                        error!("{}", broker_error);
                        return Err(broker_error.into());
                    }
                }
            }
        }

        // Cancel consumers.
        for consumer_tag in consumer_tags {
            debug!("Cancelling consumer {}", consumer_tag);
            self.broker.cancel(&consumer_tag).await?;
        }

        if pending_tasks > 0 {
            // Warm shutdown loop. When there are still pending tasks we wait for them
            // to finish. We get updates about pending tasks through the `task_event_rx` channel.
            // We also watch for a second SIGINT or SIGTERM, in which case we immediately shutdown.
            info!("Waiting on {} pending tasks...", pending_tasks);
            loop {
                select! {
                    ending = ender.wait() => {
                        if let Ok(SigType::Interrupt) = ending {
                            warn!("Okay fine, shutting down now. See ya!");
                            return Err(CeleryError::ForcedShutdown);
                        }
                    },
                    maybe_event = task_event_rx.recv() => {
                        if let Some(event) = maybe_event {
                            debug!("Received task event {:?}", event);
                            match event {
                                TaskEvent::StatusChange(TaskStatus::Pending) => pending_tasks += 1,
                                TaskEvent::StatusChange(TaskStatus::Finished) => pending_tasks -= 1,
                            };
                            if pending_tasks <= 0 {
                                break;
                            }
                        }
                    },
                }
            }
        }

        info!("No more pending tasks. See ya!");

        Ok(())
    }
}

#[allow(unused)]
enum SigType {
    /// Equivalent to SIGINT on unix systems.
    Interrupt,
    /// Equivalent to SIGTERM on unix systems.
    Terminate,
}

/// The ender listens for signals.
#[cfg(unix)]
struct Ender {
    sigint: Signal,
    sigterm: Signal,
}

#[cfg(unix)]
impl Ender {
    fn new() -> Result<Self, std::io::Error> {
        let sigint = signal(SignalKind::interrupt())?;
        let sigterm = signal(SignalKind::terminate())?;

        Ok(Ender { sigint, sigterm })
    }

    /// Waits for either an interrupt or terminate.
    async fn wait(&mut self) -> Result<SigType, std::io::Error> {
        let sigtype;

        select! {
            _ = self.sigint.recv() => {
                sigtype = SigType::Interrupt
            },
            _ = self.sigterm.recv() => {
                sigtype = SigType::Terminate
            }
        }

        Ok(sigtype)
    }
}

#[cfg(windows)]
struct Ender;

#[cfg(windows)]
impl Ender {
    fn new() -> Result<Self, std::io::Error> {
        Ok(Ender)
    }

    async fn wait(&mut self) -> Result<SigType, std::io::Error> {
        tokio::signal::ctrl_c().await?;

        Ok(SigType::Interrupt)
    }
}

#[cfg(test)]
mod tests;
