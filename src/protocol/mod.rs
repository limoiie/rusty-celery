//! Defines the Celery protocol.
//!
//! The top part of the protocol is the [`Message`] struct, which builds on
//! top of the protocol for a broker. This is why a broker's [`Delivery`](crate::broker::Broker::Delivery)
//! type must implement [`TryCreateMessage`].

use std::collections::HashSet;
use std::convert::TryFrom;
use std::process;
use std::time::SystemTime;

use chrono::{DateTime, Duration, Utc};
use log::{debug, warn};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, from_value, json, Value};
use uuid::Uuid;

use crate::error::{ContentTypeError, ProtocolError, TaskError, TraceError};
use crate::kombu_serde::AnyValue;
use crate::task::{Signature, Task};

static ORIGIN: Lazy<Option<String>> = Lazy::new(|| {
    hostname::get()
        .ok()
        .and_then(|sys_hostname| sys_hostname.into_string().ok())
        .map(|sys_hostname| format!("gen{}@{}", process::id(), sys_hostname))
});

/// Serialization formats supported for message body.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum MessageContentType {
    #[default]
    Json,
    #[cfg(any(test, feature = "extra_content_types"))]
    Yaml,
    #[cfg(any(test, feature = "extra_content_types"))]
    Pickle,
    #[cfg(any(test, feature = "extra_content_types"))]
    MsgPack,
}

impl MessageContentType {
    pub fn name(&self) -> &'static str {
        match self {
            MessageContentType::Json => "application/json",
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Yaml => "application/x-yaml",
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Pickle => "application/x-python-serialize",
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::MsgPack => "application/x-msgpack",
        }
    }

    pub fn from_name(name: &str) -> Option<MessageContentType> {
        match name {
            "application/json" => Some(MessageContentType::Json),
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-yaml" => Some(MessageContentType::Yaml),
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-python-serialize" => Some(MessageContentType::Pickle),
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-msgpack" => Some(MessageContentType::MsgPack),
            _ => None,
        }
    }
}

impl MessageContentType {
    pub fn dump_bytes<D>(&self, data: &D) -> Vec<u8>
    where
        D: Serialize,
    {
        self.try_dump_bytes(data).unwrap()
    }

    pub fn try_dump_bytes<D>(&self, data: &D) -> Result<Vec<u8>, ContentTypeError>
    where
        D: Serialize,
    {
        match self {
            MessageContentType::Json => serde_json::to_vec(data).map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Yaml => serde_yaml::to_vec(data).map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Pickle => {
                serde_pickle::to_vec(data, serde_pickle::SerOptions::new())
                    .map_err(ContentTypeError::from)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::MsgPack => rmp_serde::to_vec(data).map_err(ContentTypeError::from),
        }
    }

    pub fn dump<D>(&self, data: &D) -> String
    where
        D: Serialize,
    {
        self.try_dump(data).unwrap()
    }

    pub fn try_dump<D>(&self, data: &D) -> Result<String, ContentTypeError>
    where
        D: Serialize,
    {
        match self {
            MessageContentType::Json => serde_json::to_string(data).map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Yaml => serde_yaml::to_string(data).map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Pickle | MessageContentType::MsgPack => {
                Err(ContentTypeError::Unsupported(
                    "Can not dump as string: this kind of content may contain raw bytes".into(),
                ))
            }
        }
    }

    pub fn load<D>(&self, data: &String) -> D
    where
        D: for<'de> Deserialize<'de>,
    {
        debug!(
            "Try decoding as {} with `{}'",
            std::any::type_name::<D>(),
            data
        );

        match self {
            MessageContentType::Json => serde_json::from_str(data.as_str()).unwrap(),
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Yaml => serde_yaml::from_str(data.as_str()).unwrap(),
            #[cfg(any(test, feature = "extra_content_types"))]
            _ => unimplemented!(),
        }
    }

    pub fn try_load<D>(&self, data: &String) -> Result<D, ContentTypeError>
    where
        D: for<'de> Deserialize<'de>,
    {
        debug!(
            "Try decoding as {} with `{}'",
            std::any::type_name::<D>(),
            data
        );

        match self {
            MessageContentType::Json => {
                serde_json::from_str(data.as_str()).map_err(ContentTypeError::from)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            MessageContentType::Yaml => {
                serde_yaml::from_str(data.as_str()).map_err(ContentTypeError::from)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            _ => unimplemented!(),
        }
    }

    pub fn to_value<D>(&self, data: &D) -> AnyValue
    where
        D: Serialize,
    {
        self.try_to_value(data).unwrap()
    }

    pub fn try_to_value<D>(&self, data: &D) -> Result<AnyValue, ContentTypeError>
    where
        D: Serialize,
    {
        match self {
            ContentType::Json => serde_json::to_value(data)
                .map(Into::into)
                .map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            ContentType::Yaml => serde_yaml::to_value(data)
                .map(Into::into)
                .map_err(ContentTypeError::from),
            #[cfg(any(test, feature = "extra_content_types"))]
            _ => unimplemented!(),
        }
    }
}

/// Create a message with a custom configuration.
pub struct MessageBuilder<T>
where
    T: Task,
{
    message: Message,
    params: Option<T::Params>,
}

impl<T> MessageBuilder<T>
where
    T: Task,
{
    /// Create a new `MessageBuilder` with a given task ID.
    pub fn new(id: String) -> Self {
        Self {
            message: Message {
                properties: MessageProperties {
                    correlation_id: id.clone(),
                    content_type: "application/json".into(),
                    content_encoding: "utf-8".into(),
                    reply_to: None,
                },
                headers: MessageHeaders {
                    id,
                    task: T::NAME.into(),
                    origin: ORIGIN.to_owned(),
                    ..Default::default()
                },
                raw_body: Vec::new(),
            },
            params: None,
        }
    }
    /// Set which serialization method is used in the body.
    ///
    /// JSON is the default, and is also the only option unless the feature "extra_content_types" is enabled.
    #[cfg(any(test, feature = "extra_content_types"))]
    pub fn content_type(mut self, content_type: MessageContentType) -> Self {
        self.message.properties.content_type = content_type.name().into();
        self
    }

    pub fn content_encoding(mut self, content_encoding: String) -> Self {
        self.message.properties.content_encoding = content_encoding;
        self
    }

    pub fn correlation_id(mut self, correlation_id: String) -> Self {
        self.message.properties.correlation_id = correlation_id;
        self
    }

    pub fn reply_to(mut self, reply_to: String) -> Self {
        self.message.properties.reply_to = Some(reply_to);
        self
    }

    pub fn id(mut self, id: String) -> Self {
        self.message.headers.id = id;
        self
    }

    pub fn task(mut self, task: String) -> Self {
        self.message.headers.task = task;
        self
    }

    pub fn lang(mut self, lang: String) -> Self {
        self.message.headers.lang = Some(lang);
        self
    }

    pub fn root_id(mut self, root_id: String) -> Self {
        self.message.headers.root_id = Some(root_id);
        self
    }

    pub fn parent_id(mut self, parent_id: String) -> Self {
        self.message.headers.parent_id = Some(parent_id);
        self
    }

    pub fn group(mut self, group: String) -> Self {
        self.message.headers.group = Some(group);
        self
    }

    pub fn meth(mut self, meth: String) -> Self {
        self.message.headers.meth = Some(meth);
        self
    }

    pub fn shadow(mut self, shadow: String) -> Self {
        self.message.headers.shadow = Some(shadow);
        self
    }

    pub fn retries(mut self, retries: u32) -> Self {
        self.message.headers.retries = Some(retries);
        self
    }

    pub fn argsrepr(mut self, argsrepr: String) -> Self {
        self.message.headers.argsrepr = Some(argsrepr);
        self
    }

    pub fn kwargsrepr(mut self, kwargsrepr: String) -> Self {
        self.message.headers.kwargsrepr = Some(kwargsrepr);
        self
    }

    pub fn origin(mut self, origin: String) -> Self {
        self.message.headers.origin = Some(origin);
        self
    }

    pub fn time_limit(mut self, time_limit: u32) -> Self {
        self.message.headers.timelimit.1 = Some(time_limit);
        self
    }

    pub fn hard_time_limit(mut self, time_limit: u32) -> Self {
        self.message.headers.timelimit.0 = Some(time_limit);
        self
    }

    pub fn eta(mut self, eta: DateTime<Utc>) -> Self {
        self.message.headers.eta = Some(eta);
        self
    }

    pub fn countdown(self, countdown: u32) -> Self {
        let now = DateTime::<Utc>::from(SystemTime::now());
        let eta = now + Duration::seconds(countdown as i64);
        self.eta(eta)
    }

    pub fn expires(mut self, expires: DateTime<Utc>) -> Self {
        self.message.headers.expires = Some(expires);
        self
    }

    pub fn expires_in(self, expires_in: u32) -> Self {
        let now = DateTime::<Utc>::from(SystemTime::now());
        let expires = now + Duration::seconds(expires_in as i64);
        self.expires(expires)
    }

    pub fn params(mut self, params: T::Params) -> Self {
        self.params = Some(params);
        self
    }

    /// Get the `Message` with the custom configuration.
    pub fn build(mut self) -> Result<Message, ProtocolError> {
        if let Some(params) = self.params.take() {
            let body = MessageBody::<T>::new(params);

            let content_type =
                MessageContentType::from_name(self.message.properties.content_type.as_str());
            self.message.raw_body = match content_type {
                Some(content_type) => content_type.try_dump_bytes(&body)?,
                None => Err(ProtocolError::BodySerializationError(
                    ContentTypeError::Unknown,
                ))?,
            };
        };
        Ok(self.message)
    }
}

/// A [`Message`] is the core of the Celery protocol and is built on top of a [`Broker`](crate::broker::Broker)'s protocol.
/// Every message corresponds to a task.
///
/// Note that the [`raw_body`](Message::raw_body) field is the serialized form of a [`MessageBody`]
/// so that a worker can read the meta data of a message without having to deserialize the body
/// first.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct Message {
    /// Message properties correspond to the equivalent AMQP delivery properties.
    pub properties: MessageProperties,

    /// Message headers contain additional meta data pertaining to the Celery protocol.
    pub headers: MessageHeaders,

    /// A serialized [`MessageBody`].
    pub raw_body: Vec<u8>,
}

impl Message {
    /// Try deserializing the body.
    pub fn body<T: Task>(&self) -> Result<MessageBody<T>, ProtocolError> {
        match self.properties.content_type.as_str() {
            "application/json" => {
                let value: Value = from_slice(&self.raw_body)?;
                debug!("Deserialized message body: {:?}", value);
                if let Value::Array(ref vec) = value {
                    if let [Value::Array(ref args), Value::Object(ref kwargs), Value::Object(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    kwargs.insert((*arg_name).into(), arg.clone());
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Object(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Object(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value::<MessageBody<T>>(value)?)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-yaml" => {
                use serde_yaml::{from_slice, from_value, Value};
                let value: Value = from_slice(&self.raw_body)?;
                debug!("Deserialized message body: {:?}", value);
                if let Value::Sequence(ref vec) = value {
                    if let [Value::Sequence(ref args), Value::Mapping(ref kwargs), Value::Mapping(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    kwargs.insert((*arg_name).into(), arg.clone());
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Mapping(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Mapping(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-python-serialize" => {
                use serde_pickle::{from_slice, from_value, DeOptions, HashableValue, Value};
                let value: Value = from_slice(&self.raw_body, DeOptions::new())?;
                // debug!("Deserialized message body: {:?}", value);
                if let Value::List(ref vec) = value {
                    if let [Value::List(ref args), Value::Dict(ref kwargs), Value::Dict(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    let key = HashableValue::String((*arg_name).into());
                                    kwargs.insert(key, arg.clone());
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Dict(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Dict(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            #[cfg(any(test, feature = "extra_content_types"))]
            "application/x-msgpack" => {
                use rmp_serde::from_slice;
                use rmpv::{ext::from_value, Value};
                let value: Value = from_slice(&self.raw_body)?;
                debug!("Deserialized message body: {:?}", value);
                if let Value::Array(ref vec) = value {
                    if let [Value::Array(ref args), Value::Map(ref kwargs), Value::Map(ref embed)] =
                        vec[..]
                    {
                        if !args.is_empty() {
                            // Non-empty args, need to try to coerce them into kwargs.
                            let mut kwargs = kwargs.clone();
                            let embed = embed.clone();
                            let arg_names = T::ARGS;
                            for (i, arg) in args.iter().enumerate() {
                                if let Some(arg_name) = arg_names.get(i) {
                                    // messagepack is storing the map as a vec where each item
                                    // is a tuple of (key, value). here we will look for an item
                                    // with the matching key and replace it, or insert a new entry
                                    // at the end of the vec
                                    let existing_entry = kwargs
                                        .iter()
                                        .enumerate()
                                        .filter(|(_, (key, _))| {
                                            if let Value::String(key) = key {
                                                if let Some(key) = key.as_str() {
                                                    key == *arg_name
                                                } else {
                                                    false
                                                }
                                            } else {
                                                false
                                            }
                                        })
                                        .map(|(i, _)| i)
                                        .next();
                                    if let Some(index) = existing_entry {
                                        kwargs[index] = ((*arg_name).into(), arg.clone());
                                    } else {
                                        kwargs.push(((*arg_name).into(), arg.clone()));
                                    }
                                } else {
                                    break;
                                }
                            }
                            return Ok(MessageBody(
                                vec![],
                                from_value::<T::Params>(Value::Map(kwargs))?,
                                from_value::<MessageBodyEmbed>(Value::Map(embed))?,
                            ));
                        }
                    }
                }
                Ok(from_value(value)?)
            }
            _ => Err(ProtocolError::BodySerializationError(
                ContentTypeError::Unknown,
            )),
        }
    }

    /// Get the task ID.
    pub fn task_id(&self) -> &str {
        &self.headers.id
    }

    pub fn json_serialized(&self) -> Result<Vec<u8>, ProtocolError> {
        let root_id = match &self.headers.root_id {
            Some(root_id) => json!(root_id.clone()),
            None => Value::Null,
        };
        let reply_to = match &self.properties.reply_to {
            Some(reply_to) => json!(reply_to.clone()),
            None => Value::Null,
        };
        let eta = match self.headers.eta {
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null,
        };
        let expires = match self.headers.expires {
            Some(time) => json!(time.to_rfc3339()),
            None => Value::Null,
        };
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let delivery_tag = uuid.to_owned();
        let msg_json_value = json!({
            "body": base64::encode(self.raw_body.clone()),
            "content-encoding": self.properties.content_encoding.clone(),
            "content-type": self.properties.content_type.clone(),
            "headers": {
                "id": self.headers.id.clone(),
                "task": self.headers.task.clone(),
                "lang": self.headers.lang.clone(),
                "root_id": root_id,
                "parent_id": self.headers.parent_id.clone(),
                "group": self.headers.group.clone(),
                "meth": self.headers.meth.clone(),
                "shadow": self.headers.shadow.clone(),
                "eta": eta,
                "expires": expires,
                "retries": self.headers.retries.clone(),
                "timelimit": self.headers.timelimit.clone(),
                "argsrepr": self.headers.argsrepr.clone(),
                "kwargsrepr": self.headers.kwargsrepr.clone(),
                "origin": self.headers.origin.clone()
            },
            "properties": json!({
                "correlation_id": self.properties.correlation_id.clone(),
                "reply_to": reply_to,
                "delivery_tag": delivery_tag,
                "body_encoding": "base64",
            })
        });
        let res = serde_json::to_string(&msg_json_value)?;
        Ok(res.into_bytes())
    }
}

impl<T> TryFrom<Signature<T>> for Message
where
    T: Task,
{
    type Error = ProtocolError;

    /// Get a new [`MessageBuilder`] from a task signature.
    fn try_from(mut task_sig: Signature<T>) -> Result<Self, Self::Error> {
        // Create random correlation id.
        let mut buffer = Uuid::encode_buffer();
        let uuid = Uuid::new_v4().to_hyphenated().encode_lower(&mut buffer);
        let id = uuid.to_owned();

        let mut builder = MessageBuilder::<T>::new(id);

        // 'countdown' arbitrarily takes priority over 'eta'.
        if let Some(countdown) = task_sig.countdown.take() {
            builder = builder.countdown(countdown);
            if task_sig.eta.is_some() {
                warn!(
                    "Task {} specified both a 'countdown' and an 'eta'. Ignoring 'eta'.",
                    T::NAME
                )
            }
        } else if let Some(eta) = task_sig.eta.take() {
            builder = builder.eta(eta);
        }

        // 'expires_in' arbitrarily takes priority over 'expires'.
        if let Some(expires_in) = task_sig.expires_in.take() {
            builder = builder.expires_in(expires_in);
            if task_sig.expires.is_some() {
                warn!(
                    "Task {} specified both 'expires_in' and 'expires'. Ignoring 'expires'.",
                    T::NAME
                )
            }
        } else if let Some(expires) = task_sig.expires.take() {
            builder = builder.expires(expires);
        }

        #[cfg(any(test, feature = "extra_content_types"))]
        if let Some(content_type) = task_sig.options.content_type {
            builder = builder.content_type(content_type);
        }

        if let Some(time_limit) = task_sig.options.time_limit.take() {
            builder = builder.time_limit(time_limit);
        }

        if let Some(time_limit) = task_sig.options.hard_time_limit.take() {
            builder = builder.hard_time_limit(time_limit);
        }

        builder.params(task_sig.params).build()
    }
}

/// A trait for attempting to create a [`Message`] from `self`. This will be implemented
/// by types that can act like message "factories", like for instance the
/// [`Signature`](crate::task::Signature) type.
pub trait TryCreateMessage {
    fn try_create_message(&self) -> Result<Message, ProtocolError>;
}

impl<T> TryCreateMessage for Signature<T>
where
    T: Task + Clone,
{
    /// Creating a message from a signature without consuming the signature requires cloning it.
    /// For one-shot conversions, directly use [`Message::try_from`] instead.
    fn try_create_message(&self) -> Result<Message, ProtocolError> {
        Message::try_from(self.clone())
    }
}

/// A trait for attempting to deserialize a [`Message`] from `self`. This is required to be implemented
/// on a broker's [`Delivery`](crate::broker::Broker::Delivery) type.
pub trait TryDeserializeMessage {
    fn try_deserialize_message(&self) -> Result<Message, ProtocolError>;
}

/// Message meta data pertaining to the broker.
#[derive(Eq, PartialEq, Debug, Clone)]
pub struct MessageProperties {
    /// A unique ID associated with the task, usually the same as [`MessageHeaders::id`].
    pub correlation_id: String,

    /// The MIME type of the body.
    pub content_type: String,

    /// The encoding of the body.
    pub content_encoding: String,

    /// Used by the RPC backend when failures are reported by the parent process.
    pub reply_to: Option<String>,
}

/// Additional meta data pertaining to the Celery protocol.
#[derive(Eq, PartialEq, Debug, Default, Deserialize, Clone)]
pub struct MessageHeaders {
    /// A unique ID of the task.
    pub id: String,

    /// The name of the task.
    pub task: String,

    /// The programming language associated with the task.
    pub lang: Option<String>,

    /// The first task in the work-flow.
    pub root_id: Option<String>,

    /// The ID of the task that called this task within a work-flow.
    pub parent_id: Option<String>,

    /// The unique ID of the task's group, if this task is a member.
    pub group: Option<String>,

    /// Currently unused but could be used in the future to specify class+method pairs.
    pub meth: Option<String>,

    /// Modifies the task name that is used in logs.
    pub shadow: Option<String>,

    /// A future time after which the task should be executed.
    pub eta: Option<DateTime<Utc>>,

    /// A future time after which the task should be discarded if it hasn't executed
    /// yet.
    pub expires: Option<DateTime<Utc>>,

    /// The number of times the task has been retried without success.
    pub retries: Option<u32>,

    /// A tuple specifying the hard and soft time limits, respectively.
    ///
    /// *Note that as of writing this, the Python celery docs actually have a typo where it says
    /// these are reversed.*
    pub timelimit: (Option<u32>, Option<u32>),

    /// A string representation of the positional arguments of the task.
    pub argsrepr: Option<String>,

    /// A string representation of the keyword arguments of the task.
    pub kwargsrepr: Option<String>,

    /// A string representing the nodename of the process that produced the task.
    pub origin: Option<String>,

    /// A boolean value indicating whether to ignore the result or not.
    pub ignore_result: Option<bool>,
}

/// The body of a message. Contains the task itself as well as callback / errback
/// signatures and work-flow primitives.
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct MessageBody<T: Task>(Vec<u8>, pub(crate) T::Params, pub(crate) MessageBodyEmbed);

impl<T> MessageBody<T>
where
    T: Task,
{
    pub fn new(params: T::Params) -> Self {
        Self(vec![], params, MessageBodyEmbed::default())
    }

    pub fn parts(self) -> (T::Params, MessageBodyEmbed) {
        (self.1, self.2)
    }
}

/// Contains callback / errback signatures and work-flow primitives.
#[derive(Eq, PartialEq, Debug, Default, Serialize, Deserialize)]
pub struct MessageBodyEmbed {
    /// An array of serialized signatures of tasks to call with the result of this task.
    #[serde(default)]
    pub callbacks: Option<Vec<String>>,

    /// An array of serialized signatures of tasks to call if this task results in an error.
    ///
    /// Note that `errbacks` work differently from `callbacks` because the error returned by
    /// a task may not be serializable. Therefore the `errbacks` tasks are passed the task ID
    /// instead of the error itself.
    #[serde(default)]
    pub errbacks: Option<Vec<String>>,

    /// An array of serialized signatures of the remaining tasks in the chain.
    #[serde(default)]
    pub chain: Option<Vec<String>>,

    /// The serialized signature of the chord callback.
    #[serde(default)]
    pub chord: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BodyEncoding {
    Base64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeliveryProperties {
    pub correlation_id: String,
    pub reply_to: Option<String>,
    pub delivery_tag: String,
    pub body_encoding: BodyEncoding,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Delivery {
    pub body: String,
    #[serde(rename = "content-encoding")]
    pub content_encoding: String,
    #[serde(rename = "content-type")]
    pub content_type: String,
    pub headers: MessageHeaders,
    pub properties: DeliveryProperties,
}

impl Delivery {
    pub fn try_deserialize_message(&self) -> Result<Message, ProtocolError> {
        let raw_body = match self.properties.body_encoding {
            BodyEncoding::Base64 => base64::decode(self.body.clone())
                .map_err(|e| ProtocolError::InvalidProperty(format!("body error: {}", e)))?,
        };
        Ok(Message {
            properties: MessageProperties {
                correlation_id: self.properties.correlation_id.clone(),
                content_type: self.content_type.clone(),
                content_encoding: self.content_encoding.clone(),
                reply_to: self.properties.reply_to.clone(),
            },
            headers: MessageHeaders {
                id: self.headers.id.clone(),
                task: self.headers.task.clone(),
                lang: self.headers.lang.clone(),
                root_id: self.headers.root_id.clone(),
                parent_id: self.headers.parent_id.clone(),
                group: self.headers.group.clone(),
                meth: self.headers.meth.clone(),
                shadow: self.headers.shadow.clone(),
                eta: self.headers.eta,
                expires: self.headers.expires,
                retries: self.headers.retries,
                timelimit: self.headers.timelimit,
                argsrepr: self.headers.argsrepr.clone(),
                kwargsrepr: self.headers.kwargsrepr.clone(),
                origin: self.headers.origin.clone(),
                ignore_result: self.headers.ignore_result,
            },
            raw_body,
        })
    }
}

#[cfg(test)]
mod tests;

/// Task execution status.
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

/// A set of all the ready/finished status.
pub static READY_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::SUCCESS, State::FAILURE, State::REVOKED]));

/// A set of all the exception status.
pub static EXCEPTION_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::RETRY, State::FAILURE, State::REVOKED]));

/// A set of all the ready status.
pub static PROPAGATE_STATES: Lazy<HashSet<State>> =
    Lazy::new(|| HashSet::from([State::FAILURE, State::REVOKED]));

impl State {
    /// Check if status is ready/finished.
    pub fn is_ready(&self) -> bool {
        READY_STATES.contains(self)
    }

    /// Check if status is exceptional.
    pub fn is_exception(&self) -> bool {
        EXCEPTION_STATES.contains(self)
    }

    /// Check if status is successful.
    pub fn is_successful(&self) -> bool {
        matches!(self, State::SUCCESS)
    }
}

pub type TaskId = String;
pub type Traceback = ();
pub type Exc = TraceError;
pub type ExecResult<D> = Result<D, Exc>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeneralError {
    exc_type: String,
    exc_message: String,
    exc_module: String,
}

pub type ContentType = MessageContentType;

/// The general part of a task meta.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMetaInfo {
    /// Task id which is an ObjectId.
    pub task_id: TaskId,

    /// Task execution status.
    pub status: State,

    /// Traceback when there was an exception.
    pub traceback: Option<Traceback>,

    /// Children tasks of this task.
    pub children: Vec<String>,

    /// The date time when task status turned into ready.
    pub date_done: Option<String>,

    /// The group id for this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,

    /// The task id of this task's parent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,

    /// Task name.
    ///
    /// This will be the task function's name by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The serialized args passed into the task function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<u8>>,

    /// The serialized kwargs passed into the task function.
    ///
    /// In the domain of rust driver, this field should be avoid since no optional keyword args
    /// in rust. PS: we can hack this by passing kwargs as the last hashmap arg into the task
    /// function.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<Vec<u8>>,

    /// The worker who executes this task.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,

    /// The number of retry times when there was excepted exception.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// The queue to which this task belong.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// The content type of the result.
    #[serde(skip)]
    pub content_type: ContentType,
}

/// Task meta information.
///
/// [crate::protocol::TaskMeta] traces the execution information about a task. It will be maintained
/// by [crate::backend::Backend].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskMeta<R = AnyValue> {
    /// The general part information.
    #[serde(flatten)]
    pub info: TaskMetaInfo,

    /// Serialized result.
    ///
    /// Either be the returned value, or be an exception.
    pub result: Option<R>,
}

impl<R> TaskMeta<R> {
    /// Check if the task is ready/finished.
    pub fn is_ready(&self) -> bool {
        self.info.status.is_ready()
    }

    /// Check if the task is exceptional.
    pub fn is_exception(&self) -> bool {
        self.info.status.is_exception()
    }

    /// Check if the task is successful.
    pub fn is_successful(&self) -> bool {
        self.info.status.is_successful()
    }
}

impl<D> Default for TaskMeta<D>
where
    D: for<'de> Deserialize<'de>,
{
    fn default() -> Self {
        TaskMeta {
            info: Default::default(),
            result: None,
        }
    }
}

impl<T> TryFrom<TaskMeta<AnyValue>> for TaskMeta<ExecResult<T>>
where
    T: for<'de> Deserialize<'de>,
{
    type Error = ContentTypeError;

    fn try_from(meta: TaskMeta<AnyValue>) -> Result<TaskMeta<ExecResult<T>>, Self::Error> {
        let info = meta.info;
        let result = meta
            .result
            .map(AnyValue::into)
            .transpose()?
            .map(|result| inner::restore(result, info.status))
            .transpose()?;

        Ok(TaskMeta { info, result })
    }
}

impl<T> TryFrom<TaskMeta<ExecResult<T>>> for TaskMeta<AnyValue>
where
    T: Serialize,
{
    type Error = ContentTypeError;

    fn try_from(meta: TaskMeta<ExecResult<T>>) -> Result<Self, Self::Error> {
        let info = meta.info;
        let result = meta
            .result
            .map(|result| inner::prepare(&result, &info))
            .transpose()?;

        Ok(TaskMeta { info, result })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupMetaInfo {
    /// The group id
    pub task_id: String,  // note: it's strange to call a group id as task_id, but celery says that
    /// The date time when all the tasks in this group are done
    pub date_done: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GroupMeta<R = AnyValue> {
    /// Group meta info
    #[serde(flatten)]
    pub info: GroupMetaInfo,
    /// The serialized group result
    pub result: Option<R>,
}

impl<R> Default for GroupMeta<R> {
    fn default() -> Self {
        GroupMeta {
            info: GroupMetaInfo::default(),
            result: None,
        }
    }
}

mod inner {
    use super::*;

    type Error = ContentTypeError;

    pub(super) fn restore<D>(data: AnyValue, state: State) -> Result<ExecResult<D>, Error>
    where
        D: for<'de> Deserialize<'de>,
    {
        match state {
            _ if state.is_exception() => Ok(_restore_exception(data)),
            _ if state.is_successful() => Ok(_restore_value(data)),
            _ => Err(Error::Unknown),
        }
    }

    pub(super) fn prepare<D>(result: &ExecResult<D>, info: &TaskMetaInfo) -> Result<AnyValue, Error>
    where
        D: Serialize,
    {
        match result {
            Ok(data) => _prepare_value(data, info.content_type),
            Err(err) if info.status.is_exception() => _prepare_exception(err, info.content_type),
            Err(_) => Err(ContentTypeError::Unknown),
        }
    }

    fn _restore_value<D>(data: AnyValue) -> ExecResult<D>
    where
        D: for<'de> Deserialize<'de>,
    {
        // todo: unwrap or ?
        Ok(data.into().unwrap())
    }

    fn _restore_exception<D>(data: AnyValue) -> ExecResult<D>
    where
        D: for<'de> Deserialize<'de>,
    {
        // todo: unwrap or ?
        let err: GeneralError = data.into().unwrap();
        let err = match err.exc_type.as_str() {
            "TaskError" => Exc::TaskError(TaskError::ExpectedError(err.exc_message)),
            "TimeoutError" => Exc::TaskError(TaskError::TimeoutError),
            "RetryTaskError" => Exc::TaskError(TaskError::Retry(None /*todo*/)),
            "RevokedError" => Exc::TaskError(TaskError::RevokedError(err.exc_message)),
            _ => Exc::TaskError(TaskError::UnexpectedError(err.exc_message)), // todo
        };
        Err(err)
    }

    fn _prepare_value<D: Serialize>(result: &D, s: ContentType) -> Result<AnyValue, Error> {
        // todo
        //   if result is ResultBase {
        //       result.as_tuple()
        //   }
        s.try_to_value(result)
    }

    fn _prepare_exception(exc: &Exc, s: ContentType) -> Result<AnyValue, Error> {
        let (typ, msg, module) = match exc {
            Exc::TaskError(err) => match err {
                TaskError::ExpectedError(err) => ("TaskError", err.as_str(), "celery.exceptions"),
                TaskError::UnexpectedError(err) => ("TaskError", err.as_str(), "celery.exceptions"),
                TaskError::TimeoutError => ("TimeoutError", "", "celery.exceptions"),
                TaskError::Retry(_time) => {
                    ("RetryTaskError", "todo!" /*todo*/, "celery.exceptions")
                }
                TaskError::RevokedError(err) => ("RevokedError", err.as_str(), "celery.exceptions"),
            },
            Exc::ExpirationError => ("TaskError", "", "celery.exceptions"),
            Exc::Retry(_time) => ("RetryTaskError", "todo!", "celery.exceptions"),
        };

        let exc_struct = GeneralError {
            exc_type: typ.into(),
            exc_message: msg.into(),
            exc_module: module.into(),
        };

        s.try_to_value(&exc_struct)
    }
}
