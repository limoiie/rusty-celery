use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ContentTypeError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnyValue {
    JSON(serde_json::Value),
    #[cfg(feature = "serde_yaml")]
    YAML(serde_yaml::Value),
}

impl From<serde_json::Value> for AnyValue {
    fn from(value: serde_json::Value) -> Self {
        AnyValue::JSON(value)
    }
}

#[cfg(feature = "serde_yaml")]
impl From<serde_yaml::Value> for AnyValue {
    fn from(value: serde_yaml::Value) -> Self {
        AnyValue::YAML(value)
    }
}

impl Serialize for AnyValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            AnyValue::JSON(value) => value.serialize(serializer),
            #[cfg(feature = "serde_yaml")]
            AnyValue::YAML(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for AnyValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match std::any::type_name::<D>() {
            de_name if de_name.contains("serde_json") => {
                serde_json::Value::deserialize(deserializer).map(|s| s.into())
            }
            #[cfg(feature = "serde_yaml")]
            de_name if de_name.contains("serde_yaml") => {
                serde_yaml::Value::deserialize(deserializer).map(|s| s.into())
            }
            _ => panic!("Invalid serializer NULL"),
        }
    }
}

impl AnyValue {
    pub fn into<D>(self) -> Result<D, ContentTypeError>
    where
        D: for<'de> Deserialize<'de>,
    {
        match self {
            AnyValue::JSON(value) => serde_json::from_value(value).map_err(ContentTypeError::from),
            #[cfg(feature = "serde_yaml")]
            AnyValue::YAML(value) => serde_yaml::from_value(value).map_err(ContentTypeError::from),
        }
    }

    pub fn kind(&self) -> SerializerKind {
        match self {
            AnyValue::JSON(_) => SerializerKind::JSON,
            #[cfg(feature = "serde_yaml")]
            AnyValue::YAML(_) => SerializerKind::YAML,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub enum SerializerKind {
    #[default]
    JSON,
    #[cfg(feature = "serde_yaml")]
    YAML,
}

impl SerializerKind {
    pub fn dump<D>(&self, data: &D) -> (&'static str, &'static str, String)
    where
        D: Serialize,
    {
        match self {
            SerializerKind::JSON => (
                "application/json",
                "utf-8",
                serde_json::to_string(data).unwrap(),
            ),
            #[cfg(feature = "serde_yaml")]
            SerializerKind::YAML => (
                "application/x-yaml",
                "utf-8",
                serde_yaml::to_string(data).unwrap(),
            ),
        }
    }

    pub fn load<D>(&self, data: &String) -> D
    where
        D: for<'de> Deserialize<'de>,
    {
        log::debug!(
            "Try decoding as {} with `{}'",
            std::any::type_name::<D>(),
            data
        );

        match self {
            SerializerKind::JSON => serde_json::from_str(data.as_str()).unwrap(),
            #[cfg(feature = "serde_yaml")]
            SerializerKind::YAML => serde_yaml::from_str(data.as_str()).unwrap(),
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
            SerializerKind::JSON => serde_json::to_value(data)
                .map(Into::into)
                .map_err(ContentTypeError::from),
            #[cfg(feature = "serde_yaml")]
            SerializerKind::YAML => serde_yaml::to_value(data)
                .map(Into::into)
                .map_err(ContentTypeError::from),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_serde_any_value() {
        let data = HashMap::from([("name", 0x7)]);

        let kind = SerializerKind::JSON;
        let value = kind.to_value(&data);

        let (_, _, serialized) = kind.dump(&value);
        let output_value = kind.load(&serialized);

        assert_eq!(value, output_value);
    }
}
