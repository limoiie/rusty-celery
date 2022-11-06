use bstr::ByteVec;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum AnyValue {
    JSON(serde_json::Value),
    #[cfg(feature = "serde_yaml")]
    YAML(serde_yaml::Value),
    #[default]
    NULL,
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
            AnyValue::NULL => panic!("Invalid serializer NULL"),
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

#[derive(Copy, Clone, Debug)]
pub enum SerializerKind {
    JSON,
    #[cfg(feature = "serde_yaml")]
    YAML,
}

impl SerializerKind {
    pub fn dump_bytes(&self, data: Vec<u8>) -> (&'static str, &'static str, String) {
        ("application/data", "binary", data.into_string_lossy()) // fixme: encoding in a correct way
    }

    pub fn dump_string(&self, data: String) -> (&'static str, &'static str, String) {
        ("text/plain", "utf-8", data)
    }

    pub fn dump<D: Serialize>(&self, data: &D) -> (&'static str, &'static str, String) {
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

    pub fn load<D: for<'de> Deserialize<'de>>(&self, data: &String) -> D {
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

    pub fn to_value<T: Serialize>(&self, data: &T) -> AnyValue {
        match self {
            SerializerKind::JSON => serde_json::to_value(data).unwrap().into(),
            #[cfg(feature = "serde_yaml")]
            SerializerKind::YAML => serde_yaml::to_value(data).unwrap().into(),
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
