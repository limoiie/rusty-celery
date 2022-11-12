use impl_trait_for_tuples::impl_for_tuples;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::ContentTypeError;
use crate::protocol::ContentType;

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

    pub fn kind(&self) -> ContentType {
        match self {
            AnyValue::JSON(_) => ContentType::Json,
            #[cfg(feature = "serde_yaml")]
            AnyValue::YAML(_) => ContentType::Yaml,
        }
    }

    pub fn bury_vec(vec: Vec<AnyValue>) -> Option<Self> {
        #[allow(unused)]
        vec.iter()
            .map(AnyValue::kind)
            .map(Some)
            .reduce(|a, b| if matches!(a, b) { a } else { None })
            .flatten()
            .map(|kind| kind.to_value(&vec))
    }
}

pub trait FromVec: Sized {
    fn from_vec(vec: Vec<AnyValue>) -> Result<Self, ContentTypeError>;
}

#[impl_for_tuples(5)]
#[tuple_types_no_default_trait_bound]
impl FromVec for TupleIdentifier {
    for_tuples!( where #( TupleIdentifier: for <'de> Deserialize<'de> )* );

    fn from_vec(vec: Vec<AnyValue>) -> Result<Self, ContentTypeError> {
        let mut iter = vec.into_iter();
        Ok(for_tuples!( ( #( iter.next().unwrap().into::<TupleIdentifier>()? ),* ) ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_serde_any_value() {
        let data = HashMap::from([("name", 0x7)]);

        let kind = ContentType::Json;
        let value = kind.to_value(&data);

        let serialized = kind.dump(&value);
        let output_value = kind.load(&serialized);

        assert_eq!(value, output_value);
    }
}
