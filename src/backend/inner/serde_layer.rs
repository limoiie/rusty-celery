use serde::{Deserialize, Serialize};

use crate::protocol::ContentType;

pub trait BackendSerdeLayer: Send + Sync + Sized {
    fn _serializer(&self) -> ContentType;

    fn _encode<D: Serialize>(&self, data: &D) -> String {
        self._serializer().dump(data)
    }

    fn _decode<D: for<'de> Deserialize<'de>>(&self, payload: String) -> D {
        self._serializer().load(&payload)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use crate::backend::*;
    use crate::kombu_serde::AnyValue;
    use crate::protocol::{ContentType, TaskMetaInfo};

    #[test]
    fn test_serde_result_meta() {
        let serializer = ContentType::Json;

        let result_meta = TaskMeta {
            info: TaskMetaInfo {
                task_id: "fake-id".to_owned(),
                status: State::SUCCESS,
                traceback: None,
                children: vec![],
                date_done: Some(DateTime::<Utc>::from(std::time::SystemTime::now()).to_rfc3339()),
                group_id: None,
                parent_id: None,
                name: None,
                args: None,
                kwargs: None,
                worker: Some("fake-hostname".to_owned()),
                retries: Some(10),
                queue: None,
                content_type: ContentType::Json,
            },
            result: Some(AnyValue::JSON(serde_json::to_value(11).unwrap())),
        };

        let data = serializer.dump(&result_meta);
        let output_result_meta = serializer.load(&data);

        assert_eq!(result_meta, output_result_meta);
    }
}
