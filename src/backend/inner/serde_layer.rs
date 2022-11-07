use crate::backend::{Exc, TaskResult};
use crate::error::TaskError;
use crate::kombu_serde::{AnyValue, SerializerKind};
use crate::protocol::State;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeneralError {
    exc_type: String,
    exc_message: String,
    exc_module: String,
}

pub trait BackendSerdeLayer: Send + Sync + Sized {
    fn _serializer(&self) -> SerializerKind;

    fn _encode<D: Serialize>(&self, data: &D) -> String {
        self._serializer().dump(data).2
    }

    fn _decode<D: for<'de> Deserialize<'de>>(&self, payload: String) -> D {
        self._serializer().load(&payload)
    }

    fn _prepare_result<D: Serialize>(&self, result: TaskResult<D>, state: State) -> AnyValue {
        match result {
            Ok(ref data) => self._prepare_value(data),
            Err(ref err) if state.is_exception() => self._prepare_exception(err),
            Err(_) => todo!(),
        }
    }

    fn _prepare_value<D: Serialize>(&self, result: &D) -> AnyValue {
        // todo
        //   if result is ResultBase {
        //       result.as_tuple()
        //   }
        self._serializer().data_to_value(result)
    }

    fn _prepare_exception(&self, exc: &Exc) -> AnyValue {
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

        self._serializer().data_to_value(&exc_struct)
    }

    fn _restore_result<D>(&self, data: AnyValue, state: State) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        match state {
            _ if state.is_exception() => Some(Err(self._restore_exception(data))),
            _ if state.is_successful() => Some(Ok(self._restore_value(data))),
            _ => None,
        }
    }

    fn _restore_value<D>(&self, data: AnyValue) -> D
    where
        D: for<'de> Deserialize<'de>,
    {
        self._serializer().value_to_data(data).unwrap()
    }

    fn _restore_exception(&self, data: AnyValue) -> Exc {
        let err: GeneralError = self._serializer().value_to_data(data).unwrap();
        match err.exc_type.as_str() {
            "TaskError" => Exc::TaskError(TaskError::ExpectedError(err.exc_message)),
            "TimeoutError" => Exc::TaskError(TaskError::TimeoutError),
            "RetryTaskError" => Exc::TaskError(TaskError::Retry(None /*todo*/)),
            "RevokedError" => Exc::TaskError(TaskError::RevokedError(err.exc_message)),
            _ => Exc::TaskError(TaskError::UnexpectedError(err.exc_message)), // todo
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use crate::kombu_serde::{AnyValue, SerializerKind};

    use crate::backend::*;

    #[test]
    fn test_serde_result_meta() {
        let serializer = SerializerKind::JSON;

        let result_meta = TaskMeta {
            task_id: "fake-id".to_owned(),
            status: State::SUCCESS,
            result: AnyValue::JSON(serde_json::to_value(11).unwrap()),
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
        };

        let (_content_type, _encoding, data) = serializer.dump(&result_meta);
        let output_result_meta = serializer.load(&data);

        assert_eq!(result_meta, output_result_meta);
    }
}
