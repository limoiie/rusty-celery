use crate::backend::{Exc, TaskResult};
use crate::error::TaskError;
use crate::kombu_serde::{AnyValue, SerializerKind};
use crate::states::State;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeneralError {
    exc_type: String,
    exc_message: String,
    exc_module: String,
}

pub trait BackendSerdeLayer: Send + Sync + Sized {
    fn serializer(&self) -> SerializerKind;

    fn encode<D: Serialize>(&self, data: &D) -> String {
        self.serializer().dump(data).2
    }

    fn decode<D: for<'de> Deserialize<'de>>(&self, payload: String) -> D {
        self.serializer().load(&payload)
    }

    fn prepare_result<D: Serialize>(&self, result: TaskResult<D>, state: State) -> AnyValue {
        match result {
            Ok(ref data) => self.prepare_value(data),
            Err(ref err) if state.is_exception() => self.prepare_exception(err),
            Err(_) => todo!(),
        }
    }

    fn prepare_value<D: Serialize>(&self, result: &D) -> AnyValue {
        // todo
        //   if result is ResultBase {
        //       result.as_tuple()
        //   }
        self.serializer().data_to_value(result)
    }

    fn prepare_exception(&self, exc: &Exc) -> AnyValue {
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

        self.serializer().data_to_value(&exc_struct)
    }

    fn recover_result<D>(&self, data: AnyValue, state: State) -> Option<TaskResult<D>>
    where
        D: for<'de> Deserialize<'de>,
    {
        match state {
            _ if state.is_exception() => Some(Err(self.recover_exception(data))),
            _ if state.is_successful() => Some(Ok(self.recover_value(data))),
            _ => None,
        }
    }

    fn recover_value<D>(&self, data: AnyValue) -> D
    where
        D: for<'de> Deserialize<'de>,
    {
        self.serializer().value_to_data(data).unwrap()
    }

    fn recover_exception(&self, data: AnyValue) -> Exc {
        let err: GeneralError = self.serializer().value_to_data(data).unwrap();
        match err.exc_type.as_str() {
            "TaskError" => Exc::TaskError(TaskError::ExpectedError(err.exc_message)),
            "TimeoutError" => Exc::TaskError(TaskError::TimeoutError),
            "RetryTaskError" => Exc::TaskError(TaskError::Retry(None /*todo*/)),
            "RevokedError" => Exc::TaskError(TaskError::RevokedError(err.exc_message)),
            _ => Exc::TaskError(TaskError::UnexpectedError(err.exc_message)), // todo
        }
    }
}
