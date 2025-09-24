use arrow::error::ArrowError;
use pyo3::exceptions::PyOSError;
use pyo3::PyErr;
use std::error;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    ArrowError(ArrowError),
    ParquetError(parquet::errors::ParquetError),
    SyncError(String),
    ChannelError(String),
    NotImplementedError(String),
    DataError(String),
    ConnectionError(String),
    DatabaseError(String),
    IOError(String),
    OracleError(techx_rust_oracle::Error),
    OdbcError(odbc_api::Error),
    PostgresError(postgres::error::Error),
    OpenDalError(opendal::Error),
    EncryptionSDKError(aws_esdk::types::error::Error),
}

impl Error {
    pub fn sync_error(source: &str, cause: &(dyn std::any::Any + Send)) -> Self {
        Error::SyncError(if let Some(s) = cause.downcast_ref::<&str>() {
            format!("{source} panic occurred: {s}")
        } else if let Some(s) = cause.downcast_ref::<String>() {
            format!("{source} panic occurred: {s}")
        } else {
            format!("{source} panic occurred")
        })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ArrowError(ref e) => e.fmt(f),
            Error::ParquetError(ref e) => e.fmt(f),
            Error::OracleError(ref e) => e.fmt(f),
            Error::OdbcError(ref e) => e.fmt(f),
            Error::SyncError(ref message) => {
                write!(f, "Join handle error: {}", message)
            }
            Error::ChannelError(ref message) => {
                write!(f, "Sender/Receiver error: {}", message)
            }
            Error::ConnectionError(ref message) => {
                write!(f, "Connection error: {}", message)
            }
            Error::DatabaseError(ref message) => {
                write!(f, "Database error: {}", message)
            }
            Error::IOError(ref message) => {
                write!(f, "IO error: {}", message)
            }
            Error::OpenDalError(ref e) => e.fmt(f),
            Error::NotImplementedError(ref message) => {
                write!(f, "Not Implemented error: {}", message)
            }
            Error::DataError(ref message) => {
                write!(f, "Data error: {message}")
            }
            Error::EncryptionSDKError(ref e) => e.fmt(f),
            Error::PostgresError(ref e) => e.fmt(f),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match *self {
            // The cause is the underlying implementation error type. Is implicitly
            // cast to the trait object `&error::Error`. This works because the
            // underlying type already implements the `Error` trait.
            Error::ArrowError(ref e) => Some(e),
            Error::ParquetError(ref e) => Some(e),
            Error::OracleError(ref e) => Some(e),
            Error::OdbcError(ref e) => Some(e),
            Error::OpenDalError(ref e) => Some(e),
            Error::EncryptionSDKError(ref e) => Some(e),
            Error::PostgresError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Error {
        Error::ArrowError(err)
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(err: parquet::errors::ParquetError) -> Error {
        Error::ParquetError(err)
    }
}

impl From<techx_rust_oracle::Error> for Error {
    fn from(err: techx_rust_oracle::Error) -> Error {
        Error::OracleError(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::ArrowError(ArrowError::from(err))
    }
}

impl From<odbc_api::Error> for Error {
    fn from(err: odbc_api::Error) -> Error {
        Error::OdbcError(err)
    }
}

impl<T> From<crossbeam_channel::SendError<T>> for Error {
    fn from(err: crossbeam_channel::SendError<T>) -> Self {
        Error::ChannelError(err.to_string())
    }
}

impl From<crossbeam_channel::RecvError> for Error {
    fn from(err: crossbeam_channel::RecvError) -> Self {
        Error::ChannelError(err.to_string())
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Error::ArrowError(ArrowError::ExternalError(Box::new(err)))
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::ChannelError(err.to_string())
    }
}

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

impl From<opendal::Error> for Error {
    fn from(err: opendal::Error) -> Self {
        Error::OpenDalError(err)
    }
}

impl From<aws_esdk::types::error::Error> for Error {
    fn from(err: aws_esdk::types::error::Error) -> Self {
        Error::EncryptionSDKError(err)
    }
}

impl From<postgres::error::Error> for Error {
    fn from(err: postgres::error::Error) -> Self {
        Error::PostgresError(err)
    }
}
