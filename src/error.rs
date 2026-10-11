use std::{fmt, time::Duration};

use crate::protocol::ErrorCode;

/// A request the SERVER rejected, as opposed to a transport failure.
///
/// Carries the HTTP status, the server's own status word ("Unauthorized")
/// and its message, plus what was being attempted so a log line says so.
/// `Display` is for the logs; `msg` is the server's words for a person.
#[derive(Debug, Clone)]
pub struct ApiError {
    pub http_status: u16,
    /// The server's status word, e.g. "NotFound"; the bare HTTP status when
    /// the body was not an envelope (a proxy's HTML page, a 0.6 fallthrough).
    pub status: String,
    pub msg: Option<String>,
    pub method: String,
    pub resource: String,
    pub operation: String,
}

impl ApiError {
    /// 401 / 403: mostly a request that raced a token refresh.
    pub fn is_auth(&self) -> bool {
        matches!(self.http_status, 401 | 403)
    }

    /// The HTTP status as the protocol's error code (unknown codes read as
    /// `InternalServerError`, as the server itself maps them).
    pub fn code(&self) -> ErrorCode {
        ErrorCode::from_u16(self.http_status)
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} /{}/{} failed ({} {})",
            self.method, self.resource, self.operation, self.http_status, self.status
        )?;
        if let Some(msg) = &self.msg {
            write!(f, ": {msg}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ApiError {}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server answered and said no. Boxed: the error carries what was
    /// attempted, and `Result<()>` must stay small.
    #[error(transparent)]
    Api(Box<ApiError>),
    /// The request never got an answer: DNS, TLS, a refused connection, a timeout.
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    /// The WebSocket upgrade was refused or the connection failed.
    #[error("socket: {0}")]
    Socket(String),
    #[error("socket is not connected")]
    NotConnected,
    #[error("no reply within {0:?}")]
    Timeout(Duration),
    #[error("bad url: {0}")]
    Url(String),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The registration step (the app's REST call for a connection id) failed.
    #[error("registration: {0}")]
    Registration(String),
}

impl From<ApiError> for Error {
    fn from(e: ApiError) -> Self {
        Error::Api(Box::new(e))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What a person reads when a request fails: the server's own words when
/// it sent some, else the error's.
pub fn problem_text(e: &Error, fallback: &str) -> String {
    match e {
        Error::Api(api) if api.msg.is_some() => api.msg.clone().unwrap_or_default(),
        other => {
            let s = other.to_string();
            if s.is_empty() {
                fallback.to_string()
            } else {
                s
            }
        }
    }
}
