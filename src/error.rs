use thiserror::Error;

/// HSDS client error types
#[derive(Error, Debug)]
pub enum HsdsError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON serialization/deserialization failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("URL parsing failed: {0}")]
    Url(#[from] url::ParseError),

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },

    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),

    #[error("Domain not found: {0}")]
    DomainNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Invalid response format: {0}")]
    InvalidResponse(String),

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

/// Result type for HSDS operations
pub type HsdsResult<T> = Result<T, HsdsError>;

impl HsdsError {
    /// Create an API error from a status code and message
    pub fn api_error(status: u16, message: impl Into<String>) -> Self {
        Self::Api {
            status,
            message: message.into(),
        }
    }

    /// Create an authentication error
    pub fn auth_error(message: impl Into<String>) -> Self {
        Self::Auth(message.into())
    }

    /// Create an invalid parameter error
    pub fn invalid_param(message: impl Into<String>) -> Self {
        Self::InvalidParameter(message.into())
    }
}
