use crate::error::{HsdsError, HsdsResult};
use base64::{Engine, engine::general_purpose};

/// Authentication trait for HSDS API
#[async_trait::async_trait]
pub trait Authentication: Send + Sync {
    /// Apply authentication to the request headers
    async fn apply_auth(&self, headers: &mut reqwest::header::HeaderMap) -> HsdsResult<()>;
}

/// Basic authentication using username/password
#[derive(Debug, Clone)]
pub struct BasicAuth {
    username: String,
    password: String,
}

impl BasicAuth {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

#[async_trait::async_trait]
impl Authentication for BasicAuth {
    async fn apply_auth(&self, headers: &mut reqwest::header::HeaderMap) -> HsdsResult<()> {
        let credentials = format!("{}:{}", self.username, self.password);
        let encoded = general_purpose::STANDARD.encode(credentials.as_bytes());
        let auth_value = format!("Basic {}", encoded);
        
        headers.insert(
            reqwest::header::AUTHORIZATION,
            auth_value.parse()
                .map_err(|e| HsdsError::auth_error(format!("Invalid auth header: {}", e)))?
        );
        
        Ok(())
    }
}

/// Bearer token authentication
#[derive(Debug, Clone)]
pub struct BearerAuth {
    token: String,
}

impl BearerAuth {
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

#[async_trait::async_trait]
impl Authentication for BearerAuth {
    async fn apply_auth(&self, headers: &mut reqwest::header::HeaderMap) -> HsdsResult<()> {
        let auth_value = format!("Bearer {}", self.token);
        headers.insert(
            reqwest::header::AUTHORIZATION,
            auth_value.parse()
                .map_err(|e| HsdsError::auth_error(format!("Invalid auth header: {}", e)))?
        );
        
        Ok(())
    }
}

/// No authentication
#[derive(Debug, Clone)]
pub struct NoAuth;

#[async_trait::async_trait]
impl Authentication for NoAuth {
    async fn apply_auth(&self, _headers: &mut reqwest::header::HeaderMap) -> HsdsResult<()> {
        Ok(())
    }
}
