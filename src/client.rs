use crate::{
    auth::Authentication,
    error::{HsdsError, HsdsResult},
    models::ErrorResponse,
    apis::{DomainApi, GroupApi, LinkApi, DatasetApi, DatatypeApi, AttributeApi},
};
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde::Deserialize;
use std::sync::Arc;
use url::Url;

/// Main HSDS client
#[derive(Clone)]
pub struct HsdsClient {
    client: Client,
    base_url: Url,
    auth: Arc<dyn Authentication>,
}

impl HsdsClient {
    /// Create a new HSDS client with authentication
    pub fn new(
        base_url: impl AsRef<str>,
        auth: impl Authentication + 'static,
    ) -> HsdsResult<Self> {
        let base_url = Url::parse(base_url.as_ref())?;
        let client = Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()?;

        Ok(Self {
            client,
            base_url,
            auth: Arc::new(auth),
        })
    }

    /// Create a new HSDS client with custom reqwest client
    pub fn with_client(
        client: Client,
        base_url: impl AsRef<str>,
        auth: impl Authentication + 'static,
    ) -> HsdsResult<Self> {
        let base_url = Url::parse(base_url.as_ref())?;

        Ok(Self {
            client,
            base_url,
            auth: Arc::new(auth),
        })
    }

    /// Get the base URL
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Get Domain API
    pub fn domains(&self) -> DomainApi<'_> {
        DomainApi::new(self)
    }

    /// Get Group API
    pub fn groups(&self) -> GroupApi<'_> {
        GroupApi::new(self)
    }

    /// Get Link API
    pub fn links(&self) -> LinkApi<'_> {
        LinkApi::new(self)
    }

    /// Get Dataset API
    pub fn datasets(&self) -> DatasetApi<'_> {
        DatasetApi::new(self)
    }

    /// Get Datatype API
    pub fn datatypes(&self) -> DatatypeApi<'_> {
        DatatypeApi::new(self)
    }

    /// Get Attribute API
    pub fn attributes(&self) -> AttributeApi<'_> {
        AttributeApi::new(self)
    }

    /// Build a request to the given path with authentication
    pub async fn request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> HsdsResult<RequestBuilder> {
        let url = self.base_url.join(path)?;
        let mut request = self.client.request(method, url);

        // Apply authentication
        let mut headers = reqwest::header::HeaderMap::new();
        self.auth.apply_auth(&mut headers).await?;
        
        for (name, value) in headers.iter() {
            request = request.header(name, value);
        }

        Ok(request)
    }

    /// Execute a request and handle common error cases
    pub async fn execute<T>(&self, request: RequestBuilder) -> HsdsResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = request.send().await?;
        self.handle_response(response).await
    }

    /// Execute a request and return raw bytes
    pub async fn execute_bytes(&self, request: RequestBuilder) -> HsdsResult<bytes::Bytes> {
        let response = request.send().await?;
        self.handle_response_bytes(response).await
    }

    /// Handle response and deserialize JSON
    async fn handle_response<T>(&self, response: Response) -> HsdsResult<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let status = response.status();
        
        if status.is_success() {
            let json = response.json::<T>().await?;
            Ok(json)
        } else {
            self.handle_error_response(status, response).await
        }
    }

    /// Handle response and return raw bytes
    async fn handle_response_bytes(&self, response: Response) -> HsdsResult<bytes::Bytes> {
        let status = response.status();
        
        if status.is_success() {
            let bytes = response.bytes().await?;
            Ok(bytes)
        } else {
            self.handle_error_response(status, response).await
        }
    }

    /// Handle error responses
    async fn handle_error_response<T>(&self, status: StatusCode, response: Response) -> HsdsResult<T> {
        // Try to parse error response
        let error_message = match response.json::<ErrorResponse>().await {
            Ok(error_resp) => {
                error_resp.message
                    .or(error_resp.error)
                    .unwrap_or_else(|| format!("HTTP {}", status))
            }
            Err(_) => format!("HTTP {}", status),
        };

        match status {
            StatusCode::UNAUTHORIZED => Err(HsdsError::auth_error(error_message)),
            StatusCode::FORBIDDEN => Err(HsdsError::PermissionDenied(error_message)),
            StatusCode::NOT_FOUND => Err(HsdsError::ObjectNotFound(error_message)),
            StatusCode::BAD_REQUEST => Err(HsdsError::invalid_param(error_message)),
            _ => Err(HsdsError::api_error(status.as_u16(), error_message)),
        }
    }

    /// Add domain query parameter to request
    pub fn with_domain(request: RequestBuilder, domain: &str) -> RequestBuilder {
        request.query(&[("domain", domain)])
    }

    /// Add pagination parameters to request
    pub fn with_pagination(
        request: RequestBuilder,
        limit: Option<u32>,
        marker: Option<&str>,
    ) -> RequestBuilder {
        let mut req = request;
        
        if let Some(limit) = limit {
            req = req.query(&[("Limit", limit.to_string())]);
        }
        
        if let Some(marker) = marker {
            req = req.query(&[("Marker", marker)]);
        }
        
        req
    }

    /// Add selection parameter for dataset queries
    pub fn with_selection(request: RequestBuilder, selection: &str) -> RequestBuilder {
        request.query(&[("select", selection)])
    }

    /// Add query parameter for dataset filtering
    pub fn with_query(request: RequestBuilder, query: &str, limit: Option<u32>) -> RequestBuilder {
        let mut req = request.query(&[("query", query)]);
        
        if let Some(limit) = limit {
            req = req.query(&[("Limit", limit.to_string())]);
        }
        
        req
    }
}
