use crate::{
    client::HsdsClient,
    error::HsdsResult,
    models::{Domain, DomainCreateRequest},
};
use reqwest::Method;
use log::{debug, info};

/// Domain API operations
pub struct DomainApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> DomainApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// Create a new Domain or Folder
    /// 
    /// # Arguments
    /// * `domain` - Domain path (e.g., "/home/user/myfile.h5")
    /// * `request` - Domain creation parameters
    pub async fn create_domain(
        &self,
        domain: &str,
        request: Option<DomainCreateRequest>,
    ) -> HsdsResult<Domain> {
        info!("Creating domain: {}", domain);
        let mut req = self.client.request(Method::PUT, "/").await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP PUT / with domain={}", domain);

        if let Some(body) = request {
            debug!("Request body: {:?}", body);
            req = req.json(&body);
        }

        self.client.execute(req).await
    }

    /// Get information about a domain
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    pub async fn get_domain(&self, domain: &str) -> HsdsResult<Domain> {
        info!("Getting domain: {}", domain);
        let mut req = self.client.request(Method::GET, "/").await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP GET / with domain={}", domain);

        self.client.execute(req).await
    }

    /// Delete a domain
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    pub async fn delete_domain(&self, domain: &str) -> HsdsResult<serde_json::Value> {
        info!("Deleting domain: {}", domain);
        let mut req = self.client.request(Method::DELETE, "/").await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP DELETE / with domain={}", domain);

        self.client.execute(req).await
    }

    /// List domains (when no domain parameter provided)
    pub async fn list_domains(&self) -> HsdsResult<serde_json::Value> {
        info!("Listing domains");
        let req = self.client.request(Method::GET, "/").await?;
        debug!("HTTP GET / (no domain parameter)");
        
        self.client.execute(req).await
    }

    /// Create a folder (convenience method)
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    pub async fn create_folder(&self, domain: &str) -> HsdsResult<Domain> {
        info!("Creating folder: {}", domain);
        let request = DomainCreateRequest { folder: Some(1) };
        debug!("Using folder creation parameters: {:?}", request);
        self.create_domain(domain, Some(request)).await
    }
}
