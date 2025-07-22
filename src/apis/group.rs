use crate::{
    client::HsdsClient,
    error::HsdsResult,
    models::{Group, GroupCreateRequest},
};
use reqwest::Method;
use log::{debug, info};

/// Group API operations
pub struct GroupApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> GroupApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// Create a new Group
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `request` - Group creation parameters (optional)
    pub async fn create_group(
        &self,
        domain: &str,
        request: Option<GroupCreateRequest>,
    ) -> HsdsResult<Group> {
        info!("Creating group in domain: {}", domain);
        let mut req = self.client.request(Method::POST, "/groups").await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP POST /groups with domain={}", domain);

        if let Some(body) = request {
            debug!("Request body: {:?}", body);
            req = req.json(&body);
        }

        self.client.execute(req).await
    }

    /// List all Groups in Domain
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    pub async fn list_groups(&self, domain: &str) -> HsdsResult<serde_json::Value> {
        info!("Listing groups in domain: {}", domain);
        let mut req = self.client.request(Method::GET, "/groups").await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP GET /groups with domain={}", domain);

        self.client.execute(req).await
    }

    /// Get information about a specific Group
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the group
    /// * `get_alias` - Whether to include alias paths (0 or 1)
    pub async fn get_group(
        &self,
        domain: &str,
        group_id: &str,
        get_alias: Option<u8>,
    ) -> HsdsResult<Group> {
        info!("Getting group {} in domain: {}", group_id, domain);
        let path = format!("/groups/{}", group_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP GET {} with domain={}", path, domain);

        if let Some(alias) = get_alias {
            debug!("Including alias information (getalias={})", alias);
            req = req.query(&[("getalias", alias)]);
        }

        self.client.execute(req).await
    }

    /// Delete a Group
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the group
    pub async fn delete_group(
        &self,
        domain: &str,
        group_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        info!("Deleting group {} in domain: {}", group_id, domain);
        let path = format!("/groups/{}", group_id);
        let mut req = self.client.request(Method::DELETE, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        debug!("HTTP DELETE {} with domain={}", path, domain);

        self.client.execute(req).await
    }
}
