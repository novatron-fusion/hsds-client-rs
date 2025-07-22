use crate::{
    client::HsdsClient,
    error::HsdsResult,
    models::{Links, LinkCreateRequest},
};
use reqwest::Method;

/// Link API operations
pub struct LinkApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> LinkApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// List all Links in a Group
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the group
    /// * `limit` - Maximum number of links to return
    /// * `marker` - Link name to start listing from
    pub async fn list_links(
        &self,
        domain: &str,
        group_id: &str,
        limit: Option<u32>,
        marker: Option<&str>,
    ) -> HsdsResult<Links> {
        let path = format!("/groups/{}/links", group_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = HsdsClient::with_pagination(req, limit, marker);

        self.client.execute(req).await
    }

    /// Create a Link in a Group
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the group
    /// * `link_name` - Name of the link
    /// * `request` - Link creation parameters
    pub async fn create_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
        request: LinkCreateRequest,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/groups/{}/links/{}", group_id, 
                          urlencoding::encode(link_name));
        let mut req = self.client.request(Method::PUT, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&request);

        self.client.execute(req).await
    }

    /// Get information about a Link
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the group
    /// * `link_name` - Name of the link
    pub async fn get_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/groups/{}/links/{}", group_id, 
                          urlencoding::encode(link_name));
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Delete a Link
    /// 
    /// # Arguments
    /// * `domain` - Domain path  
    /// * `group_id` - UUID of the group
    /// * `link_name` - Name of the link
    pub async fn delete_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/groups/{}/links/{}", group_id, 
                          urlencoding::encode(link_name));
        let mut req = self.client.request(Method::DELETE, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Create a hard link (convenience method)
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the source group
    /// * `link_name` - Name of the link
    /// * `target_id` - UUID of the target object
    pub async fn create_hard_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
        target_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let request = LinkCreateRequest {
            id: Some(target_id.to_string()),
            h5path: None,
            h5domain: None,
        };
        
        self.create_link(domain, group_id, link_name, request).await
    }

    /// Create a soft link (convenience method)
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the source group  
    /// * `link_name` - Name of the link
    /// * `target_path` - Path to the target object
    pub async fn create_soft_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
        target_path: &str,
    ) -> HsdsResult<serde_json::Value> {
        let request = LinkCreateRequest {
            id: None,
            h5path: Some(target_path.to_string()),
            h5domain: None,
        };
        
        self.create_link(domain, group_id, link_name, request).await
    }

    /// Create an external link (convenience method)
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `group_id` - UUID of the source group
    /// * `link_name` - Name of the link
    /// * `target_path` - Path to the target object
    /// * `target_domain` - External domain URL
    pub async fn create_external_link(
        &self,
        domain: &str,
        group_id: &str,
        link_name: &str,
        target_path: &str,
        target_domain: &str,
    ) -> HsdsResult<serde_json::Value> {
        let request = LinkCreateRequest {
            id: None,
            h5path: Some(target_path.to_string()),
            h5domain: Some(target_domain.to_string()),
        };
        
        self.create_link(domain, group_id, link_name, request).await
    }
}
