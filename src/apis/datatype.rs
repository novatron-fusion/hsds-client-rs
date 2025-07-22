use crate::{
    client::HsdsClient,
    error::HsdsResult,
};
use reqwest::Method;

/// Datatype API operations
pub struct DatatypeApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> DatatypeApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// Commit a Datatype to the Domain
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `datatype_def` - Datatype definition
    pub async fn commit_datatype(
        &self,
        domain: &str,
        datatype_def: serde_json::Value,
    ) -> HsdsResult<serde_json::Value> {
        let mut req = self.client.request(Method::POST, "/datatypes").await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&datatype_def);

        self.client.execute(req).await
    }

    /// Get information about a committed Datatype
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `datatype_id` - UUID of the datatype
    pub async fn get_datatype(
        &self,
        domain: &str,
        datatype_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datatypes/{}", datatype_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Delete a committed Datatype
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `datatype_id` - UUID of the datatype
    pub async fn delete_datatype(
        &self,
        domain: &str,
        datatype_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datatypes/{}", datatype_id);
        let mut req = self.client.request(Method::DELETE, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }
}
