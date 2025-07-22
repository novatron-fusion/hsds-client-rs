use crate::{
    client::HsdsClient,
    error::HsdsResult,
};
use reqwest::Method;

/// Attribute API operations
pub struct AttributeApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> AttributeApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// List all Attributes attached to an object
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `collection` - Object collection type ("groups", "datasets", "datatypes")
    /// * `obj_uuid` - UUID of the object
    pub async fn list_attributes(
        &self,
        domain: &str,
        collection: &str,
        obj_uuid: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/{}/{}/attributes", collection, obj_uuid);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Create or update an Attribute
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `collection` - Object collection type
    /// * `obj_uuid` - UUID of the object
    /// * `attr_name` - Name of the attribute
    /// * `attr_data` - Attribute data and type definition
    pub async fn put_attribute(
        &self,
        domain: &str,
        collection: &str,
        obj_uuid: &str,
        attr_name: &str,
        attr_data: serde_json::Value,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/{}/{}/attributes/{}", collection, obj_uuid, 
                          urlencoding::encode(attr_name));
        let mut req = self.client.request(Method::PUT, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&attr_data);

        self.client.execute(req).await
    }

    /// Get an Attribute
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `collection` - Object collection type
    /// * `obj_uuid` - UUID of the object
    /// * `attr_name` - Name of the attribute
    pub async fn get_attribute(
        &self,
        domain: &str,
        collection: &str,
        obj_uuid: &str,
        attr_name: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/{}/{}/attributes/{}", collection, obj_uuid, 
                          urlencoding::encode(attr_name));
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Delete an Attribute
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `collection` - Object collection type
    /// * `obj_uuid` - UUID of the object
    /// * `attr_name` - Name of the attribute
    pub async fn delete_attribute(
        &self,
        domain: &str,
        collection: &str,
        obj_uuid: &str,
        attr_name: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/{}/{}/attributes/{}", collection, obj_uuid, 
                          urlencoding::encode(attr_name));
        let mut req = self.client.request(Method::DELETE, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Convenience methods for specific object types

    /// List Group attributes
    pub async fn list_group_attributes(
        &self,
        domain: &str,
        group_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        self.list_attributes(domain, "groups", group_id).await
    }

    /// List Dataset attributes
    pub async fn list_dataset_attributes(
        &self,
        domain: &str,
        dataset_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        self.list_attributes(domain, "datasets", dataset_id).await
    }

    /// List Datatype attributes
    pub async fn list_datatype_attributes(
        &self,
        domain: &str,
        datatype_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        self.list_attributes(domain, "datatypes", datatype_id).await
    }

    /// Create Group attribute
    pub async fn put_group_attribute(
        &self,
        domain: &str,
        group_id: &str,
        attr_name: &str,
        attr_data: serde_json::Value,
    ) -> HsdsResult<serde_json::Value> {
        self.put_attribute(domain, "groups", group_id, attr_name, attr_data).await
    }

    /// Create Dataset attribute
    pub async fn put_dataset_attribute(
        &self,
        domain: &str,
        dataset_id: &str,
        attr_name: &str,
        attr_data: serde_json::Value,
    ) -> HsdsResult<serde_json::Value> {
        self.put_attribute(domain, "datasets", dataset_id, attr_name, attr_data).await
    }

    /// Create Datatype attribute
    pub async fn put_datatype_attribute(
        &self,
        domain: &str,
        datatype_id: &str,
        attr_name: &str,
        attr_data: serde_json::Value,
    ) -> HsdsResult<serde_json::Value> {
        self.put_attribute(domain, "datatypes", datatype_id, attr_name, attr_data).await
    }
}
