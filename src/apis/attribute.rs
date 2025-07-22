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
    pub async fn set_attribute_raw(
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

    /// Helper function to infer HDF5 type from a JSON value
    fn infer_type_from_value(value: &serde_json::Value) -> serde_json::Value {
        use serde_json::json;
        
        match value {
            serde_json::Value::String(_) => json!({
                "class": "H5T_STRING",
                "charSet": "H5T_CSET_UTF8",
                "length": "H5T_VARIABLE"
            }),
            serde_json::Value::Number(n) => {
                if n.is_i64() {
                    json!({
                        "class": "H5T_INTEGER",
                        "base": "H5T_STD_I64LE"
                    })
                } else if n.is_u64() {
                    json!({
                        "class": "H5T_INTEGER", 
                        "base": "H5T_STD_U64LE"
                    })
                } else {
                    json!({
                        "class": "H5T_FLOAT",
                        "base": "H5T_IEEE_F64LE"
                    })
                }
            },
            serde_json::Value::Bool(_) => json!({
                "class": "H5T_INTEGER",
                "base": "H5T_STD_U8LE"
            }),
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    // Default to string for empty arrays
                    json!({
                        "class": "H5T_STRING",
                        "charSet": "H5T_CSET_UTF8",
                        "length": "H5T_VARIABLE"
                    })
                } else {
                    // Infer type from first element
                    Self::infer_type_from_value(&arr[0])
                }
            },
            _ => json!({
                "class": "H5T_STRING",
                "charSet": "H5T_CSET_UTF8", 
                "length": "H5T_VARIABLE"
            })
        }
    }

    /// Helper function to infer shape from a JSON array value
    fn infer_shape_from_value(value: &serde_json::Value) -> Option<Vec<u64>> {
        match value {
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    return Some(vec![0]);
                }
                
                let mut shape = vec![arr.len() as u64];
                
                // Check if this is a multi-dimensional array
                if let serde_json::Value::Array(inner) = &arr[0] {
                    // For now, handle 2D arrays - could be extended for N-D
                    shape.push(inner.len() as u64);
                }
                
                Some(shape)
            },
            _ => None
        }
    }

    /// Convenience method to create an attribute with automatic type inference
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `collection` - Object collection type
    /// * `obj_uuid` - UUID of the object
    /// * `attr_name` - Name of the attribute
    /// * `value` - The attribute value (type will be inferred)
    pub async fn set_attribute_auto<T>(
        &self,
        domain: &str,
        collection: &str,
        obj_uuid: &str,
        attr_name: &str,
        value: T,
    ) -> HsdsResult<serde_json::Value>
    where
        T: serde::Serialize,
    {
        let json_value = serde_json::to_value(value).map_err(|e| {
            crate::error::HsdsError::InvalidParameter(format!("Failed to serialize value: {}", e))
        })?;
        
        let inferred_type = Self::infer_type_from_value(&json_value);
        let inferred_shape = Self::infer_shape_from_value(&json_value);
        
        let mut attr_data = serde_json::json!({
            "type": inferred_type,
            "value": json_value
        });
        
        // Add shape if it's an array
        if let Some(shape) = inferred_shape {
            attr_data["shape"] = serde_json::Value::Array(
                shape.into_iter().map(|dim| serde_json::Value::from(dim)).collect()
            );
        }
        
        self.set_attribute_raw(domain, collection, obj_uuid, attr_name, attr_data).await
    }

    /// Set an attribute on any object (group, dataset, or datatype) with automatic type inference
    /// The object type is automatically determined from the ID prefix:
    /// - g-* → group
    /// - d-* → dataset  
    /// - t-* → datatype
    pub async fn set_attribute<T>(
        &self,
        domain: &str,
        object_id: &str,
        attr_name: &str,
        value: T,
    ) -> HsdsResult<serde_json::Value>
    where
        T: serde::Serialize,
    {
        let collection = match object_id.get(0..2) {
            Some("g-") => "groups",
            Some("d-") => "datasets",
            Some("t-") => "datatypes",
            _ => return Err(crate::error::HsdsError::InvalidParameter(
                format!("Unknown object ID format: '{}'. Expected ID to start with 'g-', 'd-', or 't-'", object_id)
            )),
        };
        
        self.set_attribute_auto(domain, collection, object_id, attr_name, value).await
    }
}
