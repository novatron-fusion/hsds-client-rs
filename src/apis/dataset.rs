use crate::{
    client::HsdsClient,
    error::HsdsResult,
    models::{Dataset, Datasets, DatasetCreateRequest, DatasetValueRequest, ShapeUpdateRequest, 
             StringDataType, DataTypeSpec, ShapeSpec, StringCharSet, StringPadding, StringLength, LinkRequest},
};
use reqwest::Method;
use bytes::Bytes;
use log::debug;

/// Dataset API operations  
pub struct DatasetApi<'a> {
    client: &'a HsdsClient,
}

impl<'a> DatasetApi<'a> {
    pub fn new(client: &'a HsdsClient) -> Self {
        Self { client }
    }

    /// Create a new Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `request` - Dataset creation parameters
    pub async fn create_dataset(
        &self,
        domain: &str,
        request: DatasetCreateRequest,
    ) -> HsdsResult<Dataset> {

        let mut req = self.client.request(Method::POST, "/datasets").await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&request);

        debug!("Creating dataset in domain: {}", domain);
        debug!("DatasetCreateRequest: {:?}", request);

        let result = self.client.execute(req).await;

        if let Err(ref err) = result {
            debug!("Error details: {:?}", err);
        }
        return result
    }

    /// List all Datasets in Domain
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    pub async fn list_datasets(&self, domain: &str) -> HsdsResult<Datasets> {
        let mut req = self.client.request(Method::GET, "/datasets").await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Get information about a Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    pub async fn get_dataset(
        &self,
        domain: &str,
        dataset_id: &str,
    ) -> HsdsResult<Dataset> {
        let path = format!("/datasets/{}", dataset_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Delete a Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    pub async fn delete_dataset(
        &self,
        domain: &str,
        dataset_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}", dataset_id);
        let mut req = self.client.request(Method::DELETE, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Get Dataset shape information
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    pub async fn get_dataset_shape(
        &self,
        domain: &str,
        dataset_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/shape", dataset_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Update Dataset shape (resize)
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    /// * `request` - New shape dimensions
    pub async fn update_dataset_shape(
        &self,
        domain: &str,
        dataset_id: &str,
        request: ShapeUpdateRequest,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/shape", dataset_id);
        let mut req = self.client.request(Method::PUT, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&request);

        self.client.execute(req).await
    }

    /// Get Dataset type information
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    pub async fn get_dataset_type(
        &self,
        domain: &str,
        dataset_id: &str,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/type", dataset_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        self.client.execute(req).await
    }

    /// Write values to Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    /// * `request` - Data to write and selection parameters
    pub async fn write_dataset_values(
        &self,
        domain: &str,
        dataset_id: &str,
        request: DatasetValueRequest,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/value", dataset_id);
        let mut req = self.client.request(Method::PUT, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&request);

        self.client.execute(req).await
    }

    /// Read values from Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    /// * `select` - Optional selection string (e.g., "[3:9,0:5:2]")
    /// * `query` - Optional query condition
    /// * `limit` - Optional limit for query results
    pub async fn read_dataset_values(
        &self,
        domain: &str,
        dataset_id: &str,
        select: Option<&str>,
        query: Option<&str>,
        limit: Option<u32>,
    ) -> HsdsResult<Bytes> {
        let path = format!("/datasets/{}/value", dataset_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        if let Some(selection) = select {
            req = HsdsClient::with_selection(req, selection);
        }

        if let Some(q) = query {
            req = HsdsClient::with_query(req, q, limit);
        }

        self.client.execute_bytes(req).await
    }

    /// Read values from Dataset as JSON
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    /// * `select` - Optional selection string
    /// * `query` - Optional query condition
    /// * `limit` - Optional limit for query results
    pub async fn read_dataset_values_json(
        &self,
        domain: &str,
        dataset_id: &str,
        select: Option<&str>,
        query: Option<&str>,
        limit: Option<u32>,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/value", dataset_id);
        let mut req = self.client.request(Method::GET, &path).await?;
        req = HsdsClient::with_domain(req, domain);

        if let Some(selection) = select {
            req = HsdsClient::with_selection(req, selection);
        }

        if let Some(q) = query {
            req = HsdsClient::with_query(req, q, limit);
        }

        // Set Accept header for JSON response
        req = req.header("Accept", "application/json");

        self.client.execute(req).await
    }

    /// Read specific data points from Dataset
    /// 
    /// # Arguments
    /// * `domain` - Domain path
    /// * `dataset_id` - UUID of the dataset
    /// * `points` - Array of coordinates in the dataset
    pub async fn read_dataset_points(
        &self,
        domain: &str,
        dataset_id: &str,
        points: Vec<Vec<u64>>,
    ) -> HsdsResult<serde_json::Value> {
        let path = format!("/datasets/{}/value", dataset_id);
        let mut req = self.client.request(Method::POST, &path).await?;
        req = HsdsClient::with_domain(req, domain);
        req = req.json(&points);

        self.client.execute(req).await
    }
}

impl DatasetCreateRequest {
    /// Create a dataset from an HSDS data type string
    /// This method determines the appropriate DataTypeSpec based on the HSDS type
    pub fn from_hsds_type(
        hsds_type: &str,
        dimensions: Vec<u64>,
    ) -> Self {
        let data_type = match hsds_type {
            // String types - use structured string type
            "H5T_STRING" => DataTypeSpec::String(StringDataType::variable_ascii()),
            
            // Numeric types - use predefined types
            "H5T_STD_U8LE" | "H5T_STD_I8LE" | "H5T_STD_U16LE" | "H5T_STD_I16LE" |
            "H5T_STD_U32LE" | "H5T_STD_I32LE" | "H5T_STD_I64LE" |
            "H5T_IEEE_F32LE" | "H5T_IEEE_F64LE" => DataTypeSpec::Predefined(hsds_type.to_string()),
            
            // Default to predefined for any other type
            _ => DataTypeSpec::Predefined(hsds_type.to_string()),
        };

        Self {
            data_type,
            shape: Some(ShapeSpec::Dimensions(dimensions)),
            maxdims: None,
            creation_properties: None,
            link: None,
        }
    }

    /// Create a dataset with linking to a parent group
    pub fn from_hsds_type_with_link(
        hsds_type: &str,
        dimensions: Vec<u64>,
        parent_group_id: &str,
        dataset_name: &str,
    ) -> Self {
        let mut request = Self::from_hsds_type(hsds_type, dimensions);
        request.link = Some(LinkRequest {
            id: parent_group_id.to_string(),
            name: dataset_name.to_string(),
        });
        request
    }
}

impl StringDataType {
    /// Create a new variable-length UTF-8 string type
    pub fn variable_utf8() -> Self {
        Self {
            class: "H5T_STRING".to_string(),
            char_set: StringCharSet::Utf8,
            str_pad: StringPadding::NullPad,
            length: StringLength::Variable("H5T_VARIABLE".to_string()),
        }
    }

    /// Create a new fixed-length UTF-8 string type
    pub fn fixed_utf8(length: u32) -> Self {
        Self {
            class: "H5T_STRING".to_string(),
            char_set: StringCharSet::Utf8,
            str_pad: StringPadding::NullPad,
            length: StringLength::Fixed(length),
        }
    }

    /// Create a new variable-length ASCII string type
    pub fn variable_ascii() -> Self {
        Self {
            class: "H5T_STRING".to_string(),
            char_set: StringCharSet::Ascii,
            str_pad: StringPadding::NullPad,
            length: StringLength::Variable("H5T_VARIABLE".to_string()),
        }
    }

    /// Create a new fixed-length ASCII string type
    pub fn fixed_ascii(length: u32) -> Self {
        Self {
            class: "H5T_STRING".to_string(),
            char_set: StringCharSet::Ascii,
            str_pad: StringPadding::NullPad,
            length: StringLength::Fixed(length),
        }
    }

    /// Create a string type with custom parameters
    pub fn custom(
        char_set: StringCharSet,
        str_pad: StringPadding,
        length: StringLength,
    ) -> Self {
        Self {
            class: "H5T_STRING".to_string(),
            char_set,
            str_pad,
            length,
        }
    }
}
