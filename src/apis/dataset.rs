use crate::{
    client::HsdsClient,
    error::HsdsResult,
    models::{Dataset, Datasets, DatasetCreateRequest, DatasetValueRequest, ShapeUpdateRequest},
};
use reqwest::Method;
use bytes::Bytes;

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

        self.client.execute(req).await
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
