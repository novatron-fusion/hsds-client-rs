use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Access Control List for a single user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acl {
    pub create: Option<bool>,
    pub update: Option<bool>,
    pub delete: Option<bool>,
    #[serde(rename = "updateACL")]
    pub update_acl: Option<bool>,
    pub read: Option<bool>,
    #[serde(rename = "readACL")]
    pub read_acl: Option<bool>,
}

/// Access Control Lists for users
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acls {
    #[serde(flatten)]
    pub users: HashMap<String, Acl>,
}

/// Domain information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub root: Option<String>,
    pub owner: Option<String>,
    pub class: Option<DomainClass>,
    pub created: Option<f64>,
    #[serde(rename = "lastModified")]
    pub last_modified: Option<f64>,
    pub hrefs: Option<Vec<Href>>,
    pub acls: Option<Acls>,
}

/// Domain class enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DomainClass {
    Domain,
    Folder,
}

/// Reference link
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Href {
    pub href: String,
    pub rel: String,
}

/// Group information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Group {
    pub id: String,
    pub root: Option<String>,
    pub domain: Option<String>,
    pub alias: Option<Vec<String>>,
    pub created: Option<f64>,
    #[serde(rename = "lastModified")]
    pub last_modified: Option<f64>,
    #[serde(rename = "attributeCount")]
    pub attribute_count: Option<u32>,
    #[serde(rename = "linkCount")]
    pub link_count: Option<u32>,
    pub hrefs: Option<Vec<Href>>,
}

/// Link information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Link {
    pub id: Option<String>,
    pub created: Option<f64>,
    pub class: Option<LinkClass>,
    pub title: String,
    pub target: Option<String>,
    pub href: Option<String>,
    pub collection: Option<String>,
    pub h5path: Option<String>,
    pub h5domain: Option<String>,
}

/// Link class enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LinkClass {
    #[serde(rename = "H5L_TYPE_HARD")]
    Hard,
    #[serde(rename = "H5L_TYPE_SOFT")]
    Soft,
    #[serde(rename = "H5L_TYPE_EXTERNAL")]
    External,
}

/// Links collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Links {
    pub links: Vec<Link>,
    pub hrefs: Option<Vec<Href>>,
}

/// Dataset information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub root: Option<String>,
    pub domain: Option<String>,
    pub created: Option<f64>,
    #[serde(rename = "lastModified")]
    pub last_modified: Option<f64>,
    #[serde(rename = "attributeCount")]
    pub attribute_count: Option<u32>,
    #[serde(rename = "type")]
    pub data_type: Option<DataType>,
    pub shape: Option<Shape>,
    pub layout: Option<serde_json::Value>,
    #[serde(rename = "creationProperties")]
    pub creation_properties: Option<serde_json::Value>,
    pub hrefs: Option<Vec<Href>>,
}

/// Dataset collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datasets {
    pub datasets: Vec<String>,
    pub hrefs: Option<Vec<Href>>,
}

/// Data type information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataType {
    pub class: String,
    pub base: Option<String>,
    pub fields: Option<serde_json::Value>,
}

/// Shape information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shape {
    pub class: String,
    pub dims: Option<Vec<u64>>,
    pub maxdims: Option<Vec<u64>>,
}

/// Dataset value request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetValueRequest {
    pub start: Option<Vec<u64>>,
    pub stop: Option<Vec<u64>>,
    pub step: Option<Vec<u64>>,
    pub points: Option<Vec<Vec<u64>>>,
    pub value: Option<serde_json::Value>,
    pub value_base64: Option<String>,
}

/// Dataset shape update request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShapeUpdateRequest {
    pub shape: Vec<u64>,
}

/// Domain creation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainCreateRequest {
    pub folder: Option<u8>, // 0 or 1
}

/// Group creation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupCreateRequest {
    pub link: Option<LinkRequest>,
}

/// Dataset creation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetCreateRequest {
    #[serde(rename = "type")]
    pub data_type: DataTypeSpec,
    pub shape: Option<ShapeSpec>,
    pub maxdims: Option<Vec<u64>>,
    #[serde(rename = "creationProperties")]
    pub creation_properties: Option<serde_json::Value>,
    pub link: Option<LinkRequest>,
}

/// Data type specification (can be string or object)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DataTypeSpec {
    Predefined(String),
    Custom(DataType),
}

/// Shape specification (can be array or null)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ShapeSpec {
    Dimensions(Vec<u64>),
    Null(String), // "H5S_NULL"
}

/// Link creation request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkRequest {
    pub id: String,
    pub name: String,
}

/// Link creation body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkCreateRequest {
    pub id: Option<String>,
    pub h5path: Option<String>,
    pub h5domain: Option<String>,
}

/// Generic API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    #[serde(flatten)]
    pub data: T,
}

/// Error response from API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: Option<String>,
    pub message: Option<String>,
    pub code: Option<u16>,
}
