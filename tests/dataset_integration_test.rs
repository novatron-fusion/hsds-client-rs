use hsds_client::{HsdsClient, BasicAuth, HsdsResult};
use hsds_client::models::{DatasetCreateRequest, DataTypeSpec, ShapeSpec, LinkRequest, DatasetValueRequest, ShapeUpdateRequest};
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::json;
use base64::{Engine as _, engine::general_purpose};

/// Helper to create a test client
fn create_test_client() -> HsdsResult<HsdsClient> {
    HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )
}

/// Helper to create a unique test domain name
fn create_test_domain_name() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/home/admin/test_dataset_{}.h5", timestamp)
}

/// Helper to create a simple dataset creation request
fn create_simple_dataset_request(root_group_id: String) -> DatasetCreateRequest {
    DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_I32LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![10, 10])),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: root_group_id,
            name: "test_dataset".to_string(),
        }),
    }
}

/// Helper to create a 1D dataset creation request
fn create_1d_dataset_request(root_group_id: String) -> DatasetCreateRequest {
    DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_IEEE_F64LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![5])),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: root_group_id,
            name: "test_1d_dataset".to_string(),
        }),
    }
}

/// Helper to create an unlimited dimension dataset request
fn create_unlimited_dataset_request(root_group_id: String) -> DatasetCreateRequest {
    DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_I32LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![0])), // Start with 0 size
        maxdims: Some(vec![0]), // 0 means unlimited
        creation_properties: None,
        link: Some(LinkRequest {
            id: root_group_id,
            name: "test_unlimited_dataset".to_string(),
        }),
    }
}

/// Test creating a simple dataset
#[tokio::test]
async fn test_create_simple_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a simple 2D dataset
    let dataset_request = create_simple_dataset_request(root_group_id);
    let result = client.datasets().create_dataset(&domain_path, dataset_request).await
        .expect("Failed to create dataset");
    
    // Verify the response contains expected fields
    assert!(!result.id.is_empty(), "Dataset should have an ID");
    assert!(result.created.is_some(), "Dataset should have creation time");
    assert!(result.shape.is_some(), "Dataset should have shape information");
    
    // Verify the shape
    if let Some(shape) = &result.shape {
        assert_eq!(shape.class, "H5S_SIMPLE", "Shape class should be H5S_SIMPLE");
        if let Some(dims) = &shape.dims {
            assert_eq!(dims, &vec![10, 10], "Dimensions should match [10, 10]");
        }
    }
    
    println!("✓ Created simple dataset: {}", result.id);
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &result.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test creating a 1D dataset
#[tokio::test]
async fn test_create_1d_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a 1D dataset
    let dataset_request = create_1d_dataset_request(root_group_id);
    let result = client.datasets().create_dataset(&domain_path, dataset_request).await
        .expect("Failed to create 1D dataset");
    
    // Verify the dataset properties
    assert!(!result.id.is_empty(), "Dataset should have an ID");
    if let Some(shape) = &result.shape {
        if let Some(dims) = &shape.dims {
            assert_eq!(dims, &vec![5], "Dimensions should match [5]");
        }
    }
    
    println!("✓ Created 1D dataset: {}", result.id);
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &result.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test creating a dataset with unlimited dimensions
#[tokio::test]
async fn test_create_unlimited_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create an unlimited dimension dataset
    let dataset_request = create_unlimited_dataset_request(root_group_id);
    let result = client.datasets().create_dataset(&domain_path, dataset_request).await
        .expect("Failed to create unlimited dataset");
    
    // Verify the dataset has unlimited maxdims
    assert!(!result.id.is_empty(), "Dataset should have an ID");
    if let Some(shape) = &result.shape {
        if let Some(maxdims) = &shape.maxdims {
            assert_eq!(maxdims, &vec![0], "Maxdims should be [0] for unlimited");
        }
    }
    
    println!("✓ Created unlimited dataset: {}", result.id);
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &result.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test listing datasets in a domain
#[tokio::test]
async fn test_list_datasets() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a couple of datasets
    let dataset1 = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id.clone())).await
        .expect("Failed to create first dataset");
    let dataset2 = client.datasets().create_dataset(&domain_path, create_1d_dataset_request(root_group_id)).await
        .expect("Failed to create second dataset");
    
    // List all datasets
    let datasets_list = client.datasets().list_datasets(&domain_path).await
        .expect("Failed to list datasets");
    
    // Verify we have at least two datasets
    assert!(datasets_list.datasets.len() >= 2, "Should have at least 2 datasets");
    
    // Verify our dataset IDs are in the list
    assert!(datasets_list.datasets.contains(&dataset1.id), "First dataset should be in the list");
    assert!(datasets_list.datasets.contains(&dataset2.id), "Second dataset should be in the list");
    
    println!("✓ Listed {} datasets", datasets_list.datasets.len());
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset1.id).await.ok();
    client.datasets().delete_dataset(&domain_path, &dataset2.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting dataset information
#[tokio::test]
async fn test_get_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a dataset
    let created_dataset = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Get the dataset information
    let retrieved_dataset = client.datasets().get_dataset(&domain_path, &created_dataset.id).await
        .expect("Failed to get dataset");
    
    // Verify the retrieved dataset matches the created one
    assert_eq!(retrieved_dataset.id, created_dataset.id, "Dataset IDs should match");
    
    println!("✓ Retrieved dataset: {}", retrieved_dataset.id);
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &created_dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting nonexistent dataset (should fail)
#[tokio::test]
async fn test_get_nonexistent_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Try to get a nonexistent dataset
    let fake_dataset_id = "d-nonexistent-dataset-id";
    let result = client.datasets().get_dataset(&domain_path, fake_dataset_id).await;
    
    // Should fail
    assert!(result.is_err(), "Getting nonexistent dataset should fail");
    
    println!("✓ Correctly failed to get nonexistent dataset");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting a dataset
#[tokio::test]
async fn test_delete_dataset() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a dataset
    let dataset = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Verify the dataset exists
    let _retrieved = client.datasets().get_dataset(&domain_path, &dataset.id).await
        .expect("Failed to get dataset before deletion");
    
    // Delete the dataset
    let _result = client.datasets().delete_dataset(&domain_path, &dataset.id).await
        .expect("Failed to delete dataset");
    
    // Verify the dataset no longer exists
    let get_result = client.datasets().get_dataset(&domain_path, &dataset.id).await;
    assert!(get_result.is_err(), "Dataset should not exist after deletion");
    
    println!("✓ Successfully deleted dataset");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting dataset shape information
#[tokio::test]
async fn test_get_dataset_shape() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a dataset
    let dataset = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Get the dataset shape
    let shape_result = client.datasets().get_dataset_shape(&domain_path, &dataset.id).await
        .expect("Failed to get dataset shape");
    
    // Verify shape information - the response has a nested "shape" field
    let shape = shape_result.get("shape").expect("Response should have shape field");
    assert_eq!(shape.get("class").unwrap().as_str().unwrap(), "H5S_SIMPLE", "Shape class should be H5S_SIMPLE");
    
    if let Some(dims) = shape.get("dims") {
        if let Some(dims_array) = dims.as_array() {
            assert_eq!(dims_array.len(), 2, "Should have 2 dimensions");
            assert_eq!(dims_array[0].as_u64(), Some(10), "First dimension should be 10");
            assert_eq!(dims_array[1].as_u64(), Some(10), "Second dimension should be 10");
        }
    }
    
    println!("✓ Retrieved dataset shape information");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test updating dataset shape (resize)
#[tokio::test]
async fn test_update_dataset_shape() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create an unlimited dimension dataset
    let dataset = client.datasets().create_dataset(&domain_path, create_unlimited_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Update the shape to extend it
    let shape_update = ShapeUpdateRequest {
        shape: vec![10], // Extend from 0 to 10 elements
    };
    
    let _result = client.datasets().update_dataset_shape(&domain_path, &dataset.id, shape_update).await
        .expect("Failed to update dataset shape");
    
    // Verify the shape was updated
    let updated_shape = client.datasets().get_dataset_shape(&domain_path, &dataset.id).await
        .expect("Failed to get updated shape");
    
    if let Some(dims) = updated_shape.get("dims") {
        if let Some(dims_array) = dims.as_array() {
            assert_eq!(dims_array.len(), 1, "Should have 1 dimension");
            assert_eq!(dims_array[0].as_u64(), Some(10), "Dimension should be 10");
        }
    }
    
    println!("✓ Successfully updated dataset shape");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting dataset type information
#[tokio::test]
async fn test_get_dataset_type() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a dataset
    let dataset = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Get the dataset type
    let type_result = client.datasets().get_dataset_type(&domain_path, &dataset.id).await
        .expect("Failed to get dataset type");
    
    // Verify type information - the response has a nested "type" field
    let data_type = type_result.get("type").expect("Response should have type field");
    assert_eq!(data_type.get("class").unwrap().as_str().unwrap(), "H5T_INTEGER", "Type class should be H5T_INTEGER");
    assert_eq!(data_type.get("base").unwrap().as_str().unwrap(), "H5T_STD_I32LE", "Type base should be H5T_STD_I32LE");
    
    println!("✓ Retrieved dataset type information");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test writing and reading dataset values (JSON format)
#[tokio::test]
async fn test_write_read_dataset_values_json() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a 1D dataset for easier testing
    let dataset = client.datasets().create_dataset(&domain_path, create_1d_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Write some values
    let values = json!([1.1, 2.2, 3.3, 4.4, 5.5]);
    let value_request = DatasetValueRequest {
        start: None,
        stop: None,
        step: None,
        points: None,
        value: Some(values.clone()),
        value_base64: None,
    };
    
    let _write_result = client.datasets().write_dataset_values(&domain_path, &dataset.id, value_request).await
        .expect("Failed to write dataset values");
    
    // Read the values back as JSON
    let read_result = client.datasets().read_dataset_values_json(&domain_path, &dataset.id, None, None, None).await
        .expect("Failed to read dataset values");
    
    // Verify the values match
    if let Some(read_values) = read_result.get("value") {
        assert_eq!(read_values, &values, "Read values should match written values");
    }
    
    println!("✓ Successfully wrote and read dataset values");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test reading dataset with selection
#[tokio::test]
async fn test_read_dataset_with_selection() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a 1D dataset
    let dataset = client.datasets().create_dataset(&domain_path, create_1d_dataset_request(root_group_id)).await
        .expect("Failed to create dataset");
    
    // Write some values
    let values = json!([10.0, 20.0, 30.0, 40.0, 50.0]);
    let value_request = DatasetValueRequest {
        start: None,
        stop: None,
        step: None,
        points: None,
        value: Some(values),
        value_base64: None,
    };
    
    let _write_result = client.datasets().write_dataset_values(&domain_path, &dataset.id, value_request).await
        .expect("Failed to write dataset values");
    
    // Read with selection (first 3 elements)
    let selection = "[0:3]";
    let read_result = client.datasets().read_dataset_values_json(&domain_path, &dataset.id, Some(selection), None, None).await
        .expect("Failed to read dataset values with selection");
    
    // Verify we got 3 elements
    if let Some(read_values) = read_result.get("value") {
        if let Some(array) = read_values.as_array() {
            assert_eq!(array.len(), 3, "Should have read 3 elements");
            assert_eq!(array[0].as_f64(), Some(10.0), "First element should be 10.0");
            assert_eq!(array[1].as_f64(), Some(20.0), "Second element should be 20.0");
            assert_eq!(array[2].as_f64(), Some(30.0), "Third element should be 30.0");
        }
    }
    
    println!("✓ Successfully read dataset with selection");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test writing binary data to dataset
#[tokio::test]
async fn test_write_binary_data() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a dataset suitable for binary data (uint8)
    let dataset_request = DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_U8LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![10])),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: root_group_id,
            name: "binary_dataset".to_string(),
        }),
    };
    
    let dataset = client.datasets().create_dataset(&domain_path, dataset_request).await
        .expect("Failed to create binary dataset");
    
    // Create some binary data
    let binary_data = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    let encoded_data = general_purpose::STANDARD.encode(&binary_data);
    
    // Write binary data using the value_base64 field
    let value_request = DatasetValueRequest {
        start: None,
        stop: None,
        step: None,
        points: None,
        value: None,
        value_base64: Some(encoded_data),
    };
    
    let _write_result = client.datasets().write_dataset_values(&domain_path, &dataset.id, value_request).await
        .expect("Failed to write binary data");
    
    // Read the binary data back
    let read_result = client.datasets().read_dataset_values(&domain_path, &dataset.id, None, None, None).await
        .expect("Failed to read binary data");
    
    // Verify we can read it back (should be raw bytes)
    assert!(!read_result.is_empty(), "Should have received data");
    
    println!("✓ Successfully wrote and read binary data");
    
    // Clean up
    client.datasets().delete_dataset(&domain_path, &dataset.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test multiple dataset operations in sequence
#[tokio::test]
async fn test_multiple_dataset_operations() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create multiple datasets
    let dataset1 = client.datasets().create_dataset(&domain_path, create_simple_dataset_request(root_group_id.clone())).await
        .expect("Failed to create first dataset");
    let dataset2 = client.datasets().create_dataset(&domain_path, create_1d_dataset_request(root_group_id.clone())).await
        .expect("Failed to create second dataset");
    let dataset3 = client.datasets().create_dataset(&domain_path, create_unlimited_dataset_request(root_group_id)).await
        .expect("Failed to create third dataset");
    
    // List datasets to verify all are there
    let datasets_list = client.datasets().list_datasets(&domain_path).await
        .expect("Failed to list datasets");
    
    assert!(datasets_list.datasets.len() >= 3, "Should have at least 3 datasets");
    
    // Get each dataset to verify they exist
    let _retrieved1 = client.datasets().get_dataset(&domain_path, &dataset1.id).await
        .expect("Failed to get first dataset");
    let _retrieved2 = client.datasets().get_dataset(&domain_path, &dataset2.id).await
        .expect("Failed to get second dataset");
    let _retrieved3 = client.datasets().get_dataset(&domain_path, &dataset3.id).await
        .expect("Failed to get third dataset");
    
    println!("✓ Successfully performed multiple dataset operations");
    
    // Clean up all datasets
    client.datasets().delete_dataset(&domain_path, &dataset1.id).await.ok();
    client.datasets().delete_dataset(&domain_path, &dataset2.id).await.ok();
    client.datasets().delete_dataset(&domain_path, &dataset3.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}
