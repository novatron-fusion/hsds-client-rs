use hsds_client::{HsdsClient, BasicAuth, HsdsResult};
use std::time::{SystemTime, UNIX_EPOCH};
use serde_json::{json, Value};

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
    format!("/home/admin/test_datatype_{}.h5", timestamp)
}

/// Helper to create a simple integer datatype definition
fn create_integer_datatype() -> Value {
    json!({
        "type": "H5T_STD_I32LE"
    })
}

/// Helper to create a compound datatype definition
fn create_compound_datatype() -> Value {
    json!({
        "type": {
            "class": "H5T_COMPOUND",
            "fields": [
                {
                    "name": "x",
                    "type": "H5T_IEEE_F64LE"
                },
                {
                    "name": "y", 
                    "type": "H5T_IEEE_F64LE"
                }
            ]
        }
    })
}

/// Helper to create a float datatype definition
fn create_float_datatype() -> Value {
    json!({
        "type": "H5T_IEEE_F64LE"
    })
}

/// Test committing a simple integer datatype
#[tokio::test]
async fn test_commit_integer_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Commit an integer datatype
    let datatype_def = create_integer_datatype();
    let result = client.datatypes().commit_datatype(&domain_path, datatype_def).await
        .expect("Failed to commit datatype");
    
    // Verify the response contains an id
    assert!(result.get("id").is_some(), "Response should contain datatype id");
    let datatype_id = result.get("id").unwrap().as_str().unwrap();
    assert!(!datatype_id.is_empty(), "Datatype ID should not be empty");
    
    // Clean up
    client.datatypes().delete_datatype(&domain_path, datatype_id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test committing a compound datatype
#[tokio::test]
async fn test_commit_compound_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Commit a compound datatype
    let datatype_def = create_compound_datatype();
    let result = client.datatypes().commit_datatype(&domain_path, datatype_def).await
        .expect("Failed to commit compound datatype");
    
    // Verify the response
    assert!(result.get("id").is_some(), "Response should contain datatype id");
    let datatype_id = result.get("id").unwrap().as_str().unwrap();
    
    // Verify the datatype class is compound
    if let Some(datatype_info) = result.get("type") {
        assert_eq!(
            datatype_info.get("class").unwrap().as_str().unwrap(),
            "H5T_COMPOUND",
            "Datatype class should be H5T_COMPOUND"
        );
    }
    
    // Clean up
    client.datatypes().delete_datatype(&domain_path, datatype_id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test committing a float datatype
#[tokio::test]
async fn test_commit_float_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Commit a float datatype
    let datatype_def = create_float_datatype();
    let result = client.datatypes().commit_datatype(&domain_path, datatype_def).await
        .expect("Failed to commit float datatype");
    
    // Verify the response
    assert!(result.get("id").is_some(), "Response should contain datatype id");
    let datatype_id = result.get("id").unwrap().as_str().unwrap();
    
    // Clean up
    client.datatypes().delete_datatype(&domain_path, datatype_id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting datatype information
#[tokio::test]
async fn test_get_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // First commit a datatype
    let datatype_def = create_integer_datatype();
    let commit_result = client.datatypes().commit_datatype(&domain_path, datatype_def).await
        .expect("Failed to commit datatype");
    
    let datatype_id = commit_result.get("id").unwrap().as_str().unwrap();
    
    // Now get the datatype information
    let result = client.datatypes().get_datatype(&domain_path, datatype_id).await
        .expect("Failed to get datatype");
    
    // Verify the response contains expected fields
    assert!(result.get("id").is_some(), "Response should contain id");
    assert!(result.get("type").is_some(), "Response should contain type information");
    assert!(result.get("created").is_some(), "Response should contain created timestamp");
    
    // Verify the datatype class
    let datatype_info = result.get("type").unwrap();
    if datatype_info.is_string() {
        // For simple types, the type is just a string
        assert_eq!(datatype_info.as_str().unwrap(), "H5T_STD_I32LE");
    } else {
        // For complex types, check the class
        assert_eq!(
            datatype_info.get("class").unwrap().as_str().unwrap(),
            "H5T_INTEGER",
            "Datatype class should be H5T_INTEGER"
        );
    }
    
    // Clean up
    client.datatypes().delete_datatype(&domain_path, datatype_id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting non-existent datatype (should fail)
#[tokio::test]
async fn test_get_nonexistent_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Try to get a non-existent datatype
    let fake_id = "non-existent-datatype-id";
    let result = client.datatypes().get_datatype(&domain_path, fake_id).await;
    
    // This should fail
    assert!(result.is_err(), "Getting non-existent datatype should fail");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting a datatype
#[tokio::test]
async fn test_delete_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // First commit a datatype
    let datatype_def = create_integer_datatype();
    let commit_result = client.datatypes().commit_datatype(&domain_path, datatype_def).await
        .expect("Failed to commit datatype");
    
    let datatype_id = commit_result.get("id").unwrap().as_str().unwrap();
    
    // Verify the datatype exists
    let _get_result = client.datatypes().get_datatype(&domain_path, datatype_id).await
        .expect("Datatype should exist before deletion");
    
    // Delete the datatype
    let _delete_result = client.datatypes().delete_datatype(&domain_path, datatype_id).await
        .expect("Failed to delete datatype");
    
    // Verify the datatype no longer exists
    let get_after_delete = client.datatypes().get_datatype(&domain_path, datatype_id).await;
    assert!(get_after_delete.is_err(), "Datatype should not exist after deletion");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting non-existent datatype (should fail)
#[tokio::test]
async fn test_delete_nonexistent_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Try to delete a non-existent datatype
    let fake_id = "non-existent-datatype-id";
    let result = client.datatypes().delete_datatype(&domain_path, fake_id).await;
    
    // This should fail
    assert!(result.is_err(), "Deleting non-existent datatype should fail");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test datatype operations without domain (should fail)
#[tokio::test]
async fn test_datatype_operations_without_domain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let nonexistent_domain = "/home/admin/nonexistent_domain.h5";
    
    // Try to commit a datatype to a non-existent domain
    let datatype_def = create_integer_datatype();
    let commit_result = client.datatypes().commit_datatype(&nonexistent_domain, datatype_def).await;
    
    // This should fail
    assert!(commit_result.is_err(), "Committing datatype to non-existent domain should fail");
    
    // Try to get a datatype from a non-existent domain
    let get_result = client.datatypes().get_datatype(&nonexistent_domain, "some-id").await;
    assert!(get_result.is_err(), "Getting datatype from non-existent domain should fail");
    
    // Try to delete a datatype from a non-existent domain
    let delete_result = client.datatypes().delete_datatype(&nonexistent_domain, "some-id").await;
    assert!(delete_result.is_err(), "Deleting datatype from non-existent domain should fail");
}

/// Test commit datatype with invalid definition (should fail)
#[tokio::test]
async fn test_commit_invalid_datatype() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Try to commit an invalid datatype (missing required fields)
    let invalid_datatype = json!({
        "invalid_field": "invalid_value"
    });
    
    let result = client.datatypes().commit_datatype(&domain_path, invalid_datatype).await;
    
    // This should fail
    assert!(result.is_err(), "Committing invalid datatype should fail");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test multiple datatype operations in sequence
#[tokio::test]
async fn test_multiple_datatype_operations() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Commit multiple different datatypes
    let integer_def = create_integer_datatype();
    let compound_def = create_compound_datatype();
    let float_def = create_float_datatype();
    
    let int_result = client.datatypes().commit_datatype(&domain_path, integer_def).await
        .expect("Failed to commit integer datatype");
    let int_id = int_result.get("id").unwrap().as_str().unwrap();
    
    let compound_result = client.datatypes().commit_datatype(&domain_path, compound_def).await
        .expect("Failed to commit compound datatype");
    let compound_id = compound_result.get("id").unwrap().as_str().unwrap();
    
    let float_result = client.datatypes().commit_datatype(&domain_path, float_def).await
        .expect("Failed to commit float datatype");
    let float_id = float_result.get("id").unwrap().as_str().unwrap();
    
    // Verify all datatypes exist and have correct types
    let int_get = client.datatypes().get_datatype(&domain_path, int_id).await
        .expect("Failed to get integer datatype");
    // Check if type field exists and is correct format
    assert!(int_get.get("type").is_some(), "Integer datatype should have type field");
    
    let compound_get = client.datatypes().get_datatype(&domain_path, compound_id).await
        .expect("Failed to get compound datatype");
    if let Some(compound_type) = compound_get.get("type") {
        if let Some(class) = compound_type.get("class") {
            assert_eq!(class.as_str().unwrap(), "H5T_COMPOUND");
        }
    }
    
    let float_get = client.datatypes().get_datatype(&domain_path, float_id).await
        .expect("Failed to get float datatype");
    // Check if the float datatype response has the correct format
    assert!(float_get.get("type").is_some(), "Float datatype should have type field");
    
    // Clean up all datatypes
    client.datatypes().delete_datatype(&domain_path, int_id).await.ok();
    client.datatypes().delete_datatype(&domain_path, compound_id).await.ok();
    client.datatypes().delete_datatype(&domain_path, float_id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}
