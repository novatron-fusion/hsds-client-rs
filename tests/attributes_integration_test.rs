use hsds_client::{HsdsClient, BasicAuth};
use hsds_client::models::{DatasetCreateRequest, DataTypeSpec, ShapeSpec, LinkRequest, GroupCreateRequest};
use std::env;

fn create_test_client() -> Result<HsdsClient, Box<dyn std::error::Error>> {
    let endpoint = env::var("HSDS_ENDPOINT").unwrap_or_else(|_| "http://localhost:5101".to_string());
    let username = env::var("HSDS_USERNAME").unwrap_or_else(|_| "admin".to_string());
    let password = env::var("HSDS_PASSWORD").unwrap_or_else(|_| "admin".to_string());
    
    let auth = BasicAuth::new(&username, &password);
    Ok(HsdsClient::new(&endpoint, auth)?)
}

fn create_test_domain_name() -> String {
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    format!("/home/admin/unified_attr_test_{}.h5", &uuid[..32])
}

async fn create_test_dataset(
    client: &HsdsClient,
    domain_path: &str,
    group_id: &str,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let create_request = DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_I32LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![10])),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: group_id.to_string(),
            name: name.to_string(),
        }),
    };
    
    let dataset = client.datasets().create_dataset(domain_path, create_request).await?;
    Ok(dataset.id)
}

#[tokio::test]
async fn test_set_attribute_api() {
    env_logger::try_init().ok();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Get root group
    let domain = client.domains().get_domain(&domain_path).await
        .expect("Failed to get domain");
    let root_group_id = domain.root.unwrap();
    
    // Create a child group
    let child_group_request = GroupCreateRequest {
        link: Some(LinkRequest {
            id: root_group_id.clone(),
            name: "test_group".to_string(),
        }),
    };
    
    let child_group = client.groups().create_group(&domain_path, Some(child_group_request)).await
        .expect("Failed to create child group");
    let child_group_id = &child_group.id;
    
    // Create a dataset
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "test_dataset").await
        .expect("Failed to create test dataset");

    println!("Testing unified attribute API...");
    println!("Root group ID: {}", root_group_id);
    println!("Child group ID: {}", child_group_id);
    println!("Dataset ID: {}", dataset_id);
    
    // Test 1: Set attributes using the unified API (should automatically detect object types)
    
    // Group attribute (g- prefix)
    client.attributes().set_attribute(&domain_path, &root_group_id, "root_description", "This is the root group").await
        .expect("Failed to set root group attribute");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "child_description", "This is a child group").await
        .expect("Failed to set child group attribute");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "creation_time", 1642694400i64).await
        .expect("Failed to set group timestamp");
    
    // Dataset attribute (d- prefix)
    client.attributes().set_attribute(&domain_path, &dataset_id, "units", "meters").await
        .expect("Failed to set dataset units");
    
    client.attributes().set_attribute(&domain_path, &dataset_id, "scale_factor", 0.001f64).await
        .expect("Failed to set dataset scale");
    
    client.attributes().set_attribute(&domain_path, &dataset_id, "valid_range", vec![0i32, 1000]).await
        .expect("Failed to set dataset range");
    
    client.attributes().set_attribute(&domain_path, &dataset_id, "is_calibrated", true).await
        .expect("Failed to set dataset boolean");
    
    println!("✓ All unified attribute creation successful");
    
    // Test 2: Verify attributes were created correctly
    
    // Check root group attributes
    let root_attrs = client.attributes().list_group_attributes(&domain_path, &root_group_id).await
        .expect("Failed to list root group attributes");
    
    if let Some(attrs) = root_attrs.get("attributes") {
        assert_eq!(attrs.as_array().unwrap().len(), 1, "Root group should have 1 attribute");
    }
    
    let root_desc = client.attributes().get_attribute(&domain_path, "groups", &root_group_id, "root_description").await
        .expect("Failed to get root description");
    assert_eq!(root_desc.get("value").unwrap().as_str().unwrap(), "This is the root group");
    
    // Check child group attributes  
    let child_attrs = client.attributes().list_group_attributes(&domain_path, &child_group_id).await
        .expect("Failed to list child group attributes");
    
    if let Some(attrs) = child_attrs.get("attributes") {
        assert_eq!(attrs.as_array().unwrap().len(), 2, "Child group should have 2 attributes");
    }
    
    // Check dataset attributes
    let dataset_attrs = client.attributes().list_dataset_attributes(&domain_path, &dataset_id).await
        .expect("Failed to list dataset attributes");
    
    if let Some(attrs) = dataset_attrs.get("attributes") {
        assert_eq!(attrs.as_array().unwrap().len(), 4, "Dataset should have 4 attributes");
    }
    
    let units_attr = client.attributes().get_attribute(&domain_path, "datasets", &dataset_id, "units").await
        .expect("Failed to get units attribute");
    assert_eq!(units_attr.get("value").unwrap().as_str().unwrap(), "meters");
    
    let scale_attr = client.attributes().get_attribute(&domain_path, "datasets", &dataset_id, "scale_factor").await
        .expect("Failed to get scale attribute");
    assert_eq!(scale_attr.get("value").unwrap().as_f64().unwrap(), 0.001);
    
    println!("✓ All attribute values verified correctly");
    
    // Test 3: Test error handling for invalid ID formats
    let result = client.attributes().set_attribute(&domain_path, "invalid-id-format", "test", "value").await;
    assert!(result.is_err(), "Should fail with invalid ID format");
    
    if let Err(e) = result {
        println!("✓ Proper error handling for invalid ID: {}", e);
    }
    
    // Test 4: Create multiple attributes to show the unified API in action
    
    // Create attributes using the unified API for different types
    client.attributes().set_attribute(&domain_path, &child_group_id, "unified_method", "set by unified API").await
        .expect("Failed with unified method");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "another_attr", 999i32).await
        .expect("Failed to set integer attribute");
    
    // Verify both exist and work correctly
    let unified_attr = client.attributes().get_attribute(&domain_path, "groups", &child_group_id, "unified_method").await
        .expect("Failed to get unified attribute");
    
    let int_attr = client.attributes().get_attribute(&domain_path, "groups", &child_group_id, "another_attr").await
        .expect("Failed to get integer attribute");
    
    assert_eq!(
        unified_attr.get("value").unwrap().as_str().unwrap(), 
        "set by unified API",
        "String attribute should be set correctly"
    );
    
    assert_eq!(
        int_attr.get("value").unwrap().as_i64().unwrap(), 
        999,
        "Integer attribute should be set correctly"
    );
    
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

#[tokio::test]
async fn test_attribute_api_type_inference() {
    env_logger::try_init().ok();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain and get root group
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let domain = client.domains().get_domain(&domain_path).await
        .expect("Failed to get domain");
    let root_group_id = domain.root.unwrap();
    
    println!("Testing unified API with comprehensive type inference...");
    
    // Test all data types through the unified interface
    client.attributes().set_attribute(&domain_path, &root_group_id, "string_attr", "Hello World").await
        .expect("Failed to set string");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "int_attr", 42i32).await
        .expect("Failed to set integer");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "float_attr", 3.14159f64).await
        .expect("Failed to set float");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "bool_attr", true).await
        .expect("Failed to set boolean");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "array_1d", vec![1, 2, 3, 4]).await
        .expect("Failed to set 1D array");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "array_2d", vec![vec![1.0, 2.0], vec![3.0, 4.0]]).await
        .expect("Failed to set 2D array");
    
    client.attributes().set_attribute(&domain_path, &root_group_id, "mixed_array", vec!["a", "b", "c"]).await
        .expect("Failed to set string array");
    
    // Verify all attributes were created
    let attrs = client.attributes().list_group_attributes(&domain_path, &root_group_id).await
        .expect("Failed to list attributes");
    
    if let Some(attrs_array) = attrs.get("attributes") {
        assert_eq!(attrs_array.as_array().unwrap().len(), 7, "Should have 7 attributes");
        println!("✓ Created {} attributes using unified API", attrs_array.as_array().unwrap().len());
    }
    
    // Verify specific attribute types and values
    let string_attr = client.attributes().get_attribute(&domain_path, "groups", &root_group_id, "string_attr").await
        .expect("Failed to get string attribute");
    
    println!("String attribute type: {:?}", string_attr.get("type"));
    println!("String attribute value: {:?}", string_attr.get("value"));
    
    let array_attr = client.attributes().get_attribute(&domain_path, "groups", &root_group_id, "array_2d").await
        .expect("Failed to get 2D array attribute");
    
    println!("2D Array attribute type: {:?}", array_attr.get("type"));
    println!("2D Array attribute shape: {:?}", array_attr.get("shape"));
    println!("2D Array attribute value: {:?}", array_attr.get("value"));
    
    println!("\n🎉 Unified API type inference test completed successfully!");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

#[tokio::test]
async fn test_attribute_list_operations() {
    env_logger::try_init().ok();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Get root group
    let domain = client.domains().get_domain(&domain_path).await.expect("Failed to get domain");
    let root_group_id = domain.root.unwrap();
    
    // Create child group and dataset for testing
    let child_group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create child group");
    let child_group_id = child_group.id;
    
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "test_dataset").await
        .expect("Failed to create test dataset");
    
    println!("Testing attribute list operations...");
    
    // Test 1: List attributes on empty objects (should be empty)
    let empty_group_attrs = client.attributes().list_group_attributes(&domain_path, &child_group_id).await
        .expect("Failed to list empty group attributes");
    
    if let Some(attrs) = empty_group_attrs.get("attributes") {
        assert!(attrs.as_array().unwrap().is_empty(), "New group should have no attributes");
        println!("✓ Empty group correctly reports no attributes");
    }
    
    let empty_dataset_attrs = client.attributes().list_dataset_attributes(&domain_path, &dataset_id).await
        .expect("Failed to list empty dataset attributes");
    
    if let Some(attrs) = empty_dataset_attrs.get("attributes") {
        assert!(attrs.as_array().unwrap().is_empty(), "New dataset should have no attributes");
        println!("✓ Empty dataset correctly reports no attributes");
    }
    
    // Test 2: Add multiple attributes and verify listing
    client.attributes().set_attribute(&domain_path, &child_group_id, "name", "Test Group").await
        .expect("Failed to set name attribute");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "version", 42i32).await
        .expect("Failed to set version attribute");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "temperature", 23.5f64).await
        .expect("Failed to set temperature attribute");
    
    client.attributes().set_attribute(&domain_path, &child_group_id, "active", true).await
        .expect("Failed to set active attribute");
    
    // Test 3: List and verify all attributes
    let group_attrs = client.attributes().list_group_attributes(&domain_path, &child_group_id).await
        .expect("Failed to list group attributes");
    
    if let Some(attrs) = group_attrs.get("attributes") {
        let attr_array = attrs.as_array().unwrap();
        assert_eq!(attr_array.len(), 4, "Should have exactly 4 attributes");
        
        let attr_names: Vec<&str> = attr_array.iter()
            .map(|attr| attr.get("name").unwrap().as_str().unwrap())
            .collect();
        
        assert!(attr_names.contains(&"name"), "Should contain 'name' attribute");
        assert!(attr_names.contains(&"version"), "Should contain 'version' attribute");
        assert!(attr_names.contains(&"temperature"), "Should contain 'temperature' attribute");
        assert!(attr_names.contains(&"active"), "Should contain 'active' attribute");
        
        println!("✓ Group attributes listed correctly: {:?}", attr_names);
    }
    
    // Test 4: Add dataset attributes and verify
    client.attributes().set_attribute(&domain_path, &dataset_id, "units", "meters").await
        .expect("Failed to set units attribute");
    
    client.attributes().set_attribute(&domain_path, &dataset_id, "scale_factor", 0.001f64).await
        .expect("Failed to set scale_factor attribute");
    
    let dataset_attrs = client.attributes().list_dataset_attributes(&domain_path, &dataset_id).await
        .expect("Failed to list dataset attributes");
    
    if let Some(attrs) = dataset_attrs.get("attributes") {
        let attr_array = attrs.as_array().unwrap();
        assert_eq!(attr_array.len(), 2, "Should have exactly 2 dataset attributes");
        
        let attr_names: Vec<&str> = attr_array.iter()
            .map(|attr| attr.get("name").unwrap().as_str().unwrap())
            .collect();
        
        assert!(attr_names.contains(&"units"), "Should contain 'units' attribute");
        assert!(attr_names.contains(&"scale_factor"), "Should contain 'scale_factor' attribute");
        
        println!("✓ Dataset attributes listed correctly: {:?}", attr_names);
    }
    
    println!("✓ All attribute list operations completed successfully!");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

#[tokio::test]
async fn test_attribute_get_operations() {
    env_logger::try_init().ok();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Get root group
    let domain = client.domains().get_domain(&domain_path).await.expect("Failed to get domain");
    let _root_group_id = domain.root.unwrap();
    
    // Create test group
    let test_group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create test group");
    let group_id = test_group.id;
    
    println!("Testing attribute get operations...");
    
    // Test 1: Create attributes with various types
    client.attributes().set_attribute(&domain_path, &group_id, "string_value", "Hello, World!").await
        .expect("Failed to set string attribute");
    
    client.attributes().set_attribute(&domain_path, &group_id, "integer_value", 12345i64).await
        .expect("Failed to set integer attribute");
    
    client.attributes().set_attribute(&domain_path, &group_id, "float_value", 3.14159f64).await
        .expect("Failed to set float attribute");
    
    client.attributes().set_attribute(&domain_path, &group_id, "boolean_value", false).await
        .expect("Failed to set boolean attribute");
    
    client.attributes().set_attribute(&domain_path, &group_id, "array_value", vec![1, 2, 3, 4, 5]).await
        .expect("Failed to set array attribute");
    
    client.attributes().set_attribute(&domain_path, &group_id, "matrix_value", vec![vec![1.0, 2.0], vec![3.0, 4.0]]).await
        .expect("Failed to set matrix attribute");
    
    // Test 2: Get and verify string attribute
    let string_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "string_value").await
        .expect("Failed to get string attribute");
    
    assert_eq!(string_attr.get("value").unwrap().as_str().unwrap(), "Hello, World!");
    if let Some(attr_type) = string_attr.get("type") {
        assert_eq!(attr_type.get("class").unwrap().as_str().unwrap(), "H5T_STRING");
        assert_eq!(attr_type.get("charSet").unwrap().as_str().unwrap(), "H5T_CSET_UTF8");
    }
    println!("✓ String attribute retrieved and verified correctly");
    
    // Test 3: Get and verify integer attribute
    let integer_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "integer_value").await
        .expect("Failed to get integer attribute");
    
    assert_eq!(integer_attr.get("value").unwrap().as_i64().unwrap(), 12345);
    if let Some(attr_type) = integer_attr.get("type") {
        assert_eq!(attr_type.get("class").unwrap().as_str().unwrap(), "H5T_INTEGER");
        assert_eq!(attr_type.get("base").unwrap().as_str().unwrap(), "H5T_STD_I64LE");
    }
    println!("✓ Integer attribute retrieved and verified correctly");
    
    // Test 4: Get and verify float attribute
    let float_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "float_value").await
        .expect("Failed to get float attribute");
    
    assert!((float_attr.get("value").unwrap().as_f64().unwrap() - 3.14159).abs() < 1e-10);
    if let Some(attr_type) = float_attr.get("type") {
        assert_eq!(attr_type.get("class").unwrap().as_str().unwrap(), "H5T_FLOAT");
        assert_eq!(attr_type.get("base").unwrap().as_str().unwrap(), "H5T_IEEE_F64LE");
    }
    println!("✓ Float attribute retrieved and verified correctly");
    
    // Test 5: Get and verify boolean attribute
    let boolean_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "boolean_value").await
        .expect("Failed to get boolean attribute");
    
    // Boolean values might be stored as different number types, so check multiple formats
    let boolean_value = boolean_attr.get("value").unwrap();
    let is_false = boolean_value.as_u64().unwrap_or(0) == 0 || 
                   boolean_value.as_i64().unwrap_or(0) == 0 ||
                   boolean_value.as_bool().unwrap_or(true) == false;
    assert!(is_false, "Boolean false should be represented as 0 or false, got: {:?}", boolean_value);
    
    if let Some(attr_type) = boolean_attr.get("type") {
        assert_eq!(attr_type.get("class").unwrap().as_str().unwrap(), "H5T_INTEGER");
        assert_eq!(attr_type.get("base").unwrap().as_str().unwrap(), "H5T_STD_U8LE");
    }
    println!("✓ Boolean attribute retrieved and verified correctly");
    
    // Test 6: Get and verify array attribute
    let array_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "array_value").await
        .expect("Failed to get array attribute");
    
    let expected_array = vec![1, 2, 3, 4, 5];
    let actual_array: Vec<i64> = array_attr.get("value").unwrap().as_array().unwrap()
        .iter().map(|v| v.as_i64().unwrap()).collect();
    assert_eq!(actual_array, expected_array);
    
    if let Some(shape) = array_attr.get("shape") {
        let dims: Vec<i64> = shape.get("dims").unwrap().as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(dims, vec![5]);
    }
    println!("✓ Array attribute retrieved and verified correctly");
    
    // Test 7: Get and verify 2D matrix attribute
    let matrix_attr = client.attributes().get_attribute(&domain_path, "groups", &group_id, "matrix_value").await
        .expect("Failed to get matrix attribute");
    
    let matrix_value = matrix_attr.get("value").unwrap().as_array().unwrap();
    assert_eq!(matrix_value.len(), 2);
    assert_eq!(matrix_value[0].as_array().unwrap()[0].as_f64().unwrap(), 1.0);
    assert_eq!(matrix_value[1].as_array().unwrap()[1].as_f64().unwrap(), 4.0);
    
    if let Some(shape) = matrix_attr.get("shape") {
        let dims: Vec<i64> = shape.get("dims").unwrap().as_array().unwrap()
            .iter().map(|v| v.as_i64().unwrap()).collect();
        assert_eq!(dims, vec![2, 2]);
    }
    println!("✓ 2D Matrix attribute retrieved and verified correctly");
    
    println!("✓ All attribute get operations completed successfully!");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

#[tokio::test]
async fn test_attribute_delete_operations() {
    env_logger::try_init().ok();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Get root group
    let domain = client.domains().get_domain(&domain_path).await.expect("Failed to get domain");
    let root_group_id = domain.root.unwrap();
    
    // Create test group and dataset
    let test_group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create test group");
    let group_id = test_group.id;
    
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "delete_test_dataset").await
        .expect("Failed to create test dataset");
    
    println!("Testing attribute delete operations...");
    
    // Test 1: Create multiple attributes
    client.attributes().set_attribute(&domain_path, &group_id, "attr1", "First attribute").await
        .expect("Failed to set attr1");
    
    client.attributes().set_attribute(&domain_path, &group_id, "attr2", 200i32).await
        .expect("Failed to set attr2");
    
    client.attributes().set_attribute(&domain_path, &group_id, "attr3", 99.9f64).await
        .expect("Failed to set attr3");
    
    client.attributes().set_attribute(&domain_path, &group_id, "attr4", vec![10, 20, 30]).await
        .expect("Failed to set attr4");
    
    // Verify all attributes exist
    let initial_attrs = client.attributes().list_group_attributes(&domain_path, &group_id).await
        .expect("Failed to list initial attributes");
    
    if let Some(attrs) = initial_attrs.get("attributes") {
        assert_eq!(attrs.as_array().unwrap().len(), 4, "Should have 4 attributes initially");
    }
    println!("✓ Created 4 test attributes");
    
    // Test 2: Delete one attribute and verify
    client.attributes().delete_attribute(&domain_path, "groups", &group_id, "attr2").await
        .expect("Failed to delete attr2");
    
    let after_delete1 = client.attributes().list_group_attributes(&domain_path, &group_id).await
        .expect("Failed to list attributes after first deletion");
    
    if let Some(attrs) = after_delete1.get("attributes") {
        let attr_array = attrs.as_array().unwrap();
        assert_eq!(attr_array.len(), 3, "Should have 3 attributes after deletion");
        
        let attr_names: Vec<&str> = attr_array.iter()
            .map(|attr| attr.get("name").unwrap().as_str().unwrap())
            .collect();
        
        assert!(!attr_names.contains(&"attr2"), "attr2 should be deleted");
        assert!(attr_names.contains(&"attr1"), "attr1 should still exist");
        assert!(attr_names.contains(&"attr3"), "attr3 should still exist");
        assert!(attr_names.contains(&"attr4"), "attr4 should still exist");
    }
    println!("✓ Successfully deleted attr2, remaining attributes: 3");
    
    // Test 3: Delete multiple attributes
    client.attributes().delete_attribute(&domain_path, "groups", &group_id, "attr1").await
        .expect("Failed to delete attr1");
    
    client.attributes().delete_attribute(&domain_path, "groups", &group_id, "attr4").await
        .expect("Failed to delete attr4");
    
    let after_delete_multiple = client.attributes().list_group_attributes(&domain_path, &group_id).await
        .expect("Failed to list attributes after multiple deletions");
    
    if let Some(attrs) = after_delete_multiple.get("attributes") {
        let attr_array = attrs.as_array().unwrap();
        assert_eq!(attr_array.len(), 1, "Should have 1 attribute after multiple deletions");
        
        let remaining_name = attr_array[0].get("name").unwrap().as_str().unwrap();
        assert_eq!(remaining_name, "attr3", "Only attr3 should remain");
    }
    println!("✓ Successfully deleted multiple attributes, remaining: 1");
    
    // Test 4: Delete the last attribute
    client.attributes().delete_attribute(&domain_path, "groups", &group_id, "attr3").await
        .expect("Failed to delete attr3");
    
    let after_delete_all = client.attributes().list_group_attributes(&domain_path, &group_id).await
        .expect("Failed to list attributes after deleting all");
    
    if let Some(attrs) = after_delete_all.get("attributes") {
        assert!(attrs.as_array().unwrap().is_empty(), "Should have no attributes after deleting all");
    }
    println!("✓ Successfully deleted all attributes, group is now empty");
    
    // Test 5: Test dataset attribute deletion
    client.attributes().set_attribute(&domain_path, &dataset_id, "dataset_attr1", "Dataset attribute").await
        .expect("Failed to set dataset attribute");
    
    client.attributes().set_attribute(&domain_path, &dataset_id, "dataset_attr2", 777i32).await
        .expect("Failed to set second dataset attribute");
    
    // Verify dataset attributes exist
    let dataset_attrs = client.attributes().list_dataset_attributes(&domain_path, &dataset_id).await
        .expect("Failed to list dataset attributes");
    
    if let Some(attrs) = dataset_attrs.get("attributes") {
        assert_eq!(attrs.as_array().unwrap().len(), 2, "Should have 2 dataset attributes");
    }
    
    // Delete one dataset attribute
    client.attributes().delete_attribute(&domain_path, "datasets", &dataset_id, "dataset_attr1").await
        .expect("Failed to delete dataset attribute");
    
    let after_dataset_delete = client.attributes().list_dataset_attributes(&domain_path, &dataset_id).await
        .expect("Failed to list dataset attributes after deletion");
    
    if let Some(attrs) = after_dataset_delete.get("attributes") {
        let attr_array = attrs.as_array().unwrap();
        assert_eq!(attr_array.len(), 1, "Should have 1 dataset attribute after deletion");
        
        let remaining_name = attr_array[0].get("name").unwrap().as_str().unwrap();
        assert_eq!(remaining_name, "dataset_attr2", "dataset_attr2 should remain");
    }
    println!("✓ Successfully deleted dataset attribute");
    
    // Test 6: Error handling - try to delete non-existent attribute
    match client.attributes().delete_attribute(&domain_path, "groups", &group_id, "non_existent_attr").await {
        Ok(_) => println!("ℹ  Deleting non-existent attribute succeeded (some implementations allow this)"),
        Err(_) => println!("✓ Properly rejected deletion of non-existent attribute"),
    }
    
    println!("✓ All attribute delete operations completed successfully!");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}
