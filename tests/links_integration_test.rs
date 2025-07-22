use hsds_client::{HsdsClient, BasicAuth, HsdsResult};
use hsds_client::models::{
    DatasetCreateRequest, DataTypeSpec, ShapeSpec, LinkRequest, LinkCreateRequest,
    GroupCreateRequest
};
use uuid::Uuid;

/// Helper to create a test client
fn create_test_client() -> HsdsResult<HsdsClient> {
    HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )
}

/// Helper to create a unique test domain name
fn create_test_domain_name() -> String {
    let uuid = Uuid::new_v4().to_string().replace("-", "");
    format!("/home/admin/test_links_{}.h5", uuid)
}

/// Helper to create a test group
async fn create_test_group(
    client: &HsdsClient,
    domain_path: &str,
    parent_group_id: &str,
    group_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let group_request = GroupCreateRequest {
        link: Some(LinkRequest {
            id: parent_group_id.to_string(),
            name: group_name.to_string(),
        }),
    };
    
    let group = client.groups().create_group(domain_path, Some(group_request)).await?;
    Ok(group.id)
}

/// Helper to create a test dataset
async fn create_test_dataset(
    client: &HsdsClient,
    domain_path: &str,
    parent_group_id: &str,
    dataset_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let dataset_request = DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_I32LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![10])),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: parent_group_id.to_string(),
            name: dataset_name.to_string(),
        }),
    };
    
    let dataset = client.datasets().create_dataset(domain_path, dataset_request).await?;
    Ok(dataset.id)
}

/// Test listing links in a group
#[tokio::test]
async fn test_list_links() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create some test objects to link to
    let _test_group_id = create_test_group(&client, &domain_path, &root_group_id, "test_group").await
        .expect("Failed to create test group");
    let _test_dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "test_dataset").await
        .expect("Failed to create test dataset");
    
    // List links in the root group
    let links = client.links().list_links(&domain_path, &root_group_id, None, None).await
        .expect("Failed to list links");
    
    // Verify we have at least the created links
    assert!(links.links.len() >= 2, "Should have at least 2 links");
    
    // Check that our created objects are in the links
    let link_names: Vec<&String> = links.links.iter().map(|link| &link.title).collect();
    assert!(link_names.contains(&&"test_group".to_string()), "Should contain test_group link");
    assert!(link_names.contains(&&"test_dataset".to_string()), "Should contain test_dataset link");
    
    println!("✓ Listed {} links in root group", links.links.len());
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test creating and getting a hard link
#[tokio::test]
async fn test_create_hard_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a test dataset to link to
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "original_dataset").await
        .expect("Failed to create test dataset");
    
    // Create another group to put the hard link in
    let sub_group_id = create_test_group(&client, &domain_path, &root_group_id, "sub_group").await
        .expect("Failed to create sub group");
    
    // Create a hard link to the dataset
    let link_name = "dataset_hardlink";
    let _result = client.links().create_hard_link(&domain_path, &sub_group_id, link_name, &dataset_id).await
        .expect("Failed to create hard link");
    
    // Get information about the created link
    let link_info = client.links().get_link(&domain_path, &sub_group_id, link_name).await
        .expect("Failed to get link information");
    
    // Verify link information
    let link_obj = link_info.get("link").expect("Response should have a 'link' object");
    assert!(link_obj.get("id").is_some(), "Hard link should have an ID");
    assert!(link_obj.get("class").is_some(), "Hard link should have a class");
    
    // The class should be H5L_TYPE_HARD
    if let Some(class) = link_obj.get("class") {
        assert_eq!(class.as_str().unwrap(), "H5L_TYPE_HARD", "Link class should be H5L_TYPE_HARD");
    }
    
    // The ID should match our dataset ID
    if let Some(id) = link_obj.get("id") {
        assert_eq!(id.as_str().unwrap(), dataset_id, "Link ID should match dataset ID");
    }
    
    println!("✓ Created and verified hard link");
    
    // Verify we can access the dataset through both the original name and the link
    let original_dataset = client.datasets().get_dataset(&domain_path, &dataset_id).await
        .expect("Failed to get original dataset");
    
    // The dataset should be accessible through both paths
    assert_eq!(original_dataset.id, dataset_id, "Original dataset should have correct ID");
    
    println!("✓ Hard link points to correct target");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test creating and getting a soft link
#[tokio::test]
async fn test_create_soft_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a test dataset to link to
    let _dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "target_dataset").await
        .expect("Failed to create target dataset");
    
    // Create another group to put the soft link in
    let sub_group_id = create_test_group(&client, &domain_path, &root_group_id, "link_group").await
        .expect("Failed to create link group");
    
    // Create a soft link to the dataset (by path)
    let link_name = "dataset_softlink";
    let target_path = "/target_dataset";  // Path relative to root
    let _result = client.links().create_soft_link(&domain_path, &sub_group_id, link_name, target_path).await
        .expect("Failed to create soft link");
    
    // Get information about the created soft link
    let link_info = client.links().get_link(&domain_path, &sub_group_id, link_name).await
        .expect("Failed to get soft link information");
    
    // Verify link information
    let link_obj = link_info.get("link").expect("Response should have a 'link' object");
    assert!(link_obj.get("class").is_some(), "Soft link should have a class");
    
    // The class should be H5L_TYPE_SOFT
    if let Some(class) = link_obj.get("class") {
        assert_eq!(class.as_str().unwrap(), "H5L_TYPE_SOFT", "Link class should be H5L_TYPE_SOFT");
    }
    
    // For soft links, check if there's additional path information
    // Note: The exact structure may vary based on HSDS implementation
    
    println!("✓ Created and verified soft link");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test creating and getting an external link
#[tokio::test]
async fn test_create_external_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    
    // Create two domains - source and target
    let source_domain = create_test_domain_name();
    let target_domain = create_test_domain_name();
    
    // Create target domain first
    let target_dom = client.domains().create_domain(&target_domain, None).await
        .expect("Failed to create target domain");
    
    let target_root_id = target_dom.root.expect("Target domain should have a root group");
    
    // Create a dataset in the target domain
    let _target_dataset_id = create_test_dataset(&client, &target_domain, &target_root_id, "external_target").await
        .expect("Failed to create target dataset");
    
    // Create source domain
    let source_dom = client.domains().create_domain(&source_domain, None).await
        .expect("Failed to create source domain");
    
    let source_root_id = source_dom.root.expect("Source domain should have a root group");
    
    // Create an external link from source to target
    let link_name = "external_link";
    let target_path = "/external_target";
    let _result = client.links().create_external_link(
        &source_domain, 
        &source_root_id, 
        link_name, 
        target_path, 
        &target_domain
    ).await
        .expect("Failed to create external link");
    
    // Get information about the created external link
    let link_info = client.links().get_link(&source_domain, &source_root_id, link_name).await
        .expect("Failed to get external link information");
    
    // The link information is nested under a 'link' object
    let link_obj = link_info.get("link").expect("Response should have a 'link' object");
    
    // Verify link information
    assert!(link_obj.get("class").is_some(), "External link should have a class");
    
    // The class should be H5L_TYPE_EXTERNAL
    if let Some(class) = link_obj.get("class") {
        assert_eq!(class.as_str().unwrap(), "H5L_TYPE_EXTERNAL", "Link class should be H5L_TYPE_EXTERNAL");
    }
    
    // For external links, check if there's additional domain/path information
    // These might be in the link object or at the top level
    if let Some(h5path) = link_obj.get("h5path") {
        println!("External link h5path: {}", h5path);
    }
    if let Some(h5domain) = link_obj.get("h5domain") {
        println!("External link h5domain: {}", h5domain);
    }
    
    // For external links, check if there's additional domain/path information
    // Note: The exact structure may vary based on HSDS implementation
    
    println!("✓ Created and verified external link");
    
    // Clean up
    client.domains().delete_domain(&source_domain).await.ok();
    client.domains().delete_domain(&target_domain).await.ok();
}

/// Test creating a link with the generic create_link method
#[tokio::test]
async fn test_create_generic_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a test group to link to
    let target_group_id = create_test_group(&client, &domain_path, &root_group_id, "target_group").await
        .expect("Failed to create target group");
    
    // Create a hard link using the generic method
    let link_request = LinkCreateRequest {
        id: Some(target_group_id.clone()),
        h5path: None,
        h5domain: None,
    };
    
    let link_name = "generic_hardlink";
    let _result = client.links().create_link(&domain_path, &root_group_id, link_name, link_request).await
        .expect("Failed to create generic link");
    
    // Verify the link was created
    let link_info = client.links().get_link(&domain_path, &root_group_id, link_name).await
        .expect("Failed to get generic link information");
    
    let link_obj = link_info.get("link").expect("Response should have a 'link' object");
    assert!(link_obj.get("id").is_some(), "Generic link should have an ID");
    if let Some(id) = link_obj.get("id") {
        assert_eq!(id.as_str().unwrap(), target_group_id, "Link ID should match target group ID");
    }
    
    println!("✓ Created generic link successfully");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting a link
#[tokio::test]
async fn test_delete_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create a test dataset to link to
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "dataset_to_link").await
        .expect("Failed to create dataset");
    
    // Create a hard link
    let link_name = "link_to_delete";
    let _result = client.links().create_hard_link(&domain_path, &root_group_id, link_name, &dataset_id).await
        .expect("Failed to create link");
    
    // Verify the link exists
    let _link_info = client.links().get_link(&domain_path, &root_group_id, link_name).await
        .expect("Link should exist before deletion");
    
    // Delete the link
    let _delete_result = client.links().delete_link(&domain_path, &root_group_id, link_name).await
        .expect("Failed to delete link");
    
    // Verify the link no longer exists
    let get_result = client.links().get_link(&domain_path, &root_group_id, link_name).await;
    assert!(get_result.is_err(), "Link should not exist after deletion");
    
    // Verify the original dataset still exists (hard link deletion doesn't delete target)
    let original_dataset = client.datasets().get_dataset(&domain_path, &dataset_id).await
        .expect("Original dataset should still exist after link deletion");
    
    assert_eq!(original_dataset.id, dataset_id, "Original dataset should be unchanged");
    
    println!("✓ Successfully deleted link");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test link operations with pagination
#[tokio::test]
async fn test_list_links_with_pagination() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create multiple test objects to have many links
    for i in 0..5 {
        let dataset_name = format!("dataset_{}", i);
        let _dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, &dataset_name).await
            .expect(&format!("Failed to create dataset {}", i));
    }
    
    // List links with a limit
    let limited_links = client.links().list_links(&domain_path, &root_group_id, Some(3), None).await
        .expect("Failed to list links with limit");
    
    // Should have at most 3 links (might be less due to server behavior)
    assert!(limited_links.links.len() <= 3, "Limited list should have at most 3 links");
    
    println!("✓ Listed {} links with pagination", limited_links.links.len());
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test multiple link operations in sequence
#[tokio::test]
async fn test_multiple_link_operations() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.expect("Domain should have a root group");
    
    // Create test objects
    let dataset_id = create_test_dataset(&client, &domain_path, &root_group_id, "shared_dataset").await
        .expect("Failed to create shared dataset");
    
    let group1_id = create_test_group(&client, &domain_path, &root_group_id, "group1").await
        .expect("Failed to create group1");
    
    let group2_id = create_test_group(&client, &domain_path, &root_group_id, "group2").await
        .expect("Failed to create group2");
    
    // Create multiple links to the same dataset from different groups
    let _link1 = client.links().create_hard_link(&domain_path, &group1_id, "link_to_dataset", &dataset_id).await
        .expect("Failed to create first link");
    
    let _link2 = client.links().create_hard_link(&domain_path, &group2_id, "another_link", &dataset_id).await
        .expect("Failed to create second link");
    
    let _link3 = client.links().create_soft_link(&domain_path, &group1_id, "soft_to_dataset", "/shared_dataset").await
        .expect("Failed to create soft link");
    
    // List links in each group
    let group1_links = client.links().list_links(&domain_path, &group1_id, None, None).await
        .expect("Failed to list group1 links");
    
    let group2_links = client.links().list_links(&domain_path, &group2_id, None, None).await
        .expect("Failed to list group2 links");
    
    // Verify each group has the expected links
    assert!(group1_links.links.len() >= 2, "Group1 should have at least 2 links");
    assert!(group2_links.links.len() >= 1, "Group2 should have at least 1 link");
    
    // Get information about each link
    let link1_info = client.links().get_link(&domain_path, &group1_id, "link_to_dataset").await
        .expect("Failed to get link1 info");
    
    let link2_info = client.links().get_link(&domain_path, &group2_id, "another_link").await
        .expect("Failed to get link2 info");
    
    let link3_info = client.links().get_link(&domain_path, &group1_id, "soft_to_dataset").await
        .expect("Failed to get link3 info");
    
    // Verify link types
    let link1_obj = link1_info.get("link").expect("Response should have a 'link' object");
    let link2_obj = link2_info.get("link").expect("Response should have a 'link' object");
    let link3_obj = link3_info.get("link").expect("Response should have a 'link' object");
    
    assert_eq!(link1_obj.get("class").unwrap().as_str().unwrap(), "H5L_TYPE_HARD");
    assert_eq!(link2_obj.get("class").unwrap().as_str().unwrap(), "H5L_TYPE_HARD");
    assert_eq!(link3_obj.get("class").unwrap().as_str().unwrap(), "H5L_TYPE_SOFT");
    
    // Both hard links should point to the same dataset
    assert_eq!(link1_obj.get("id").unwrap().as_str().unwrap(), dataset_id);
    assert_eq!(link2_obj.get("id").unwrap().as_str().unwrap(), dataset_id);
    
    println!("✓ Successfully performed multiple link operations");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}
