use hsds_client::{HsdsClient, BasicAuth, HsdsResult};
use hsds_client::models::{GroupCreateRequest, LinkRequest};
use std::time::{SystemTime, UNIX_EPOCH};

/// Helper to create a test client
fn create_test_client() -> HsdsResult<HsdsClient> {
    HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )
}

/// Helper to create a unique test domain
fn create_test_domain_name() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos(); // Use nanoseconds for better uniqueness
    format!("/home/admin/test_groups_{}.h5", timestamp)
}

/// Test basic group creation
#[tokio::test]
async fn test_create_group() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Create a child group
    let group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create group");
    
    assert!(!group.id.is_empty(), "Group should have an ID");
    assert!(group.created.is_some(), "Group should have creation time");
    
    // Clean up
    client.groups().delete_group(&domain_path, &group.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test group creation with link
#[tokio::test]
async fn test_create_group_with_link() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain first
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.as_ref().expect("Domain should have root group");
    
    // Create group with link to parent
    let link_request = LinkRequest {
        id: root_group_id.clone(),
        name: "test_child_group".to_string(),
    };
    
    let group_request = GroupCreateRequest {
        link: Some(link_request),
    };
    
    let group = client.groups().create_group(&domain_path, Some(group_request)).await
        .expect("Failed to create group with link");
    
    assert!(!group.id.is_empty(), "Group should have an ID");
    
    // Clean up
    client.groups().delete_group(&domain_path, &group.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting group information
#[tokio::test]
async fn test_get_group() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Create a group
    let created_group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create group");
    
    // Get the group back
    let retrieved_group = client.groups().get_group(&domain_path, &created_group.id, None).await
        .expect("Failed to retrieve group");
    
    assert_eq!(created_group.id, retrieved_group.id, "Group IDs should match");
    
    // Clean up
    client.groups().delete_group(&domain_path, &created_group.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test getting group with alias information
#[tokio::test]
async fn test_get_group_with_alias() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.as_ref().expect("Domain should have root group");
    
    // Create group with link (so it has an alias)
    let link_request = LinkRequest {
        id: root_group_id.clone(),
        name: "aliased_group".to_string(),
    };
    
    let group_request = GroupCreateRequest {
        link: Some(link_request),
    };
    
    let group = client.groups().create_group(&domain_path, Some(group_request)).await
        .expect("Failed to create group with link");
    
    // Get group with alias information
    let group_with_alias = client.groups().get_group(&domain_path, &group.id, Some(1)).await
        .expect("Failed to get group with alias");
    
    assert_eq!(group.id, group_with_alias.id, "Group IDs should match");
    
    // Clean up
    client.groups().delete_group(&domain_path, &group.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test listing groups in domain
#[tokio::test]
async fn test_list_groups() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Create a couple of groups
    let group1 = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create first group");
    
    let group2 = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create second group");
    
    // List groups
    let groups_response = client.groups().list_groups(&domain_path).await
        .expect("Failed to list groups");
    
    // The response should contain information about the groups
    assert!(!groups_response.to_string().is_empty(), "Groups list should not be empty");
    
    // Clean up
    client.groups().delete_group(&domain_path, &group1.id).await.ok();
    client.groups().delete_group(&domain_path, &group2.id).await.ok();
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting a group
#[tokio::test]
async fn test_delete_group() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create test domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    // Create a group
    let group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create group");
    
    // Delete the group
    client.groups().delete_group(&domain_path, &group.id).await
        .expect("Failed to delete group");
    
    // Try to get the deleted group (should fail)
    let result = client.groups().get_group(&domain_path, &group.id, None).await;
    assert!(result.is_err(), "Getting deleted group should fail");
    
    // Clean up domain
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Integration test that exercises the full group lifecycle
#[tokio::test]
async fn test_group_lifecycle() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // 1. Create domain
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create test domain");
    
    let root_group_id = domain.root.as_ref().expect("Domain should have root group");
    
    // 2. Create child group
    let child_group = client.groups().create_group(&domain_path, None).await
        .expect("Failed to create child group");
    
    // 3. Create linked group
    let link_request = LinkRequest {
        id: root_group_id.clone(),
        name: "linked_child".to_string(),
    };
    
    let group_request = GroupCreateRequest {
        link: Some(link_request),
    };
    
    let linked_group = client.groups().create_group(&domain_path, Some(group_request)).await
        .expect("Failed to create linked group");
    
    // 4. Verify we can retrieve both groups
    let retrieved_child = client.groups().get_group(&domain_path, &child_group.id, None).await
        .expect("Failed to retrieve child group");
    
    let retrieved_linked = client.groups().get_group(&domain_path, &linked_group.id, Some(1)).await
        .expect("Failed to retrieve linked group");
    
    assert_eq!(child_group.id, retrieved_child.id);
    assert_eq!(linked_group.id, retrieved_linked.id);
    
    // 5. List all groups
    let _groups = client.groups().list_groups(&domain_path).await
        .expect("Failed to list groups");
    
    // 6. Clean up groups
    client.groups().delete_group(&domain_path, &child_group.id).await
        .expect("Failed to delete child group");
    
    client.groups().delete_group(&domain_path, &linked_group.id).await
        .expect("Failed to delete linked group");
    
    // 7. Clean up domain
    client.domains().delete_domain(&domain_path).await
        .expect("Failed to delete test domain");
}
