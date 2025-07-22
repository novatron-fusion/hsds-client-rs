use hsds_client::{HsdsClient, BasicAuth, HsdsResult};
use std::time::{SystemTime, UNIX_EPOCH};

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
        .as_nanos(); // Use nanoseconds for better uniqueness
    format!("/home/admin/test_domain_{}.h5", timestamp)
}

/// Test basic domain creation
#[tokio::test]
async fn test_create_domain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    let domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create domain");
    
    assert!(domain.root.is_some(), "Domain should have a root group");
    assert_eq!(domain.owner.as_deref(), Some("admin"), "Domain should be owned by admin");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test domain creation with folder flag
#[tokio::test]
async fn test_create_folder() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    // Folders should NOT have .h5 extension - they are directories, not files
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let folder_path = format!("/home/admin/test_folder_{}", timestamp);
    
    let domain = client.domains().create_folder(&folder_path).await
        .expect("Failed to create folder");
    
    // Folders don't have root groups, while domains do
    assert!(domain.root.is_none(), "Folder should not have a root group");
    assert_eq!(domain.owner.as_deref(), Some("admin"), "Folder should be owned by admin");
    
    // Clean up
    client.domains().delete_domain(&folder_path).await.ok();
}

/// Test getting domain information
#[tokio::test]
async fn test_get_domain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create domain
    let created_domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create domain");
    
    // Get domain back
    let retrieved_domain = client.domains().get_domain(&domain_path).await
        .expect("Failed to retrieve domain");
    
    assert_eq!(created_domain.root, retrieved_domain.root, "Root groups should match");
    assert_eq!(created_domain.owner, retrieved_domain.owner, "Owners should match");
    
    // Clean up
    client.domains().delete_domain(&domain_path).await.ok();
}

/// Test deleting domain
#[tokio::test]
async fn test_delete_domain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    let domain_path = create_test_domain_name();
    
    // Create domain
    let _domain = client.domains().create_domain(&domain_path, None).await
        .expect("Failed to create domain");
    
    // Delete domain
    client.domains().delete_domain(&domain_path).await
        .expect("Failed to delete domain");
    
    // Try to get deleted domain (should fail)
    let result = client.domains().get_domain(&domain_path).await;
    assert!(result.is_err(), "Getting deleted domain should fail");
}

/// Test accessing existing /home domain
#[tokio::test]
async fn test_get_home_domain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    
    let home_domain = client.domains().get_domain("/home").await
        .expect("Failed to get /home domain");
    
    assert_eq!(home_domain.owner.as_deref(), Some("admin"), "/home should be owned by admin");
    assert!(home_domain.hrefs.is_some(), "/home should have hrefs");
}

/// Test accessing user subdomain
#[tokio::test]
async fn test_get_user_subdomain() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    
    let user_domain = client.domains().get_domain("/home/admin").await
        .expect("Failed to get /home/admin domain");
    
    assert_eq!(user_domain.owner.as_deref(), Some("admin"), "/home/admin should be owned by admin");
}

/// Integration test for domain lifecycle
#[tokio::test]
async fn test_domain_lifecycle() {
    let _ = env_logger::try_init();
    
    let client = create_test_client().expect("Failed to create client");
    
    // Test different domain types
    let file_domain = create_test_domain_name(); // Has .h5 extension for files
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let folder_domain = format!("/home/admin/test_folder_{}", timestamp); // No extension for folders
    
    // 1. Create file domain
    let domain = client.domains().create_domain(&file_domain, None).await
        .expect("Failed to create file domain");
    assert!(domain.root.is_some(), "File domain should have root group");
    
    // 2. Create folder domain
    let folder = client.domains().create_folder(&folder_domain).await
        .expect("Failed to create folder domain");
    assert!(folder.root.is_none(), "Folder should not have a root group");
    assert_eq!(folder.owner.as_deref(), Some("admin"), "Folder should be owned by admin");
    
    // 3. Retrieve both
    let retrieved_file = client.domains().get_domain(&file_domain).await
        .expect("Failed to retrieve file domain");
    
    let retrieved_folder = client.domains().get_domain(&folder_domain).await
        .expect("Failed to retrieve folder domain");
    
    assert_eq!(domain.root, retrieved_file.root);
    assert_eq!(folder.owner, retrieved_folder.owner);
    
    // 4. Clean up
    client.domains().delete_domain(&file_domain).await
        .expect("Failed to delete file domain");
    
    client.domains().delete_domain(&folder_domain).await
        .expect("Failed to delete folder domain");
    
    // 5. Verify deletion
    assert!(client.domains().get_domain(&file_domain).await.is_err());
    assert!(client.domains().get_domain(&folder_domain).await.is_err());
}
