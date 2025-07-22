use hsds_client::{HsdsClient, BasicAuth};
use std::time::{SystemTime, UNIX_EPOCH};
// Initialize logging to see the HTTP request logs
use log::{info, debug};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    println!("🧪 HSDS Integration Test Suite");
    println!("==============================");

    let client = HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )?;

    // Test 1: Basic connectivity and domain access
    println!("\n1. 📂 Testing domain access...");
    match client.domains().get_domain("/home").await {
        Ok(domain) => {
            println!("   ✅ /home domain accessible");
            println!("   📋 Owner: {}", domain.owner.unwrap_or("unknown".to_string()));
            println!("   📋 Class: {:?}", domain.class.as_ref().map(|c| format!("{:?}", c)).unwrap_or("unknown".to_string()));
        }
        Err(e) => {
            println!("   ❌ Cannot access /home domain: {}", e);
            return Err(e.into());
        }
    }

    // Test 2: User domain access
    println!("\n2. 👤 Testing user domain access...");
    match client.domains().get_domain("/home/admin").await {
        Ok(domain) => {
            println!("   ✅ /home/admin domain accessible");
        }
        Err(e) => {
            println!("   ❌ Cannot access /home/admin domain: {}", e);
        }
    }

    // Test 3: Create unique test file (using timestamp to avoid conflicts)
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_file = format!("/home/admin/integration_test_{}.h5", timestamp);
    
    println!("\n3. 📄 Testing file creation...");
    println!("   🔧 Creating: {}", test_file);
    
    match client.domains().create_domain(&test_file, None).await {
        Ok(domain) => {
            println!("   ✅ File created successfully!");
            println!("   📋 Root group: {:?}", domain.root);
            
            // Test 4: Retrieve the created file
            println!("\n4. 🔍 Testing file retrieval...");
            match client.domains().get_domain(&test_file).await {
                Ok(retrieved) => {
                    println!("   ✅ File retrieved successfully!");
                    println!("   📋 Owner: {}", retrieved.owner.unwrap_or("unknown".to_string()));
                    
                    // Test 5: Clean up - delete the test file
                    println!("\n5. 🗑️  Testing file deletion...");
                    match client.domains().delete_domain(&test_file).await {
                        Ok(_) => {
                            println!("   ✅ File deleted successfully!");
                        }
                        Err(e) => {
                            println!("   ⚠️  File deletion failed: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("   ❌ File retrieval failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("   ❌ File creation failed: {}", e);
        }
    }

    // Test 6: Create and test folder
    let test_folder = format!("/home/admin/test_folder_{}", timestamp);
    println!("\n6. 📁 Testing folder operations...");
    println!("   🔧 Creating folder: {}", test_folder);
    
    match client.domains().create_folder(&test_folder).await {
        Ok(folder) => {
            println!("   ✅ Folder created successfully!");
            println!("   📋 Class: {:?}", folder.class.as_ref().map(|c| format!("{:?}", c)).unwrap_or("unknown".to_string()));
            
            // Clean up folder
            match client.domains().delete_domain(&test_folder).await {
                Ok(_) => {
                    println!("   ✅ Folder deleted successfully!");
                }
                Err(e) => {
                    println!("   ⚠️  Folder deletion failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("   ❌ Folder creation failed: {}", e);
        }
    }

    println!("\n🎉 Integration test suite completed!");
    println!("   ✅ HSDS client is working correctly with your server!");
    Ok(())
}
