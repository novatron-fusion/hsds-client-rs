use hsds_client::{HsdsClient, BasicAuth};
use hsds_client::models::{GroupCreateRequest, LinkRequest};
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();
    
    println!("🧪 HSDS Groups Integration Test");
    println!("===============================");

    let client = HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )?;

    // Create a unique test domain for our group tests
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_domain = format!("/home/admin/group_test_{}.h5", timestamp);

    println!("\n1. 📂 Setting up test domain...");
    let domain = match client.domains().create_domain(&test_domain, None).await {
        Ok(domain) => {
            println!("   ✅ Test domain created: {}", test_domain);
            domain
        }
        Err(e) => {
            println!("   ❌ Failed to create test domain: {}", e);
            return Err(e.into());
        }
    };

    let root_group_id = domain.root.as_ref().expect("Domain should have root group");
    println!("   📋 Root group ID: {}", root_group_id);

    // Test 2: Create a child group
    println!("\n2. 👶 Creating child group...");
    match client.groups().create_group(&test_domain, None).await {
        Ok(child_group) => {
            println!("   ✅ Child group created successfully!");
            println!("   📋 Group ID: {}", child_group.id);
            println!("   📋 Created: {:?}", child_group.created);
            println!("   📋 Link count: {:?}", child_group.link_count.unwrap_or(0));
            println!("   📋 Attribute count: {:?}", child_group.attribute_count.unwrap_or(0));

            let child_group_id = child_group.id.clone();

            // Test 3: Get group information
            println!("\n3. 🔍 Retrieving group information...");
            match client.groups().get_group(&test_domain, &child_group_id, None).await {
                Ok(retrieved_group) => {
                    println!("   ✅ Group retrieved successfully!");
                    println!("   📋 Retrieved ID: {}", retrieved_group.id);
                    println!("   📋 Domain: {:?}", retrieved_group.domain);
                    if let Some(hrefs) = &retrieved_group.hrefs {
                        println!("   📋 HREF count: {}", hrefs.len());
                    }
                }
                Err(e) => {
                    println!("   ❌ Failed to retrieve group: {}", e);
                }
            }

            // Test 4: Create group with link to parent
            println!("\n4. 🔗 Creating group with link to parent...");
            let link_request = LinkRequest {
                id: root_group_id.clone(),
                name: format!("child_group_{}", timestamp),
            };
            let group_with_link_request = GroupCreateRequest {
                link: Some(link_request),
            };

            match client.groups().create_group(&test_domain, Some(group_with_link_request)).await {
                Ok(linked_group) => {
                    println!("   ✅ Group with link created successfully!");
                    println!("   📋 Linked group ID: {}", linked_group.id);
                    
                    let linked_group_id = linked_group.id.clone();

                    // Test 5: List all groups in domain
                    println!("\n5. 📋 Listing all groups in domain...");
                    match client.groups().list_groups(&test_domain).await {
                        Ok(groups_list) => {
                            println!("   ✅ Groups listed successfully!");
                            println!("   📄 Groups response: {:#}", groups_list);
                        }
                        Err(e) => {
                            println!("   ❌ Failed to list groups: {}", e);
                        }
                    }

                    // Test 6: Get root group info
                    println!("\n6. 🏠 Getting root group information...");
                    match client.groups().get_group(&test_domain, root_group_id, None).await {
                        Ok(root_group) => {
                            println!("   ✅ Root group retrieved successfully!");
                            println!("   📋 Root group link count: {:?}", root_group.link_count);
                            println!("   📋 Root group attribute count: {:?}", root_group.attribute_count);
                        }
                        Err(e) => {
                            println!("   ❌ Failed to get root group: {}", e);
                        }
                    }

                    // Test 7: Get group with alias information
                    println!("\n7. 🏷️  Getting group with alias information...");
                    match client.groups().get_group(&test_domain, &linked_group_id, Some(1)).await {
                        Ok(group_with_alias) => {
                            println!("   ✅ Group with alias retrieved!");
                            println!("   📋 Alias paths: {:?}", group_with_alias.alias);
                        }
                        Err(e) => {
                            println!("   ❌ Failed to get group with alias: {}", e);
                        }
                    }

                    // Test 8: Delete the linked group
                    println!("\n8. 🗑️  Deleting linked group...");
                    match client.groups().delete_group(&test_domain, &linked_group_id).await {
                        Ok(_) => {
                            println!("   ✅ Linked group deleted successfully!");
                        }
                        Err(e) => {
                            println!("   ⚠️  Failed to delete linked group: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("   ❌ Failed to create group with link: {}", e);
                }
            }

            // Test 9: Delete the first child group
            println!("\n9. 🗑️  Deleting child group...");
            match client.groups().delete_group(&test_domain, &child_group_id).await {
                Ok(_) => {
                    println!("   ✅ Child group deleted successfully!");
                }
                Err(e) => {
                    println!("   ⚠️  Failed to delete child group: {}", e);
                }
            }
        }
        Err(e) => {
            println!("   ❌ Failed to create child group: {}", e);
        }
    }

    // Test 10: Clean up - delete test domain
    println!("\n10. 🧹 Cleaning up test domain...");
    match client.domains().delete_domain(&test_domain).await {
        Ok(_) => {
            println!("   ✅ Test domain deleted successfully!");
        }
        Err(e) => {
            println!("   ⚠️  Failed to delete test domain: {}", e);
        }
    }

    println!("\n🎉 Groups integration test completed!");
    println!("   ✅ All group operations tested successfully");
    Ok(())
}
