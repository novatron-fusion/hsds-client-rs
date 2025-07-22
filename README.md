# HSDS Rust Client

A Rust client library for the HDF Scalable Data Service (HSDS) REST API, generated from the OpenAPI specification.

## Features

- **Async/Await Support**: Built on `reqwest` and `tokio` for high-performance async HTTP operations
- **Type-Safe**: Strongly typed models generated from OpenAPI spec
- **Authentication**: Support for Basic Auth, Bearer tokens, or no authentication
- **Comprehensive API Coverage**: All HSDS endpoints including domains, groups, datasets, datatypes, links, and attributes
- **Error Handling**: Structured error types with detailed error information
- **LabVIEW Integration Ready**: Optional FFI layer for LabVIEW integration

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
hsds_client = { path = "../hsds_client" }
tokio = { version = "1.0", features = ["full"] }
```

## Quick Start

```rust
use hsds_client::{HsdsClient, BasicAuth, NoAuth};
use hsds_client::models::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client (no authentication)
    let client = HsdsClient::new("http://hsdshdflab.hdfgroup.org", NoAuth)?;
    
    // Or with authentication
    let client = HsdsClient::new(
        "http://your-hsds-server.com",
        BasicAuth::new("username", "password")
    )?;

    // Create a domain (like an HDF5 file)
    let domain_path = "/home/user/myfile.h5";
    let domain = client.domains().create_domain(domain_path, None).await?;
    
    println!("Created domain with root group: {:?}", domain.root);
    
    // Create a dataset
    let dataset_request = DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined("H5T_STD_I32LE".to_string()),
        shape: Some(ShapeSpec::Dimensions(vec![100, 100])),
        maxdims: Some(vec![0, 0]), // Unlimited dimensions
        creation_properties: None,
        link: Some(LinkRequest {
            id: domain.root.unwrap(),
            name: "my_data".to_string(),
        }),
    };
    
    let dataset = client.datasets().create_dataset(domain_path, dataset_request).await?;
    println!("Created dataset: {}", dataset.id);

    Ok(())
}
```

## API Overview

### Domains
```rust
// Create domain
let domain = client.domains().create_domain("/path/to/file.h5", None).await?;

// Get domain info
let domain = client.domains().get_domain("/path/to/file.h5").await?;

// Delete domain
client.domains().delete_domain("/path/to/file.h5").await?;
```

### Groups
```rust
// Create group
let group = client.groups().create_group(domain_path, None).await?;

// Get group info
let group = client.groups().get_group(domain_path, &group_id, None).await?;

// List all groups
let groups = client.groups().list_groups(domain_path).await?;
```

### Datasets
```rust
// Create dataset
let request = DatasetCreateRequest { /* ... */ };
let dataset = client.datasets().create_dataset(domain_path, request).await?;

// Write data
let write_request = DatasetValueRequest {
    start: Some(vec![0, 0]),
    stop: Some(vec![10, 10]),
    value: Some(serde_json::json!([[1, 2, 3]; 10])),
    // ...
};
client.datasets().write_dataset_values(domain_path, &dataset_id, write_request).await?;

// Read data
let data = client.datasets().read_dataset_values_json(
    domain_path, 
    &dataset_id,
    Some("[0:10,0:10]"), // Selection
    None, // Query
    None  // Limit
).await?;
```

### Links
```rust
// Create hard link
client.links().create_hard_link(domain_path, &group_id, "link_name", &target_id).await?;

// Create soft link
client.links().create_soft_link(domain_path, &group_id, "link_name", "/path/target").await?;

// List links in group
let links = client.links().list_links(domain_path, &group_id, None, None).await?;
```

### Attributes
```rust
// Add attribute to group
let attr_data = serde_json::json!({
    "type": "H5T_C_S1",
    "value": "attribute value"
});
client.attributes().put_group_attribute(domain_path, &group_id, "attr_name", attr_data).await?;

// List attributes
let attrs = client.attributes().list_group_attributes(domain_path, &group_id).await?;
```

## Error Handling

The client uses a comprehensive error type system:

```rust
use hsds_client::error::HsdsError;

match client.domains().get_domain("/nonexistent").await {
    Ok(domain) => println!("Found: {:?}", domain),
    Err(HsdsError::ObjectNotFound(msg)) => println!("Not found: {}", msg),
    Err(HsdsError::PermissionDenied(msg)) => println!("Access denied: {}", msg),
    Err(HsdsError::Auth(msg)) => println!("Authentication failed: {}", msg),
    Err(e) => println!("Other error: {}", e),
}
```

## Logging

The client includes built-in logging for HTTP requests to help with debugging and monitoring. Add the `log` crate and a logging implementation like `env_logger` to your dependencies:

```toml
[dependencies]
hsds_client = { path = "../hsds_client" }
log = "0.4"
env_logger = "0.11"  # or your preferred logging implementation
```

### Enable Logging

```rust
use hsds_client::{HsdsClient, BasicAuth};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();
    
    let client = HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )?;
    
    // Make API calls - logging will show HTTP requests
    let domain = client.domains().get_domain("/home").await?;
    
    Ok(())
}
```

### Logging Levels

Set the `RUST_LOG` environment variable to control logging verbosity:

```bash
# Windows
set RUST_LOG=info
cargo run --example your_example

# Linux/Mac
export RUST_LOG=info
cargo run --example your_example
```

**Available log levels:**

- **`RUST_LOG=info`** - Shows high-level operations:
  ```
  [INFO] Creating domain: /home/user/test.h5
  [INFO] Getting domain: /home/user/test.h5  
  [INFO] Deleting domain: /home/user/test.h5
  ```

- **`RUST_LOG=debug`** - Shows detailed HTTP request information:
  ```
  [INFO] Creating domain: /home/user/test.h5
  [DEBUG] HTTP PUT / with domain=/home/user/test.h5
  [DEBUG] Request body: DomainCreateRequest { folder: Some(1) }
  [DEBUG] starting new connection: http://localhost:5101/
  ```

- **`RUST_LOG=trace`** - Shows full reqwest HTTP tracing (very verbose)

### Filtering Logs

You can filter logs to only show HSDS client logs:

```bash
# Only show HSDS client logs
set RUST_LOG=hsds_client=debug

# Show HSDS client and reqwest logs
set RUST_LOG=hsds_client=debug,reqwest=info
```

## Authentication

### Basic Authentication
```rust
use hsds_client::BasicAuth;

let client = HsdsClient::new(
    "http://server.com",
    BasicAuth::new("username", "password")
)?;
```

### Bearer Token
```rust
use hsds_client::BearerAuth;

let client = HsdsClient::new(
    "http://server.com", 
    BearerAuth::new("your-token")
)?;
```

### No Authentication
```rust
use hsds_client::NoAuth;

let client = HsdsClient::new("http://server.com", NoAuth)?;
```

## Building

```bash
# Build the library
cargo build

# Run tests
cargo test

# Build with FFI support for LabVIEW
cargo build --features ffi

# Run examples
cargo run --example basic_usage
```

## Integration with existing reqwest-rs-labview

This client is designed to integrate with the existing reqwest-rs-labview architecture:

1. Uses the same `reqwest` and `tokio` versions
2. Compatible error handling patterns  
3. Can share the same HTTP client instance
4. Optional FFI layer for LabVIEW integration

## Generated from OpenAPI

This client was generated from the HSDS OpenAPI 3.1 specification. The models and API methods correspond directly to the HSDS REST API endpoints.

## License

Same as parent project.
