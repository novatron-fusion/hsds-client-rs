/*
 * HSDS OpenAPI Client - Generated Rust client for HDF Scalable Data Service
 */

// Internal modules
mod client;
pub mod models;  // Make models public
mod apis;
mod error;
mod auth;

#[cfg(test)]
mod tests;

// Re-export public types and interfaces
pub use client::HsdsClient;
pub use models::*;
pub use apis::*;
pub use error::{HsdsError, HsdsResult};
pub use auth::{BasicAuth, BearerAuth, NoAuth};

// Prelude module for convenient imports
pub mod prelude {
    pub use crate::{
        HsdsClient, 
        BasicAuth, BearerAuth, NoAuth,
        HsdsError, HsdsResult,
        // Common model types
        Domain, Group, Dataset, Link,
        DatasetCreateRequest, DatasetValueRequest,
        DataTypeSpec, ShapeSpec, LinkRequest,
    };
}

// FFI exports for LabVIEW integration (optional)
#[cfg(feature = "ffi")]
pub mod ffi;

#[cfg(feature = "ffi")]
pub use ffi::*;
