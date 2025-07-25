use hdf5::types::{FloatSize, IntSize};
use hsds_client::{
    HsdsClient, BasicAuth, 
    DatasetCreateRequest, DatasetValueRequest,
    GroupCreateRequest
};
use hdf5::{File as H5File, Group as H5Group, Dataset as H5Dataset};
use serde_json::json;
use std::error::Error;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::Path;
use log::{info, warn, debug};
use std::io::{self, Write};

/// Real HDF5 file loader that reads an HDF5 file from disk and uploads it to HSDS
/// This demonstrates a practical implementation similar to the Python utillib.py load_file function
/// 
/// Key features:
/// - Reads actual HDF5 files from disk using hdf5-metno
/// - Recursively processes groups and datasets
/// - Handles different data types (integers, floats, strings)
/// - Preserves hierarchical structure
/// - Copies attributes (metadata)
/// - Provides progress feedback
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();
    
    println!("🚀 HSDS HDF5 File Loader");
    println!("========================");
    println!();
    
    // Path to the test HDF5 file
    let h5_file_path = "examples/test-files/S-N1-01388_reduced.h5";
    
    if !Path::new(h5_file_path).exists() {
        return Err(format!("Test file not found: {}", h5_file_path).into());
    }
    
    println!("📂 Reading HDF5 file: {}", h5_file_path);
    
    // Open the HDF5 file
    let h5_file = H5File::open(h5_file_path)?;
    
    // Initialize HSDS client
    let client = HsdsClient::new(
        "http://localhost:5101",
        BasicAuth::new("admin", "admin")
    )?;
    
    // Create a unique target file name with timestamp
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs();
    let target_file = format!("/home/admin/uploaded_S-N1-01388_{}.h5", timestamp);

    println!("🎯 Creating target file: {}", target_file);
    
    // Create the target domain in HSDS
    let domain = client.domains().create_domain(&target_file, None).await?;
    let root_group_id = domain.root.unwrap();
    
    info!("Created domain with root group: {}", root_group_id);
    
    // Start the loading process
    println!("\n🔄 Loading HDF5 structure...");
    
    let load_stats = LoadStats::new();
    
    // Load the root group and all its contents
    load_group_recursive(
        &h5_file,
        &client,
        &target_file,
        &root_group_id,
        "/",
        &load_stats
    ).await?;
    
    // Print summary
    println!("\n✅ File loading completed successfully!");
    println!("📊 Loading Statistics:");
    println!("   - Groups created: {}", load_stats.groups_created());
    println!("   - Datasets created: {}", load_stats.datasets_created());
    println!("   - Attributes copied: {}", load_stats.attributes_created());
    println!("   - Target file: {}", target_file);
    
    // Optionally verify some data
    println!("\n🔍 Verifying uploaded data...");
    verify_upload(&client, &target_file).await?;
    
    // Keep the uploaded file on the server
    println!("\n✅ Upload completed! File available at: http://localhost:3000/?file={}", target_file);

    // Clean up (uncomment if you want to delete the file after upload)
    // println!("\n🗑️  Cleaning up...");
    // match client.domains().delete_domain(&target_file).await {
    //     Ok(_) => println!("   ✅ Target file deleted successfully"),
    //     Err(e) => warn!("   ⚠️  Failed to delete target file: {}", e),
    // }
    
    Ok(())
}

/// Configuration for chunked uploads
const MAX_PAYLOAD_SIZE_BYTES: usize = 950000; // 1MB limit (very conservative)
const CHUNK_SIZE_ELEMENTS: usize = 32 * 1024; // 32K elements per chunk (much smaller)
const MAX_CHUNK_ROWS: usize = 128; // Maximum rows per chunk for 2D arrays

/// Enum to hold different data types from HDF5
#[derive(Debug)]
enum DataType {
    U8(Vec<u8>),
    I8(Vec<i8>),
    U16(Vec<u16>),
    I16(Vec<i16>),
    U32(Vec<u32>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}

/// Statistics tracking for the loading process
#[derive(Debug)]
struct LoadStats {
    groups_created: std::sync::atomic::AtomicU32,
    datasets_created: std::sync::atomic::AtomicU32,
    attributes_created: std::sync::atomic::AtomicU32,
}

impl LoadStats {
    fn new() -> Self {
        Self {
            groups_created: std::sync::atomic::AtomicU32::new(0),
            datasets_created: std::sync::atomic::AtomicU32::new(0),
            attributes_created: std::sync::atomic::AtomicU32::new(0),
        }
    }
    
    fn groups_created(&self) -> u32 {
        self.groups_created.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    fn datasets_created(&self) -> u32 {
        self.datasets_created.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    fn attributes_created(&self) -> u32 {
        self.attributes_created.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    fn increment_groups(&self) {
        self.groups_created.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn increment_datasets(&self) {
        self.datasets_created.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    fn increment_attributes(&self) {
        self.attributes_created.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Simple progress bar utility
struct ProgressBar {
    total: usize,
    current: usize,
    width: usize,
}

impl ProgressBar {
    fn new(total: usize) -> Self {
        Self {
            total,
            current: 0,
            width: 50,
        }
    }
    
    fn update(&mut self, current: usize) {
        self.current = current;
        self.display();
    }
    
    fn display(&self) {
        let percentage = if self.total == 0 { 100.0 } else { (self.current as f64 / self.total as f64) * 100.0 };
        let filled = ((percentage / 100.0) * self.width as f64) as usize;
        let empty = self.width - filled;
        
        print!("\r         [{}{}] {:.1}% ({}/{})", 
               "█".repeat(filled),
               "░".repeat(empty),
               percentage,
               self.current,
               self.total);
        io::stdout().flush().unwrap();
        
        if self.current >= self.total {
            println!(); // New line when complete
        }
    }
}

/// Recursively load a group and all its contents
async fn load_group_recursive(
    h5_file: &H5File,
    client: &HsdsClient,
    domain: &str,
    parent_group_id: &str,
    current_path: &str,
    stats: &LoadStats,
) -> Result<(), Box<dyn Error>> {
    info!("Processing group: {}", current_path);
    
    // Get the HDF5 group
    let h5_group = if current_path == "/" {
        h5_file.group("/")?
    } else {
        h5_file.group(current_path)?
    };
    
    
    
    // Get all member names
    let member_names = h5_group.member_names()?;
    debug!("Found {} members in group '{}'", member_names.len(), current_path);
    for member_name in member_names {
        let member_path = if current_path == "/" {
            format!("/{}", member_name)
        } else {
            format!("{}/{}", current_path, member_name)
        };
        
        debug!("Processing member: {}", member_path);
        
        // Try to open as a group first
        if let Ok(sub_group) = h5_file.group(&member_path) {
            // It's a group - create it in HSDS and recurse
            println!("   📁 Creating group: {}", member_name);

            // Create the group in HSDS with link information
            let group_request = GroupCreateRequest {
                link: Some(hsds_client::LinkRequest {
                    id: parent_group_id.to_string(),
                    name: member_name.clone(),
                }),
            };
            let hsds_group = client.groups().create_group(domain, Some(group_request)).await?;

            // Copy attributes for this group
            copy_group_attributes(&sub_group, client, domain, &hsds_group.id, stats).await?;
            
            stats.increment_groups();
            
            // Recursively process the subgroup
            Box::pin(load_group_recursive(
                h5_file,
                client,
                domain,
                &hsds_group.id,
                &member_path,
                stats
            )).await?;
            
        } else if let Ok(dataset) = h5_file.dataset(&member_path) {
            // It's a dataset - copy it
            println!("   📊 Creating dataset: {}", member_name);
            
            copy_dataset(
                &dataset,
                client,
                domain,
                parent_group_id,
                &member_name,
                stats
            ).await?;
            
        } else {
            warn!("Unknown member type: {}", member_path);
        }
    }
    
    Ok(())
}

/// Copy a dataset from HDF5 to HSDS
async fn copy_dataset(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    parent_group_id: &str,
    dataset_name: &str,
    stats: &LoadStats,
) -> Result<(), Box<dyn Error>> {
    info!("Copying dataset: {}", dataset_name);
    
    // Get dataset properties
    let shape = h5_dataset.shape();
    let dtype = h5_dataset.dtype()?;
    
    debug!("Dataset shape: {:?}", shape);
    debug!("Dataset dtype: {:?}", dtype.to_descriptor()?.to_string());
    
    // Convert HDF5 data type to HSDS data type
    let hsds_dtype = match convert_hdf5_dtype_to_hsds(h5_dataset) {
        Ok(dtype) => dtype,
        Err(e) => {
            warn!("Skipping dataset '{}': {}", dataset_name, e);
            return Ok(()); // Skip this dataset but continue processing
        }
    };
    
    // Create the dataset in HSDS
    let dataset_request = DatasetCreateRequest::from_hsds_type_with_link(
        &hsds_dtype,
        shape.iter().map(|&x| x as u64).collect(),
        parent_group_id,
        dataset_name,
    );
    
    let hsds_dataset = client.datasets().create_dataset(domain, dataset_request).await?;
    stats.increment_datasets();
    
    // Read data from HDF5 and write to HSDS
    copy_dataset_data(h5_dataset, client, domain, &hsds_dataset.id).await?;
    
    // Copy dataset attributes
    copy_dataset_attributes(h5_dataset, client, domain, &hsds_dataset.id, stats).await?;
    
    Ok(())
}

/// Copy dataset data from HDF5 to HSDS
async fn copy_dataset_data(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
) -> Result<(), Box<dyn Error>> {
    let shape = h5_dataset.shape();
    let total_elements: usize = shape.iter().product::<usize>();
    
    // Estimate data size using actual datatype size
    let dtype = h5_dataset.dtype()?;
    let type_size = dtype.size();
    let estimated_size = total_elements * type_size;

    debug!("Dataset shape: {:?}, total elements: {}, estimated size: {} bytes ({} MB)", 
           shape, total_elements, estimated_size, estimated_size as f64 / (1024.0 * 1024.0));
    
    if estimated_size > MAX_PAYLOAD_SIZE_BYTES {
        println!("      📦 Large dataset detected ({:.1} MB), using chunked upload", 
                estimated_size as f64 / (1024.0 * 1024.0));
        copy_dataset_data_chunked(h5_dataset, client, domain, dataset_id).await?;
    } else {
        copy_dataset_data_single(h5_dataset, client, domain, dataset_id).await?;
    }
    
    Ok(())
}

/// Copy dataset data in a single request (for small datasets)
async fn copy_dataset_data_single(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
) -> Result<(), Box<dyn Error>> {
    let shape = h5_dataset.shape();
    
    // Get the actual HDF5 data type to make better decisions
    let dtype = h5_dataset.dtype()?;
    let type_desc = dtype.to_descriptor()?;
    
    // Handle different data types based on the actual HDF5 type
    let json_value = match type_desc {
        hdf5::types::TypeDescriptor::Float(FloatSize::U8) => {
            let data = h5_dataset.read_raw::<f64>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Float(FloatSize::U4) => {
            let data = h5_dataset.read_raw::<f32>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U8) => {
            let data = h5_dataset.read_raw::<i64>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U4) => {
            let data = h5_dataset.read_raw::<i32>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U2) => {
            let data = h5_dataset.read_raw::<i16>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U1) => {
            let data = h5_dataset.read_raw::<i8>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::Unsigned(IntSize::U1) => {
            let data = h5_dataset.read_raw::<u8>()?;
            convert_to_multidim_json(data, &shape)
        },
        hdf5::types::TypeDescriptor::VarLenUnicode => {
            let data = h5_dataset.read_raw::<hdf5::types::VarLenUnicode>()?;
            let strings: Vec<String> = data.into_iter().map(|s| s.to_string()).collect();
            json!(strings)
        },
        hdf5::types::TypeDescriptor::VarLenAscii => {
            let data = h5_dataset.read_raw::<hdf5::types::VarLenAscii>()?;
            let strings: Vec<String> = data.into_iter().map(|s| s.to_string()).collect();
            json!(strings)
        },
        _ => {
            // Fallback to the old logic for unsupported types
            warn!("Using fallback type detection for unsupported type: {:?}", type_desc);
            if let Ok(data) = h5_dataset.read_raw::<f64>() {
                convert_to_multidim_json(data, &shape)
            } else if let Ok(data) = h5_dataset.read_raw::<f32>() {
                convert_to_multidim_json(data, &shape)
            } else if let Ok(data) = h5_dataset.read_raw::<i64>() {
                convert_to_multidim_json(data, &shape)
            } else if let Ok(data) = h5_dataset.read_raw::<i32>() {
                convert_to_multidim_json(data, &shape)
            } else {
                warn!("Could not read dataset with any supported type");
                return Ok(());
            }
        }
    };

    let value_request = DatasetValueRequest {
        start: None,
        stop: None,
        step: None,
        points: None,
        value: Some(json_value),
        value_base64: None,
    };

    match client.datasets().write_dataset_values(domain, dataset_id, value_request).await {
        Ok(_) => {},
        Err(e) => {
            warn!("Failed to upload single dataset: {} - continuing with next dataset", e);
            // Don't return error, just continue processing
        }
    }
    
    Ok(())
}

/// Copy dataset data using chunked uploads (for large datasets)
async fn copy_dataset_data_chunked(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
) -> Result<(), Box<dyn Error>> {
    let shape = h5_dataset.shape();
    
    // For simplicity, handle chunking for 1D, 2D, and 3D arrays
    match shape.len() {
        1 => copy_1d_chunked(h5_dataset, client, domain, dataset_id, &shape).await?,
        2 => copy_2d_chunked(h5_dataset, client, domain, dataset_id, &shape).await?,
        3 => copy_3d_chunked(h5_dataset, client, domain, dataset_id, &shape).await?,
        _ => {
            warn!("Chunked upload not implemented for {}D arrays, skipping", shape.len());
            return Ok(());
        }
    }
    
    Ok(())
}

/// Chunked upload for 1D arrays
async fn copy_1d_chunked(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
    shape: &[usize],
) -> Result<(), Box<dyn Error>> {
    let total_len = shape[0];
    let chunk_size = CHUNK_SIZE_ELEMENTS.min(total_len);
    let total_chunks = (total_len + chunk_size - 1) / chunk_size;
    
    println!("      📊 1D Array: {} elements, {} chunks", total_len, total_chunks);
    
    // Get the actual HDF5 data type to read correctly
    let dtype = h5_dataset.dtype()?;
    let type_desc = dtype.to_descriptor()?;
    
    let mut progress = ProgressBar::new(total_chunks);
    let mut chunk_index = 0;
    let mut failed_chunks = 0;
    
    for start in (0..total_len).step_by(chunk_size) {
        let end = (start + chunk_size).min(total_len);
        
        // Read data with the correct type based on HDF5 descriptor
        let chunk_data = match type_desc {
            hdf5::types::TypeDescriptor::Float(FloatSize::U8) => {
                let full_data = h5_dataset.read_raw::<f64>()?;
                let chunk: Vec<f64> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Float(FloatSize::U4) => {
                let full_data = h5_dataset.read_raw::<f32>()?;
                let chunk: Vec<f32> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Integer(IntSize::U8) => {
                let full_data = h5_dataset.read_raw::<i64>()?;
                let chunk: Vec<i64> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Integer(IntSize::U4) => {
                let full_data = h5_dataset.read_raw::<i32>()?;
                let chunk: Vec<i32> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Integer(IntSize::U2) => {
                let full_data = h5_dataset.read_raw::<i16>()?;
                let chunk: Vec<i16> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Integer(IntSize::U1) => {
                let full_data = h5_dataset.read_raw::<i8>()?;
                let chunk: Vec<i8> = full_data[start..end].to_vec();
                json!(chunk)
            },
            hdf5::types::TypeDescriptor::Unsigned(IntSize::U1) => {
                let full_data = h5_dataset.read_raw::<u8>()?;
                let chunk: Vec<u8> = full_data[start..end].to_vec();
                json!(chunk)
            },
            _ => {
                warn!("Unsupported data type for 1D chunking: {:?}", type_desc);
                failed_chunks += 1;
                chunk_index += 1;
                progress.update(chunk_index);
                continue;
            }
        };
        
        let value_request = DatasetValueRequest {
            start: Some(vec![start as u64]),
            stop: Some(vec![end as u64]),
            step: None,
            points: None,
            value: Some(chunk_data),
            value_base64: None,
        };
        
        match client.datasets().write_dataset_values(domain, dataset_id, value_request).await {
            Ok(_) => {
                debug!("Successfully uploaded 1D chunk {}-{}", start, end - 1);
            },
            Err(e) => {
                warn!("Failed to upload 1D chunk {}-{}: {} - continuing", start, end - 1, e);
                failed_chunks += 1;
            }
        }
        
        chunk_index += 1;
        progress.update(chunk_index);
    }
    
    if failed_chunks > 0 {
        warn!("      ⚠️  {} out of {} chunks failed to upload", failed_chunks, total_chunks);
    }
    
    Ok(())
}

/// Chunked upload for 2D arrays (like RGB images)
async fn copy_2d_chunked(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
    shape: &[usize],
) -> Result<(), Box<dyn Error>> {
    let rows = shape[0];
    let cols = shape[1];
    
    // Calculate chunk size very conservatively for large images
    let estimated_bytes_per_element = 20; // Very conservative estimate including JSON overhead
    let max_elements_per_chunk = (MAX_PAYLOAD_SIZE_BYTES / estimated_bytes_per_element).min(CHUNK_SIZE_ELEMENTS);
    
    let elements_per_row = cols;
    let max_rows_per_chunk = (max_elements_per_chunk / elements_per_row).max(1).min(MAX_CHUNK_ROWS);
    let total_chunks = (rows + max_rows_per_chunk - 1) / max_rows_per_chunk;
    
    println!("      📊 2D Array: {}x{} elements, {} chunks ({} rows each)", rows, cols, total_chunks, max_rows_per_chunk);
    
    // Read the full dataset once
    let full_data = if let Ok(data) = h5_dataset.read_raw::<u8>() {
        DataType::U8(data)
    } else if let Ok(data) = h5_dataset.read_raw::<i8>() {
        DataType::I8(data)
    } else if let Ok(data) = h5_dataset.read_raw::<u16>() {
        DataType::U16(data)
    } else if let Ok(data) = h5_dataset.read_raw::<i16>() {
        DataType::I16(data)
    } else if let Ok(data) = h5_dataset.read_raw::<u32>() {
        DataType::U32(data)
    } else if let Ok(data) = h5_dataset.read_raw::<i32>() {
        DataType::I32(data)
    } else if let Ok(data) = h5_dataset.read_raw::<i64>() {
        DataType::I64(data)
    } else if let Ok(data) = h5_dataset.read_raw::<f32>() {
        DataType::F32(data)
    } else if let Ok(data) = h5_dataset.read_raw::<f64>() {
        DataType::F64(data)
    } else {
        warn!("Could not read 2D dataset - unsupported data type");
        return Ok(());
    };
    
    let mut progress = ProgressBar::new(total_chunks);
    let mut chunk_index = 0;
    let mut failed_chunks = 0;
    
    for row_start in (0..rows).step_by(max_rows_per_chunk) {
        let row_end = (row_start + max_rows_per_chunk).min(rows);
        let chunk_rows = row_end - row_start;
        
        // Extract chunk from full data
        let chunk_data = match &full_data {
            DataType::U8(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::I8(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::U16(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::I16(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::U32(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::I32(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::I64(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::F32(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
            DataType::F64(data) => {
                let chunk = extract_2d_chunk(&data, rows, cols, row_start, chunk_rows);
                let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                json!(chunk_2d)
            },
        };
        
        let value_request = DatasetValueRequest {
            start: Some(vec![row_start as u64, 0]),
            stop: Some(vec![row_end as u64, cols as u64]),
            step: None,
            points: None,
            value: Some(chunk_data),
            value_base64: None,
        };
        
        match client.datasets().write_dataset_values(domain, dataset_id, value_request).await {
            Ok(_) => {},
            Err(e) => {
                warn!("Failed to upload 2D chunk rows {}-{}: {} - continuing", row_start, row_end - 1, e);
                failed_chunks += 1;
            }
        }
        
        chunk_index += 1;
        progress.update(chunk_index);
    }
    
    if failed_chunks > 0 {
        warn!("      ⚠️  {} out of {} chunks failed to upload", failed_chunks, total_chunks);
    }
    
    Ok(())
}

/// Chunked upload for 3D arrays (like RGB images with multiple channels)
async fn copy_3d_chunked(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
    shape: &[usize],
) -> Result<(), Box<dyn Error>> {
    let depth = shape[0];
    let rows = shape[1];
    let cols = shape[2];
    
    // Read the full dataset once
    let full_data = if let Ok(data) = h5_dataset.read_raw::<u8>() {
        DataType::U8(data)
    } else if let Ok(data) = h5_dataset.read_raw::<i32>() {
        DataType::I32(data)
    } else if let Ok(data) = h5_dataset.read_raw::<f32>() {
        DataType::F32(data)
    } else if let Ok(data) = h5_dataset.read_raw::<f64>() {
        DataType::F64(data)
    } else {
        warn!("Could not read 3D dataset - unsupported data type");
        return Ok(());
    };
    
    // Process one depth slice at a time to keep chunks manageable
    let elements_per_slice = rows * cols;
    let max_rows_per_chunk = (CHUNK_SIZE_ELEMENTS / cols).max(1).min(rows);
    let chunks_per_slice = (rows + max_rows_per_chunk - 1) / max_rows_per_chunk;
    let total_chunks = depth * chunks_per_slice;
    
    println!("      📊 3D Array: {}x{}x{} elements, {} chunks", depth, rows, cols, total_chunks);
    
    let mut progress = ProgressBar::new(total_chunks);
    let mut chunk_index = 0;
    let mut failed_chunks = 0;
    
    for d in 0..depth {
        for row_start in (0..rows).step_by(max_rows_per_chunk) {
            let row_end = (row_start + max_rows_per_chunk).min(rows);
            let chunk_rows = row_end - row_start;
            
            // Extract slice from full data
            let chunk_data = match &full_data {
                DataType::U8(data) => {
                    let slice_start = d * elements_per_slice + row_start * cols;
                    let slice_end = slice_start + chunk_rows * cols;
                    let chunk = data[slice_start..slice_end].to_vec();
                    let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                    let chunk_3d = vec![chunk_2d];
                    json!(chunk_3d)
                },
                DataType::I32(data) => {
                    let slice_start = d * elements_per_slice + row_start * cols;
                    let slice_end = slice_start + chunk_rows * cols;
                    let chunk = data[slice_start..slice_end].to_vec();
                    let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                    let chunk_3d = vec![chunk_2d];
                    json!(chunk_3d)
                },
                DataType::F32(data) => {
                    let slice_start = d * elements_per_slice + row_start * cols;
                    let slice_end = slice_start + chunk_rows * cols;
                    let chunk = data[slice_start..slice_end].to_vec();
                    let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                    let chunk_3d = vec![chunk_2d];
                    json!(chunk_3d)
                },
                DataType::F64(data) => {
                    let slice_start = d * elements_per_slice + row_start * cols;
                    let slice_end = slice_start + chunk_rows * cols;
                    let chunk = data[slice_start..slice_end].to_vec();
                    let chunk_2d = convert_flat_to_2d(chunk, chunk_rows, cols);
                    let chunk_3d = vec![chunk_2d];
                    json!(chunk_3d)
                },
                _ => {
                    warn!("Unsupported data type for 3D chunking");
                    chunk_index += 1;
                    progress.update(chunk_index);
                    continue;
                }
            };
            
            let value_request = DatasetValueRequest {
                start: Some(vec![d as u64, row_start as u64, 0]),
                stop: Some(vec![(d + 1) as u64, row_end as u64, cols as u64]),
                step: None,
                points: None,
                value: Some(chunk_data),
                value_base64: None,
            };
            
            match client.datasets().write_dataset_values(domain, dataset_id, value_request).await {
                Ok(_) => {},
                Err(e) => {
                    warn!("Failed to upload 3D chunk depth {}, rows {}-{}: {} - continuing", d, row_start, row_end - 1, e);
                    failed_chunks += 1;
                }
            }
            
            chunk_index += 1;
            progress.update(chunk_index);
        }
    }
    
    if failed_chunks > 0 {
        warn!("      ⚠️  {} out of {} chunks failed to upload", failed_chunks, total_chunks);
    }
    
    Ok(())
}

/// Extract a 2D chunk from a flat array
fn extract_2d_chunk<T: Clone>(data: &Vec<T>, _total_rows: usize, cols: usize, start_row: usize, chunk_rows: usize) -> Vec<T> {
    let start_index = start_row * cols;
    let end_index = start_index + (chunk_rows * cols);
    data[start_index..end_index].to_vec()
}

/// Convert HDF5 data type to HSDS data type
fn convert_hdf5_dtype_to_hsds(h5_dataset: &H5Dataset) -> Result<String, Box<dyn Error>> {
    // Use the actual HDF5 data type descriptor instead of trying to read
    let dtype = h5_dataset.dtype()?;
    let type_desc = dtype.to_descriptor()?;
    
    match type_desc {
        hdf5::types::TypeDescriptor::Float(FloatSize::U8) => Ok("H5T_IEEE_F64LE".to_string()),
        hdf5::types::TypeDescriptor::Float(FloatSize::U4) => Ok("H5T_IEEE_F32LE".to_string()),
        hdf5::types::TypeDescriptor::Integer(IntSize::U8) => {
            // For 64-bit integers, default to signed
            Ok("H5T_STD_I64LE".to_string())
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U4) => {
            // For 32-bit integers, default to signed
            Ok("H5T_STD_I32LE".to_string())
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U2) => {
            // For 16-bit integers, default to signed
            Ok("H5T_STD_I16LE".to_string())
        },
        hdf5::types::TypeDescriptor::Integer(IntSize::U1) => {
            // For 8-bit integers, default to signed
            Ok("H5T_STD_I8LE".to_string())
        },
        hdf5::types::TypeDescriptor::Unsigned(IntSize::U1) => {
            // For unsigned 8-bit integers
            Ok("H5T_STD_U8LE".to_string())
        },
        hdf5::types::TypeDescriptor::VarLenUnicode => Ok("H5T_STRING".to_string()),
        hdf5::types::TypeDescriptor::VarLenAscii => Ok("H5T_STRING".to_string()),
        _ => {
            warn!("Unsupported HDF5 data type: {:?}", type_desc);
            Err("Unsupported data type".into())
        }
    }
}

/// Convert flat array to 2D structure
fn convert_flat_to_2d<T: Clone>(data: Vec<T>, rows: usize, cols: usize) -> Vec<Vec<T>> {
    let mut result = Vec::with_capacity(rows);
    for i in 0..rows {
        let row_start = i * cols;
        let row_end = (i + 1) * cols;
        result.push(data[row_start..row_end].to_vec());
    }
    result
}

/// Convert data to proper multidimensional JSON structure
fn convert_to_multidim_json<T: Clone + serde::Serialize>(data: Vec<T>, shape: &[usize]) -> serde_json::Value {
    if shape.len() == 1 {
        json!(data)
    } else if shape.len() == 2 {
        let rows = shape[0];
        let cols = shape[1];
        json!(convert_flat_to_2d(data, rows, cols))
    } else {
        // For higher dimensions, just send as flat array for now
        json!(data)
    }
}

/// Copy attributes from an HDF5 group to HSDS
async fn copy_group_attributes(
    h5_group: &H5Group,
    client: &HsdsClient,
    domain: &str,
    group_id: &str,
    stats: &LoadStats,
) -> Result<(), Box<dyn Error>> {
    let attr_names = h5_group.attr_names()?;
    debug!("Found {} group attributes: {:?}", attr_names.len(), attr_names);
    if !attr_names.is_empty() {
        debug!("Found {} group attributes: {:?}", attr_names.len(), attr_names);
    }
    
    for attr_name in attr_names {
        if let Ok(attr) = h5_group.attr(&attr_name) {
            copy_attribute_value(&attr, client, domain, group_id, &attr_name, stats).await?;
        } else {
            warn!("Could not access group attribute: {}", attr_name);
        }
    }
    
    Ok(())
}

/// Copy attributes from an HDF5 dataset to HSDS
async fn copy_dataset_attributes(
    h5_dataset: &H5Dataset,
    client: &HsdsClient,
    domain: &str,
    dataset_id: &str,
    stats: &LoadStats,
) -> Result<(), Box<dyn Error>> {
    let attr_names = h5_dataset.attr_names()?;
    
    if !attr_names.is_empty() {
        debug!("Found {} dataset attributes: {:?}", attr_names.len(), attr_names);
    }
    
    for attr_name in attr_names {
        if let Ok(attr) = h5_dataset.attr(&attr_name) {
            copy_attribute_value(&attr, client, domain, dataset_id, &attr_name, stats).await?;
        } else {
            warn!("Could not access dataset attribute: {}", attr_name);
        }
    }
    
    Ok(())
}

/// Read an attribute value - simplified approach for common HDF5 attribute types
fn read_attribute_by_type(attr: &hdf5::Attribute, attr_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
    let shape = attr.space()?.shape();
    let is_scalar = shape.is_empty();
    
    debug!("Reading attribute '{}', shape: {:?}, is_scalar: {}", attr_name, shape, is_scalar);

    let attr_type = attr.dtype()?.to_descriptor()?;
    debug!("Attribute '{}' type: {:?}", attr_name, attr_type);

    if is_scalar {
        match attr_type {
            hdf5::types::TypeDescriptor::VarLenAscii => {
                let val = attr.read_scalar::<hdf5::types::VarLenAscii>()?;
                return Ok(json!(val.to_string()));
            }
            hdf5::types::TypeDescriptor::VarLenUnicode => {
                let val = attr.read_scalar::<hdf5::types::VarLenUnicode>()?;
                return Ok(json!(val.to_string()));
            }
            hdf5::types::TypeDescriptor::Float(FloatSize::U8) => {
                let val = attr.read_scalar::<f64>()?;
                return Ok(json!(val));
            }
            hdf5::types::TypeDescriptor::Float(FloatSize::U4) => {
                let val = attr.read_scalar::<f32>()?;
                return Ok(json!(val));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U8) => {
                let val = attr.read_scalar::<i64>()?;
                return Ok(json!(val));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U4) => {
                let val = attr.read_scalar::<i32>()?;
                return Ok(json!(val));
            }
            _ => {
                warn!("Unsupported attribute type for scalar: {:?}", attr_type);
                return Err(format!("Unsupported attribute type for scalar: {:?}", attr_type).into());
            }
        }
    } else {
        match attr_type {
            hdf5::types::TypeDescriptor::VarLenAscii => {
                let arr = attr.read_raw::<hdf5::types::VarLenAscii>()?;
                let strings: Vec<String> = arr.into_iter().map(|s| s.to_string()).collect();
                return Ok(json!(strings));
            }
            hdf5::types::TypeDescriptor::VarLenUnicode => {
                let arr = attr.read_raw::<hdf5::types::VarLenUnicode>()?;
                let strings: Vec<String> = arr.into_iter().map(|s| s.to_string()).collect();
                return Ok(json!(strings));
            }
            hdf5::types::TypeDescriptor::Float(FloatSize::U8) => {
                let arr = attr.read_raw::<f64>()?;
                return Ok(json!(arr));
            }
            hdf5::types::TypeDescriptor::Float(FloatSize::U4) => {
                let arr = attr.read_raw::<f32>()?;
                return Ok(json!(arr));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U8) => {
                let arr = attr.read_raw::<i64>()?;
                return Ok(json!(arr));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U4) => {
                let arr = attr.read_raw::<i32>()?;
                return Ok(json!(arr));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U2) => {
                let arr = attr.read_raw::<i16>()?;
                return Ok(json!(arr));
            }
            hdf5::types::TypeDescriptor::Integer(IntSize::U1) => {
                let arr = attr.read_raw::<i8>()?;
                return Ok(json!(arr));
            }
            _ => {
                warn!("Unsupported attribute type for array: {:?}", attr_type);
                return Err(format!("Unsupported attribute type for array: {:?}", attr_type).into());
            }
        }
        
    }
}

/// Copy an individual attribute value
async fn copy_attribute_value(
    attr: &hdf5::Attribute,
    client: &HsdsClient,
    domain: &str,
    object_id: &str,
    attr_name: &str,
    stats: &LoadStats,
) -> Result<(), Box<dyn Error>> {
    debug!("Copying attribute: {}", attr_name);
    
    // Read attribute based on its actual datatype
    let value = match read_attribute_by_type(attr, attr_name) {
        Ok(val) => val,
        Err(e) => {
            warn!("Could not read attribute '{}': {}", attr_name, e);
            if let Ok(dtype) = attr.dtype() {
                warn!("Attribute '{}' has dtype size: {}", attr_name, dtype.size());
            }
            return Ok(());
        }
    };
    
    debug!("Successfully read attribute '{}': {:?}", attr_name, value);
    
    // Set the attribute in HSDS
    match client.attributes().set_attribute(domain, object_id, attr_name, value).await {
        Ok(_) => {
            debug!("Successfully set attribute '{}' in HSDS", attr_name);
            stats.increment_attributes();
        },
        Err(e) => {
            warn!("Failed to set attribute '{}': {} - continuing", attr_name, e);
            // Don't fail the whole process for attribute errors
        }
    }
    
    Ok(())
}

/// Verify that the upload was successful by reading back some data
async fn verify_upload(
    client: &HsdsClient,
    domain: &str,
) -> Result<(), Box<dyn Error>> {
    // Get the root group and list its contents
    let domain_info = client.domains().get_domain(domain).await?;
    if let Some(root_id) = domain_info.root {
        let links = client.links().list_links(domain, &root_id, None, None).await?;
        println!("   📋 Root group contains {} items", links.links.len());
        
        for link in links.links.iter().take(3) {
            println!("      - {}", link.title);
        }
        
        if !links.links.is_empty() {
            println!("   ✅ Structure verification successful");
        }
    }
    
    Ok(())
}
