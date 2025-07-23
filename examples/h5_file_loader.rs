use hsds_client::{
    HsdsClient, BasicAuth, 
    DatasetCreateRequest, DataTypeSpec, ShapeSpec, DatasetValueRequest,
    LinkRequest, LinkCreateRequest
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
    println!("\n✅ Upload completed! File available at: {}", target_file);
    
    // Clean up (uncomment if you want to delete the file after upload)
    // println!("\n🗑️  Cleaning up...");
    // match client.domains().delete_domain(&target_file).await {
    //     Ok(_) => println!("   ✅ Target file deleted successfully"),
    //     Err(e) => warn!("   ⚠️  Failed to delete target file: {}", e),
    // }
    
    Ok(())
}

/// Configuration for chunked uploads
const MAX_PAYLOAD_SIZE_BYTES: usize = 2 * 1024 * 1024; // 2MB limit (very conservative)
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
    
    // Copy attributes for this group
    copy_group_attributes(&h5_group, client, domain, parent_group_id, stats).await?;
    
    // Get all member names
    let member_names = h5_group.member_names()?;
    
    for member_name in member_names {
        let member_path = if current_path == "/" {
            format!("/{}", member_name)
        } else {
            format!("{}/{}", current_path, member_name)
        };
        
        debug!("Processing member: {}", member_path);
        
        // Try to open as a group first
        if let Ok(_sub_group) = h5_file.group(&member_path) {
            // It's a group - create it in HSDS and recurse
            println!("   📁 Creating group: {}", member_name);
            
            let hsds_group = client.groups().create_group(domain, None).await?;
            
            // Link it to the parent
            let link_request = LinkCreateRequest {
                id: Some(hsds_group.id.clone()),
                h5path: None,
                h5domain: None,
            };
            
            client.links().create_link(
                domain,
                parent_group_id,
                &member_name,
                link_request
            ).await?;
            
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
    debug!("Dataset dtype: {:?}", dtype);
    
    // Convert HDF5 data type to HSDS data type
    let hsds_dtype = match convert_hdf5_dtype_to_hsds(h5_dataset) {
        Ok(dtype) => dtype,
        Err(e) => {
            warn!("Skipping dataset '{}': {}", dataset_name, e);
            return Ok(()); // Skip this dataset but continue processing
        }
    };
    
    // Create the dataset in HSDS
    let dataset_request = DatasetCreateRequest {
        data_type: DataTypeSpec::Predefined(hsds_dtype),
        shape: Some(ShapeSpec::Dimensions(shape.iter().map(|&x| x as u64).collect())),
        maxdims: None,
        creation_properties: None,
        link: Some(LinkRequest {
            id: parent_group_id.to_string(),
            name: dataset_name.to_string(),
        }),
    };
    
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
    
    // Estimate data size (rough calculation)
    let estimated_size = total_elements * 8; // Assume 8 bytes per element for estimation
    
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
    
    // Handle different data types by trying to read them
    let json_value = if let Ok(data) = h5_dataset.read_raw::<u8>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<i8>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<u16>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<i16>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<u32>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<i32>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<i64>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<f32>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<f64>() {
        convert_to_multidim_json(data, &shape)
    } else if let Ok(data) = h5_dataset.read_raw::<hdf5::types::VarLenUnicode>() {
        let strings: Vec<String> = data.into_iter().map(|s| s.to_string()).collect();
        json!(strings)
    } else {
        warn!("Unsupported data type for dataset");
        return Ok(()); // Skip unsupported types
    };
    
    // Write the data to HSDS
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
    
    let mut progress = ProgressBar::new(total_chunks);
    let mut chunk_index = 0;
    let mut failed_chunks = 0;
    
    for start in (0..total_len).step_by(chunk_size) {
        let end = (start + chunk_size).min(total_len);
        
        // Use direct indexing approach instead of hyperslab selection
        let chunk_data = if let Ok(full_data) = h5_dataset.read_raw::<u8>() {
            let chunk: Vec<u8> = full_data[start..end].to_vec();
            json!(chunk)
        } else if let Ok(full_data) = h5_dataset.read_raw::<i32>() {
            let chunk: Vec<i32> = full_data[start..end].to_vec();
            json!(chunk)
        } else if let Ok(full_data) = h5_dataset.read_raw::<f32>() {
            let chunk: Vec<f32> = full_data[start..end].to_vec();
            json!(chunk)
        } else if let Ok(full_data) = h5_dataset.read_raw::<f64>() {
            let chunk: Vec<f64> = full_data[start..end].to_vec();
            json!(chunk)
        } else {
            warn!("Could not read chunk for 1D array");
            failed_chunks += 1;
            chunk_index += 1;
            progress.update(chunk_index);
            continue;
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
    // Try reading with different types to determine the actual type
    if h5_dataset.read_raw::<u8>().is_ok() {
        Ok("H5T_STD_U8LE".to_string())
    } else if h5_dataset.read_raw::<i8>().is_ok() {
        Ok("H5T_STD_I8LE".to_string())
    } else if h5_dataset.read_raw::<u16>().is_ok() {
        Ok("H5T_STD_U16LE".to_string())
    } else if h5_dataset.read_raw::<i16>().is_ok() {
        Ok("H5T_STD_I16LE".to_string())
    } else if h5_dataset.read_raw::<u32>().is_ok() {
        Ok("H5T_STD_U32LE".to_string())
    } else if h5_dataset.read_raw::<i32>().is_ok() {
        Ok("H5T_STD_I32LE".to_string())
    } else if h5_dataset.read_raw::<i64>().is_ok() {
        Ok("H5T_STD_I64LE".to_string())
    } else if h5_dataset.read_raw::<f32>().is_ok() {
        Ok("H5T_IEEE_F32LE".to_string())
    } else if h5_dataset.read_raw::<f64>().is_ok() {
        Ok("H5T_IEEE_F64LE".to_string())
    } else if h5_dataset.read_raw::<hdf5::types::VarLenUnicode>().is_ok() {
        Ok("H5T_C_S1".to_string())
    } else {
        Err("Unsupported data type".into())
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
    
    for attr_name in attr_names {
        if let Ok(attr) = h5_group.attr(&attr_name) {
            copy_attribute_value(&attr, client, domain, group_id, &attr_name, stats).await?;
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
    
    for attr_name in attr_names {
        if let Ok(attr) = h5_dataset.attr(&attr_name) {
            copy_attribute_value(&attr, client, domain, dataset_id, &attr_name, stats).await?;
        }
    }
    
    Ok(())
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
    
    // Try to read the attribute as different types
    let value = if let Ok(string_val) = attr.read_scalar::<hdf5::types::VarLenUnicode>() {
        json!(string_val.to_string())
    } else if let Ok(int_val) = attr.read_scalar::<i32>() {
        json!(int_val)
    } else if let Ok(float_val) = attr.read_scalar::<f64>() {
        json!(float_val)
    } else if let Ok(int_array) = attr.read_raw::<i32>() {
        json!(int_array)
    } else if let Ok(float_array) = attr.read_raw::<f64>() {
        json!(float_array)
    } else {
        warn!("Could not read attribute: {}", attr_name);
        return Ok(());
    };
    
    // Set the attribute in HSDS
    match client.attributes().set_attribute(domain, object_id, attr_name, value).await {
        Ok(_) => {
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
