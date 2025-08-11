# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run ./file_utils

# COMMAND ----------

def get_available_batches(central_path):
    """Discover available batch directories in central data location."""
    dbfs_path = f"/dbfs{central_path}"
    if not os.path.exists(dbfs_path):
        return []
    
    batches = []
    for item in os.listdir(dbfs_path):
        if item.startswith("Batch") and os.path.isdir(os.path.join(dbfs_path, item)):
            batch_num = int(item.replace("Batch", ""))
            batches.append(batch_num)
    
    return sorted(batches)

def get_ingestion_path(job_name, batch_number):
    """Generate ingestion path for a specific job and batch."""
    return f"/tmp/tpcdi/ingestion/{job_name}"

def copy_batch_to_ingestion(central_path, job_name, batch_number):
    """Copy batch data from central location to job-specific ingestion path."""
    source_path = f"{central_path}/Batch{batch_number}"
    target_base_path = get_ingestion_path(job_name, batch_number)
    target_path = f"{target_base_path}/Batch{batch_number}"
    
    print(f"Copying batch {batch_number} from {source_path} to {target_path}")
    
    # Use copy_directory function from file_utils
    try:
        result = copy_directory(f"/dbfs{source_path}", f"/dbfs{target_path}", overwrite=True)
        return result is not None
    except Exception as e:
        print(f"Error copying batch {batch_number}: {str(e)}")
        return False

def cleanup_batch_ingestion(job_name, batch_number=None):
    """Clean up ingestion directory for job (specific batch or all batches)."""
    if batch_number:
        target_path = f"/dbfs{get_ingestion_path(job_name, batch_number)}/Batch{batch_number}"
    else:
        target_path = f"/dbfs/tmp/tpcdi/ingestion/{job_name}"
    
    if os.path.exists(target_path):
        import shutil
        shutil.rmtree(target_path)
        print(f"Cleaned up: {target_path}")

def prepare_batch_for_pipeline(central_path, job_name, batch_number):
    """Main function to prepare a batch for pipeline processing."""
    # Check if batch exists
    available_batches = get_available_batches(central_path)
    if batch_number not in available_batches:
        raise ValueError(f"Batch {batch_number} not found. Available: {available_batches}")
    
    # Copy batch data
    success = copy_batch_to_ingestion(central_path, job_name, batch_number)
    if not success:
        raise RuntimeError(f"Failed to copy batch {batch_number}")
    
    return get_ingestion_path(job_name, batch_number)

# COMMAND ----------

# Example usage:
# ingestion_path = prepare_batch_for_pipeline("/tmp/tpcdi/sf=10", "my-job", 1)
# cleanup_batch_ingestion("my-job", 1)  # Clean up specific batch
# cleanup_batch_ingestion("my-job")     # Clean up all batches for job 