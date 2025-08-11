# Databricks notebook source
# Shared configuration for TPC-DI workflow components

# COMMAND ----------

def build_dag_args():
    """
    Build the complete dag_args dictionary needed for workflow generation.
    This function gathers all the required parameters from the global namespace
    and returns a dictionary suitable for Jinja template rendering.
    """
    try:
        # Calculate shuffle partitions if not already done
        total_avail_memory = node_types[worker_node_type]['memory_mb'] if worker_node_count == 0 else node_types[worker_node_type]['memory_mb']*worker_node_count
        total_cores = node_types[worker_node_type]['num_cores'] if worker_node_count == 0 else node_types[worker_node_type]['num_cores']*worker_node_count
        shuffle_partitions = int(total_cores * max(1, shuffle_part_mult * scale_factor / total_avail_memory))
    except NameError: 
        dbutils.notebook.exit(f"This notebook cannot be executed standalone and MUST be called from the workflow_builder notebook!")

    return {
        "wh_target": wh_target, 
        "tpcdi_directory": tpcdi_directory, 
        "scale_factor": scale_factor, 
        "job_name": job_name,
        "repo_src_path": repo_src_path,
        "cloud_provider": cloud_provider,
        "worker_node_type": worker_node_type,
        "driver_node_type": driver_node_type,
        "worker_node_count": worker_node_count,
        "dbr": dbr_version_id,
        "shuffle_partitions": shuffle_partitions
    }

def build_batch_dag_args(batch_number, batch_ingestion_path):
    """
    Build dag_args for batch-specific pipeline generation.
    
    Args:
        batch_number (int): Current batch number being processed
        batch_ingestion_path (str): Path to the batch-specific ingestion directory
    
    Returns:
        dict: Configuration dictionary for batch pipeline templates
    """
    base_args = build_dag_args()
    
    # Add batch-specific parameters
    base_args.update({
        "current_batch_number": batch_number,
        "batch_ingestion_path": batch_ingestion_path,
        "job_name": f"{job_name}-batch-{batch_number}",  # Make job name batch-specific
    })
    
    return base_args

def get_data_generation_config():
    """
    Get only the configuration needed for data generation.
    """
    return {
        "tpcdi_directory": tpcdi_directory,
        "scale_factor": scale_factor
    }

def get_batch_ingestion_base_path():
    """
    Get the base path for batch ingestion directories.
    """
    return "/tmp/tpcdi/ingestion"

# COMMAND ----------

# Batch processing helper functions
def prepare_all_batches_for_job(central_path, job_name):
    """
    Prepare all available batches for a job.
    
    Args:
        central_path (str): Path to central data directory
        job_name (str): Name of the job
    
    Returns:
        list: List of prepared batch paths
    """
    from tools.batch_manager import get_available_batches, prepare_batch_for_pipeline
    
    batches = get_available_batches(central_path)
    prepared_batches = []
    
    for batch_num in batches:
        try:
            batch_path = prepare_batch_for_pipeline(central_path, job_name, batch_num)
            prepared_batches.append({
                "batch_number": batch_num,
                "batch_path": batch_path
            })
            print(f"Prepared batch {batch_num}: {batch_path}")
        except Exception as e:
            print(f"Failed to prepare batch {batch_num}: {str(e)}")
    
    return prepared_batches 