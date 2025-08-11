# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Preparation Notebook
# MAGIC This notebook prepares a specific batch for pipeline processing by copying data from central storage to the ingestion directory.

# COMMAND ----------

# MAGIC %run ./batch_manager

# COMMAND ----------

# Get parameters from the workflow
central_path = dbutils.widgets.get("central_path")
job_name = dbutils.widgets.get("job_name") 
batch_number = int(dbutils.widgets.get("batch_number"))

print(f"Preparing batch {batch_number} for job {job_name}")
print(f"Central data path: {central_path}")

# COMMAND ----------

# Prepare the batch using the batch manager
try:
    ingestion_path = prepare_batch_for_pipeline(central_path, job_name, batch_number)
    print(f"Successfully prepared batch {batch_number}")
    print(f"Batch data copied to: {ingestion_path}")
    
    # Update the pipeline configuration to point to this batch
    # The DLT pipeline will read from the ingestion directory
    dbutils.notebook.exit(f"SUCCESS: Batch {batch_number} prepared at {ingestion_path}")
    
except Exception as e:
    error_msg = f"FAILED: Error preparing batch {batch_number}: {str(e)}"
    print(error_msg)
    dbutils.notebook.exit(error_msg) 