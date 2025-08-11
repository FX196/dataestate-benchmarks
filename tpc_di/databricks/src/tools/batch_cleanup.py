# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Cleanup Notebook
# MAGIC This notebook cleans up the ingestion directory after all batches have been processed to save storage space.

# COMMAND ----------

# MAGIC %run ./batch_manager

# COMMAND ----------

# Get parameters from the workflow
job_name = dbutils.widgets.get("job_name")

print(f"Cleaning up ingestion directory for job: {job_name}")

# COMMAND ----------

# Clean up all batch data for this job
try:
    cleanup_batch_ingestion(job_name)
    print(f"Successfully cleaned up ingestion directory for job {job_name}")
    dbutils.notebook.exit(f"SUCCESS: Cleaned up ingestion directory for {job_name}")
    
except Exception as e:
    error_msg = f"FAILED: Error cleaning up ingestion directory: {str(e)}"
    print(error_msg)
    dbutils.notebook.exit(error_msg) 