# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions to Build out the Autoloader Streaming Tables. 
# MAGIC * Use the DLT Pipeline Config Parameters (in the JSON of the DLT Pipeine) to Provide Needed Properties of the table. 
# MAGIC * This makes the job metadata driven

# COMMAND ----------

def build_autoloader_stream(table):
  src_dir = spark.conf.get(f'{table}.path')
  df = spark.readStream.format('cloudFiles') \
      .option('cloudFiles.format', 'csv') \
      .schema(spark.conf.get(f'{table}.schema')) \
      .option("inferSchema", False) \
      .option("delimiter", spark.conf.get(f'{table}.sep')) \
      .option("header", spark.conf.get(f'{table}.header')) \
      .option("pathGlobfilter", spark.conf.get(f'{table}.filename')) \
      .load(f"{spark.conf.get('files_directory')}/sf={spark.conf.get('scale_factor')}/{src_dir}")
  return df

def generate_tables(table_nm):
  @dlt.table(name=table_nm)
  def create_table(): 
    if table_nm in spark.conf.get('tables_with_batchid').replace(" ", "").split(","):
      return build_autoloader_stream(table_nm).selectExpr("*", "cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid")
    else:
      return build_autoloader_stream(table_nm)

# COMMAND ----------

def get_table_filename_groups():
  """Group tables by their filename configuration"""
  raw_tables = spark.conf.get('raw_tables').replace(" ", "").split(",")
  filename_groups = {}
  
  for table in raw_tables:
    try:
      filename = spark.conf.get(f'{table}.filename')
      if filename not in filename_groups:
        filename_groups[filename] = []
      filename_groups[filename].append(table)
    except Exception as e:
      # If filename config doesn't exist, treat as individual table
      filename_groups[f"{table}_individual"] = [table]
  
  return filename_groups

def generate_unified_table(filename, tables):
  """Generate a single DLT table that unions multiple tables with the same filename"""
  if len(tables) == 1:
    # Single table, no union needed
    # table_name = tables[0]
    # @dlt.table(name=table_name)
    # def create_single_table():
    #   return build_autoloader_stream(table_name)
    print(f"skipping table {tables}")
    
  else:
    # Multiple tables, create union
    # Use the filename as the base table name, removing file extension and special characters
    unified_name = filename.replace(".txt", "").replace(".csv", "").replace("*", "").replace("[", "").replace("]", "").replace("(", "").replace(")", "")
    if unified_name.endswith("_"):
      unified_name = unified_name[:-1]
    unified_name = f"unified_{unified_name}"
    
    @dlt.table(name=unified_name)
    def create_unified_table():
      # Create streams for all tables
      streams = []
      for table in tables:
        try:
          streams.append(build_autoloader_stream(table))
        except Exception as e:
          print(f"Warning: Could not create stream for table {table}: {e}")
          continue
      
      if not streams:
        raise Exception(f"No valid streams created for tables: {tables}")
      elif len(streams) == 1:
        from pyspark.sql.functions import coalesce, lit, col
        df = streams[0]
        if "batchid" in df.columns:
          return df.withColumn("batchid", coalesce(col("batchid"), lit(1)))
        else:
          return df.withColumn("batchid", lit(1))
      else:
        from functools import reduce
        from pyspark.sql.functions import coalesce, lit, col
        unified_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), streams)
        if "batchid" in unified_df.columns:
          return unified_df.withColumn("batchid", coalesce(col("batchid"), lit(1)))
        else:
          return unified_df.withColumn("batchid", lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC # Programatically Create Tables using Metadata and looping through required tables
# MAGIC * Most tables use common signature - leverage metadata driven pipeline then to follow the pattern and simplify code

# COMMAND ----------

# DBTITLE 1,Generate All Raw Table Ingestion
for table in spark.conf.get('raw_tables').replace(" ", "").split(","):
  generate_tables(table)

# COMMAND ----------

filename_groups = get_table_filename_groups()

print("Filename groups identified:")
for filename, tables in filename_groups.items():
  print(f"  {filename}: {tables}")

# Generate unified tables for each filename group
for filename, tables in filename_groups.items():
  generate_unified_table(filename, tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FinWire is the Only Fixed Length Text File to ingest so no need for programmatic loop

# COMMAND ----------

@dlt.table(partition_cols=["rectype"])
def FinWire():
  return spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'text') \
    .option("inferSchema", False) \
    .option("pathGlobfilter", spark.conf.get('FinWire.filename')) \
    .load(f"{spark.conf.get('files_directory')}/sf={spark.conf.get('scale_factor')}/{spark.conf.get('FinWire.path')}") \
    .withColumn('rectype', F.substring(F.col('value'), 16, 3))
