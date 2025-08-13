# Databricks notebook source
import os
import shutil

# COMMAND ----------

def move_file(source_location, target_location):
    """Move a file using dbutils.fs.cp"""
    dbutils.fs.cp(source_location, target_location) 
    return f"Finished moving {source_location} to {target_location}"

def copy_directory(source_dir, target_dir, overwrite):
    """Copy directory from source to target with optional overwrite."""
    if os.path.exists(target_dir) and overwrite:
        print(f"Overwrite set to true. Deleting: {target_dir}.")
        shutil.rmtree(target_dir)
        print(f"Deleted {target_dir}.")
    try:
        print(f"Copying {source_dir} to {target_dir}")
        dst = shutil.copytree(source_dir, target_dir)
        print(f"Copied {source_dir} to {target_dir} succesfully!")
        return dst
    except FileExistsError:
        print(f"The folder you're trying to write to exists. Please delete it or set overwrite=True.")
    except FileNotFoundError:
        print(f"The folder you're trying to copy doesn't exist: {source_dir}")

# COMMAND ----------

# This file contains only utility functions with no side effects
# Safe to import via %run from other notebooks 
