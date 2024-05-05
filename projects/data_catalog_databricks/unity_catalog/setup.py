# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Defines custom functions to help the creation of unity catalog tables
# MAGIC

# COMMAND ----------

import os
from library.delta_extension.custom_properties import workflow_name_property

# COMMAND ----------

def get_current_notebook_path():
    return dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# COMMAND ----------

print(f"Setup notebook path: {get_current_notebook_path()}")

# COMMAND ----------

def find_schema_name(create_file_path: str):
    """
    Get the schema name from the file path.
    The folder pattern is:
        source/workflow_name/layer_name/table_name.sql

    """
    path_splits = create_file_path.split('/')
    # according workflow folder pattern, the schema(source) can always be found at the position -4 for tables
    # and -3 for function scripts
    if '_schema_functions_' in create_file_path:
        return path_splits[-3]
    return path_splits[-4]

# COMMAND ----------

def find_workflow_name(create_file_path: str):
    """
    Get the workflow name from the file path.
    The folder pattern is:
        source/workflow_name/layer_name/table_name.sql

    """
    path_splits = create_file_path.split('/')
    # function scripts do not belong to workflows
    if '_schema_functions_' in create_file_path:
        return ''

    # according workflow folder pattern, the workflow_name can always be found at the position -3,
    return path_splits[-3]

# COMMAND ----------

def find_table_name(create_file_path: str):
    """
    Get the table name from the file path. Assuming that the file name matches the table_name
    The folder pattern is:
        source/workflow_name/layer_name/table_name.sql
    """
    path_without_extension = os.path.splitext(create_file_path)[0]
    file_name = os.path.basename(path_without_extension)
    return file_name

# COMMAND ----------

def set_workflow_name_property(workflow_name: str, table_name: str):
    """
    Add the custom property on table to identify the workflow
    """
    table_type_row = spark.sql(f'DESCRIBE EXTENDED {table_name}')\
        .filter("col_name = 'Type' and data_type in ('EXTERNAL','VIEW')")\
        .select("data_type").head()

    if table_type_row and table_type_row[0] != 'VIEW':
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES('{workflow_name_property}' = '{workflow_name}');")

# COMMAND ----------

def set_workflow_name_property_for_sql_file(path: str, catalog: str = None):
    """
    Add the workflow name property to a table
    Args:
        path (str): The create table file path
    """
    table_name = find_table_name(path)
    job_name = find_workflow_name(path)
    schema_name = find_schema_name(path)
    table_name = f"{schema_name}.{table_name}"
    if catalog:
        table_name = f"{catalog}.{table_name}"
    #spark.sql(f"USE SCHEMA {schema_name}")
    set_workflow_name_property(job_name, table_name)


# COMMAND ----------

def set_workflow_name_property_for_table(table_name: str):
    """
    Add the workflow name property to a table, when executed from the create table notebook
    Args:
        path (str): The create table file path
    """
    job_name = find_workflow_name(get_current_notebook_path())
    set_workflow_name_property(job_name, table_name)


# COMMAND ----------

def get_current_notebook_job_id():
    import json 
    notebook_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
    try: 
        #The tag jobId does not exists when the notebook is not triggered by dbutils.notebook.run(...) 
        print("tags")
        print(notebook_info["tags"])
        job_id = notebook_info["tags"]["jobId"] 
    except: 
        job_id = -1 

    return job_id

# COMMAND ----------

def find_storage_account():
    """
    Returns:
        str
    """
    scope = os.getenv("KVScope")
    return dbutils.secrets.get(scope, "ADLS-storage-account")

# COMMAND ----------

