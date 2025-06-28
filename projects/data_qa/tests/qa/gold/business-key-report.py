# Databricks notebook source
# DBTITLE 1,1: Loading key definitions from repository
import os
import importlib
import inspect
from pathlib import Path
from library.dataserve.abstract_gold_class import AbstractGoldClass
from pyspark.sql.functions import col, explode

def find_modules_root_path() -> str:
    current_path = os.getcwd()
    project_name = 'DataTeam-Databricks'
    project_root_path_index = current_path.find('DataTeam-Databricks')
    project_root_path = f"{current_path[0:project_root_path_index]}{project_name}/"
    modules_rooth_path = f"{project_root_path}notebooks/gold/modules/"
    return modules_rooth_path


def find_module_package_reference(module_path: str) -> str:
    root_folder_index = str(module_path).find('notebooks')
    relative_path_reference = module_path[root_folder_index:]
    package_reference = relative_path_reference.rstrip('py').rstrip('.').replace('/', '.')
    return package_reference

def get_transformer_definition(full_module_path: str) -> AbstractGoldClass:
    module_path = find_module_package_reference(full_module_path)
    module_path_splits = module_path.split('.')
    package = module_path_splits[-2]
    fact_name = module_path_splits[-1]
    source = 'UC'

    try:
        gold_module = importlib.import_module(module_path)
    except:
        return (source, package, fact_name, ["FAILED"])
    gold_class_list = [
        m
        for m in inspect.getmembers(gold_module, lambda member: inspect.isclass(member))
        if m[1].__module__ == module_path and issubclass(getattr(gold_module, m[0]), AbstractGoldClass)
    ]
    if len(gold_class_list) == 0:
        raise Exception("Gold class not found")
    elif len(gold_class_list) > 1:
        raise Exception("Many gold classes defined in the same module.")
    class_name = gold_class_list[0][0]
    class_obj = getattr(gold_module, class_name)
    instance = class_obj()
    return (source, package, fact_name, instance.get_business_key_columns())

def list_all_fact_definitions(root_path: str):

    modules_path = Path(root_path)
    return [get_transformer_definition(str(p)) for p in modules_path.glob("**/*.py")]



all_definitions = list_all_fact_definitions(find_modules_root_path())

class_business_keys_df = spark.createDataFrame(all_definitions, schema=["source", "package", "table_name", "business_key_column"])\
    .withColumn("business_key_column", explode("business_key_column"))\
    .withColumn("schema_name", col("package"))

class_business_keys_df.createOrReplaceTempView("gold_class_business_keys")
class_business_keys_df.display()

# COMMAND ----------

# DBTITLE 1,Loading lineage definitions
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW column_lineage_view AS
# MAGIC SELECT DISTINCT cl.target_table_catalog, cl.target_table_schema, cl.target_table_name, cl.target_column_name, cl.source_table_catalog, cl.source_table_schema, cl.source_table_name, cl.source_column_name
# MAGIC   FROM system.access.column_lineage cl;
# MAGIC
# MAGIC SELECT * FROM column_lineage_view

# COMMAND ----------

# DBTITLE 1,Final Report
# MAGIC %sql
# MAGIC SELECT ck.*, cl.source_table_name, cl.source_column_name
# MAGIC   FROM gold_class_business_keys ck
# MAGIC   LEFT JOIN column_lineage_view cl
# MAGIC     ON cl.target_table_schema = ck.schema_name
# MAGIC    AND cl.target_table_name = ck.table_name
# MAGIC    AND cl.target_column_name = ck.business_key_column
# MAGIC    AND cl.target_table_catalog IN ('dev_silver_trsf', 'test_silver_trsf')
# MAGIC
