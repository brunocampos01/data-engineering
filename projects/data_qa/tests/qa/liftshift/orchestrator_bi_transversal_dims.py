# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Transversal: Dimensions

# COMMAND ----------

core_notebook = "liftshift"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_CURRENCY`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_currency.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_currency",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_PARTIES`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_parties.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_parties",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_PARTY_SITES`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_party_sites.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_party_sites",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_VENDOR`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_vendor.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_vendor",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_SEGMENT_VALUE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_segment_value.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_segment_value",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_VENDOR_BANK_INFO`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_vendor_bank_info.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_vendor_bank_info",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_VENDOR_SITES`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_vendor_sites.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_vendor_sites",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_MARKET_DATA`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_market_data.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_market_data",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_MARKET_DATA_TYPE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_market_data_type.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_market_data_type",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DW_DIM_UNIT_OF_MEASURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/bi_transversal/dimensions/dim_unit_of_measure.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "dim_unit_of_measure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
dbutils.notebook.exit(f'"{notebook_name} --> {final_text}"')
