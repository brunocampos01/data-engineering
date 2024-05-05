# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Transversal: Dimensions

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_country`

# COMMAND ----------

# DBTITLE 1,Execute the notebook, passing parameters as reference for the Dimension DIM_Country
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_country",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_country",
    "table_type": "dim",
    "col_prefix": "country",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_country.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_country.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_parties`

# COMMAND ----------

# DBTITLE 1,DIM_PARTIES orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_parties",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_parties",
    "table_type": "dim",
    "col_prefix": "party",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_parties.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_parties.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_party_sites`

# COMMAND ----------

# DBTITLE 1,DIM_PARTY_SITES Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_party_sites",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_party_sites",
    "table_type": "dim",
    "col_prefix": "party_site",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_party_sites.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_party_sites.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_vendor`

# COMMAND ----------

# DBTITLE 1,DIM_VENDOR Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vendor",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_vendor",
    "table_type": "dim",
    "col_prefix": "vendor",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_vendor.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_vendor.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_vendor_sites`

# COMMAND ----------

# DBTITLE 1,DIM_VENDOR_SITES Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vendor_sites",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_vendor_sites",
    "table_type": "dim",
    "col_prefix": "vendor_sites",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_vendor_sites.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_vendor_sites.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_vendor_bank_info`

# COMMAND ----------

# DBTITLE 1,DIM_VENDOR_BANK Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vendor_bank_info",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_vendor_bank_info",
    "table_type": "dim",
    "col_prefix": "vendor_bank",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_vendor_bank_info.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_vendor_bank_info.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_segment_value`

# COMMAND ----------

# DBTITLE 1,DIM_SEGMENT_VALUE Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_segment_value",
    "schema_gold_name": "dw_finance",
    "table_gold_name": "dim_segment_value",
    "table_type": "dim",
    "col_prefix": "segment_value",
    "query_path": "resources/sql/bi_finance/dimensions/dim_segment_value.sql",
    "datatypes_definition_file": 'resources/schemas/bi_finance/dimensions/dim_segment_value.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_pillar`

# COMMAND ----------

# DBTITLE 1,DIM_PILLAR orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_pillar",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_pillar",
    "table_type": "dim",
    "col_prefix": "pillar",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_pillar.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_pillar.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_unit_of_measure`

# COMMAND ----------

# DBTITLE 1,DIM_UNIT_OF_MEASURE Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_unit_of_measure",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "dim_unit_of_measure",
    "table_type": "dim",
    "col_prefix": "uom",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_unit_of_measure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_unit_of_measure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_data_source`

# COMMAND ----------

# DBTITLE 1,DIM_DATA_SOURCE Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_data_source",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_data_source",
    "table_type": "dim",
    "col_prefix": "data_source",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_data_source.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_data_source.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_currency_conversion_mapping`

# COMMAND ----------

# DBTITLE 1,DIM_CURRENCY_CONVERSION_MAPPING Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_currency_conversion_mapping_bak",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "dim_currency_conversion_mapping",
    "table_type": "dim",
    "col_prefix": "conv",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_currency_conversion_mapping.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_currency_conversion_mapping.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_TIME`

# COMMAND ----------

# DBTITLE 1,DIM_TIME Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_time",
    "schema_gold_name": "dw_conformed",
    "table_gold_name": "dim_time",
    "table_type": "dim",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_time.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_time.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_CURRENCY`

# COMMAND ----------

# DBTITLE 1,DIM_CURRENCY Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_currency",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "dim_currency",
    "table_type": "dim",
    "col_prefix": "currency",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_currency.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_currency.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_MARKET_DATA`

# COMMAND ----------

# DBTITLE 1,DIM_MARKET_DATA Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_market_data",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "dim_market_data",
    "table_type": "dim",
    "col_prefix": "market_data",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_market_data.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_market_data.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_MARKET_DATA_TYPE`

# COMMAND ----------

# DBTITLE 1,DIM_MARKET_DATA_TYPE Orchestrator - Execution parameters
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_market_data_type",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "dim_market_data_type",
    "table_type": "dim",
    "col_prefix": "market_data_type",
    "query_path": "resources/sql/bi_transversal/dimensions/dim_market_data_type.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/dimensions/dim_market_data_type.json',
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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
