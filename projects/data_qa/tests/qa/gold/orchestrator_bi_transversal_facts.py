# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Transversal: Facts

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_CURRENCY_CONVERSION`

# COMMAND ----------

# DBTITLE 1,FACT_CURRENCY_CONVERSION orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "fact_currency_conversion",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "fact_currency_conversion",
    "table_type": "fact",
    "col_prefix": "fcc",
    "query_path": "resources/sql/bi_transversal/facts/fact_currency_conversion.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/facts/fact_currency_conversion.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_MARKET_DATA`

# COMMAND ----------

# DBTITLE 1,FACT_MARKET_DATA orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "csldw",
    "schema_azure_name": "dw",
    "table_azure_name": "fact_market_data",
    "schema_gold_name": "dw_market_data",
    "table_gold_name": "fact_market_data",
    "table_type": "fact",
    "col_prefix": "fmd",
    "query_path": "resources/sql/bi_transversal/facts/fact_market_data.sql",
    "datatypes_definition_file": 'resources/schemas/bi_transversal/facts/fact_market_data.json',
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
