# Databricks notebook source
# MAGIC %md 
# MAGIC # Orchestrator for BI Transversal: Facts

# COMMAND ----------

core_notebook = "liftshift"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_MARKET_DATA`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts) FACT_MARKET_DATA
dict_dims_relationalships = {
    "DIM_MARKET_DATA": "MARKET_DATA_SK",
    "DIM_TIME": "DATE_SK",
}
execution_parameters = {
    "query_path": "sql/bi_transversal/facts/fact_market_data.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "fact_market_data",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
}
results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_CURRENCY_CONVERSION`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts) FACT_CURRENCY_CONVERSION
dict_dims_relationalships = {
    "DIM_CURRENCY": "CURRENCY_SK",
    "DIM_TIME": "DATE_SK",
}

execution_parameters = {
    "query_path": "sql/bi_transversal/facts/fact_currency_conversion.sql",
    "catalog_name": "csldw",
    "schema_name": "dw",
    "table_name": "fact_currency_conversion",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
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
