# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Procurement: Facts

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_SHIP_INVOICE_LINES`

# COMMAND ----------

# DBTITLE 1,FACT_SHIP_INVOICE_LINES orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "fact_vessel_invoice_lines",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "fact_ship_invoice_lines",
    "table_type": "fact",
    "col_prefix": "fvil",
    "query_path": "resources/sql/bi_procurement/facts/fact_ship_invoice_lines.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/facts/fact_ship_invoice_lines.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_SHIP_PO_LINES`

# COMMAND ----------

# DBTITLE 1,FACT_SHIP_PO_LINES orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "fact_vessel_po_lines",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "fact_ship_po_lines",
    "table_type": "fact",
    "col_prefix": "fvpol",
    "query_path": "resources/sql/bi_procurement/facts/fact_ship_po_lines.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/facts/fact_ship_po_lines.json',
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
