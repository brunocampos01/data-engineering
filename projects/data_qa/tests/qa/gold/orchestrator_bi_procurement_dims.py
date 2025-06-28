# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Procurement: Dimensions

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_contact_shipsure`

# COMMAND ----------

# DBTITLE 1,DIM_PARTIES orchestrator - Execution part
execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_contact_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_contact_shipsure",
    "table_type": "dim",
    "col_prefix": "contact_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_contact_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_contact_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_department_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_department_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_department_shipsure",
    "table_type": "dim",
    "col_prefix": "dept_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_department_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_department_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_job_class_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_job_class_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_job_class_shipsure",
    "table_type": "dim",
    "col_prefix": "job_class_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_job_class_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_job_class_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_procurement_item_type`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_procurement_item_type",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_procurement_item_type",
    "table_type": "dim",
    "col_prefix": "proc_item_type",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_procurement_item_type.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_procurement_item_type.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_reference_code`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_reference_code",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_reference_code",
    "table_type": "dim",
    "col_prefix": "ref_code",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_reference_code.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_reference_code.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_component`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vessel_component",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_component",
    "table_type": "dim",
    "col_prefix": "vessel_comp",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_component.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_component.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_part`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vessel_part",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_part",
    "table_type": "dim",
    "col_prefix": "vessel_part",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_part.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_part.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_shipsure`

# COMMAND ----------

# DBTITLE 1,Execute the notebook, passing parameters as reference for the Dimension DIM_Country
execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_vessel_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_shipsure",
    "table_type": "dim",
    "col_prefix": "vessel_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_work_order_type`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_work_order_type",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_work_order_type",
    "table_type": "dim",
    "col_prefix": "wo_type",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_work_order_type.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_work_order_type.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_job_class_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_job_class_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_job_class_shipsure",
    "table_type": "dim",
    "col_prefix": "job_class_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_job_class_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_job_class_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_rescheduled_reason`

# COMMAND ----------

execution_parameters = {
    "catalog_azure_name": "XPTOdw",
    "schema_azure_name": "dw",
    "table_azure_name": "dim_rescheduled_reason",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_rescheduled_reason",
    "table_type": "dim",
    "col_prefix": "resch_reason, resch",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_rescheduled_reason.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_rescheduled_reason.json',
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
