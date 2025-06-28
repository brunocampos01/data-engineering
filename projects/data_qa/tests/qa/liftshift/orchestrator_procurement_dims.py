# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for Procurement: Dimensions (Datawarehouse Shipsure)

# COMMAND ----------

core_notebook = "liftshift"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_ACCOUNT_SHIPSURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_account_shipsure.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_account_shipsure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_CONTACT_SHIPSURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_contact_shipsure.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_contact_shipsure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_DEPARTMENT_SHIPSURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_department_shipsure.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_department_shipsure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_JOB_CLASS_SHIPSURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_job_class_shipsure.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_job_class_shipsure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_MAINTENANCE_JOB`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_maintenance_job.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_maintenance_job",
    "table_type": "dim",
    "list_deny_cols": "['MAINT_JOB_CANCEL_DATE']"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_PROCUREMENT_ITEM_TYPE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_procurement_item_type.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_procurement_item_type",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_PROCUREMENT_STATUS`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_procurement_status.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_procurement_status",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_REFERENCE_CODE` (Datawarehouse - XPTO)

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_reference_code.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_reference_code",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_RESCHEDULED_REASON`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_rescheduled_reason.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_rescheduled_reason",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_VESSEL_COMPONENT`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_vessel_component.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_vessel_component",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_VESSEL_INVOICE_HEADER`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_vessel_invoice_header.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_vessel_invoice_header",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_VESSEL_PART`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_vessel_part.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_vessel_part",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_VESSEL_PO_HEADER`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_vessel_po_header.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_vessel_po_header",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_VESSEL_SHIPSURE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_vessel_shipsure.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_vessel_shipsure",
    "table_type": "dim",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `DIM_WORK_ORDER_TYPE`

# COMMAND ----------

execution_parameters = {
    "query_path": "sql/procurement/dimensions/dim_work_order_type.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "dim_work_order_type",
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
