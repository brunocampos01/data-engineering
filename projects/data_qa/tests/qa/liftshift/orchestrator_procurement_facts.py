# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for Procurement: Facts (Datawarehouse Shipsure)

# COMMAND ----------

core_notebook = "liftshift"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_PROCUREMENT_STATUS": "PROC_STATUS_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_enqr_suppl_cumul_snapshot.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_enqr_suppl_cumul_snapshot",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_ENQUIRY_LINES`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_PART": "VESSEL_PART_SK",
    "DIM_VESSEL_COMPONENT": "VESSEL_COMP_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_PROCUREMENT_STATUS": "PROC_STATUS_SK",
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_enquiry_lines.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_enquiry_lines",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_INVOICE_COMPARATOR`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_invoice_comparator.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_invoice_comparator",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_INVOICE_LINES`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_INVOICE_HEADER": "VESSEL_INV_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_invoice_lines.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_invoice_lines",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_PO_LINES_AGG`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_PART": "VESSEL_PART_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_po_lines_agg.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_po_lines_agg",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_PO_LINES`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_PART": "VESSEL_PART_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_po_lines.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_po_lines",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_PROCUREMENT_AGG`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_INVOICE_HEADER": "VESSEL_INV_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_CURRENCY": "CURRENCY_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_procurement_agg.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_procurement_agg",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_PROCUREMENT_STATUS": "PROC_STATUS_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_procurement_cumul_snapshot.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_procurement_cumul_snapshot",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_REQUISITION_LINES`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_PO_HEADER": "VESSEL_PO_SK",
    "DIM_VESSEL_PART": "VESSEL_PART_SK",
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_CURRENCY": "CURRENCY_SK",
    "DIM_VESSEL_COMPONENT": "VESSEL_COMP_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_CONTACT_SHIPSURE": "CONTACT_SS_SK",
    "DIM_PROCUREMENT_ITEM_TYPE": "PROC_ITEM_TYPE_SK",
    "DIM_ACCOUNT_SHIPSURE": "ACCOUNT_SS_SK",
    "DIM_PROCUREMENT_STATUS": "PROC_STATUS_SK",
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_requisition_lines.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_requisition_lines",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_WORK_ORDER_DUE_AGG_SNAPSHOT`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_DEPARTMENT_SHIPSURE": "DEPT_SS_SK",
    "DIM_JOB_CLASS_SHIPSURE": "JOB_CLASS_SS_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_WORK_ORDER_TYPE": "WO_TYPE_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_work_order_due_agg_snapshot.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_work_order_due_agg_snapshot",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VESSEL_WORK_ORDER`

# COMMAND ----------

# dim name: PK name
# dims inside the query (related with facts)
dict_dims_relationalships = {
    "DIM_VESSEL_SHIPSURE": "VESSEL_SS_SK",
    "DIM_VESSEL_COMPONENT": "VESSEL_COMP_SK",
    "DIM_MAINTENANCE_JOB": "MAINT_JOB_SK",
    "DIM_REGION": "REGION_SK",
    "DIM_WORK_ORDER_TYPE": "WO_TYPE_SK",
    "DIM_RESCHEDULED_REASON": "RESCH_REASON_SK"
}

execution_parameters = {
    "query_path": "sql/procurement/facts/fact_vessel_work_order.sql",
    "catalog_name": "XPTOdw",
    "schema_name": "dw",
    "table_name": "fact_vessel_work_order",
    "table_type": "fact",
    "dict_dims_relationalships": str(dict_dims_relationalships),
    "skip_dim_check": True
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
