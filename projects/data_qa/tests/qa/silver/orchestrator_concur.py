# Databricks notebook source
# MAGIC %md 
# MAGIC ###Defining the pre-requisites for all the steps

# COMMAND ----------

# DBTITLE 1,Pre-requisites for all the steps
core_notebook = "bronze_to_silver"
notebook_execution_timeout = 60 * 60
results = []


# COMMAND ----------

# MAGIC %md ### Run Tests for `concur.expense`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "concur",
    "table_bronze_name": "expense",
    "schema_silver_name": "concur",
    "table_silver_name": "expense",
    "list_pk_cols": "Batch_ID,File_Sequence_Number",
    "list_order_by_cols": "etl_created_datetime",
    "datatypes_definition_file": "/resources/schemas/concur/expense.json"
}
results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `concur.expense_payment_batchlist`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "concur",
    "table_bronze_name": "expense_payment_batchlist",
    "schema_silver_name": "concur",
    "table_silver_name": "expense_payment_batchlist",
    "list_pk_cols": "etl_src_pkeys_hash_key",
    "list_order_by_cols": "etl_created_datetime",
    "datatypes_definition_file": "/resources/schemas/concur/expense_payment_batchlist.json"
}
results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Results

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
