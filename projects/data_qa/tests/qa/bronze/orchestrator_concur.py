# Databricks notebook source
# MAGIC %md ### The main notebook where the tests are implemented

# COMMAND ----------

# Defining the pre-requisites for all the steps
core_notebook = "landing_to_bronze"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md ### Run Tests for CONCUR Journal Transaction expense

# COMMAND ----------

execution_parameters = {
    "landing_data": "concur/journal_transaction/expense",
    "bronze_data": "concur/journal_transaction/expense/delta",
    "primary_keys": "Batch_ID, File_Sequence_Number, Primary_Ledger",
    "datatypes_definition_file": "/resources/schemas/concur/expense.json",
    "source_identifier": "concur_expense",
    "format": "csv",
    "options": '{"header": "true", "encoding": "UTF-8", "sep": ",", "mode": "PERMISSIVE", "dateFormat": "MM/dd/yyyy"}'
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
dbutils.notebook.exit(f'"{notebook_name} --> {final_text}"')
