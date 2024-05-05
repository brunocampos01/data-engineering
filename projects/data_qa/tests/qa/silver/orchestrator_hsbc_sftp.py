# Databricks notebook source
# MAGIC %md 
# MAGIC ###Defining the pre-requisites for all the steps

# COMMAND ----------

# DBTITLE 1,Pre-requisites for all the steps
core_notebook = "bronze_to_silver"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md ### Run Tests for `hsbc_sftp.payment_status`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "hsbc_sftp",
    "table_bronze_name": "payment_status",
    "schema_silver_name": "hsbc_sftp",
    "table_silver_name": "payment_status",
    "list_pk_cols": "CstmrPmtStsRpt_GrpHdr_MsgId, CstmrPmtStsRpt_OrgnlGrpInfAndSts_OrgnlMsgNmId",
    "list_order_by_cols": "CstmrPmtStsRpt_GrpHdr_CreDtTm",
    "datatypes_definition_file": "/resources/schemas/hsbc_sftp/payment_status.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `hsbc_sftp.payment_status_transactions`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "hsbc_sftp",
    "table_bronze_name": "payment_status_transactions",
    "schema_silver_name": "hsbc_sftp",
    "table_silver_name": "payment_status_transactions",
    "list_pk_cols": "CstmrPmtStsRpt_GrpHdr_MsgId, CstmrPmtStsRpt_OrgnlGrpInfAndSts_OrgnlMsgId, CstmrPmtStsRpt_OrgnlPmtInfAndSts_TxInfAndSts_StsId",
    "list_order_by_cols": "CstmrPmtStsRpt_GrpHdr_CreDtTm",
    "datatypes_definition_file": "/resources/schemas/hsbc_sftp/payment_status_transactions.json"
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
