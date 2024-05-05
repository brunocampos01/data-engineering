# Databricks notebook source
# MAGIC %md ### The main notebook where the tests are implemented

# COMMAND ----------

# Defining the pre-requisites for all the steps
core_notebook = "landing_to_bronze"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md ### Run Tests for HSBC - Payment Status

# COMMAND ----------

execution_parameters = {
    "landing_data": "hsbc/payment_status/",
    "bronze_data": "hsbc/payment_status/delta",
    "primary_keys": "CstmrPmtStsRpt_GrpHdr_MsgId, CstmrPmtStsRpt_OrgnlGrpInfAndSts_OrgnlMsgNmId",
    "datatypes_definition_file": "/resources/schemas/hsbc_sftp/payment_status.json",
    "source_identifier": "hsbc_payment_status",
    "format": "xml",
    "options": '{"wholeText": True}',
    "row_tag": 'Document'
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for HSBC - Payment Status Transactions

# COMMAND ----------

execution_parameters = {
    "landing_data": "hsbc/payment_status_transactions/",
    "bronze_data": "hsbc/payment_status_transactions/delta",
    "primary_keys": "CstmrPmtStsRpt_GrpHdr_MsgId, CstmrPmtStsRpt_OrgnlGrpInfAndSts_OrgnlMsgId, CstmrPmtStsRpt_OrgnlPmtInfAndSts_TxInfAndSts_StsId",
    "datatypes_definition_file": "/resources/schemas/hsbc_sftp/payment_status_transactions.json",
    "source_identifier": "hsbc_payment_status_transactions",
    "format": "xml",
    "options": '{"wholeText": True}',
    "row_tag": 'Document'
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md #### Final

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
dbutils.notebook.exit(f'"{notebook_name} --> {final_text}"')
