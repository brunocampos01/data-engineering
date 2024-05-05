# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Test Data

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Imports

# COMMAND ----------

import pprint
import json
import os

from IPython.display import (
    display,
    HTML,
)

from library.logger_provider import LoggerProvider
from library.qa.bronze_to_silver_test_data import BronzeToSilverTestData
from library.qa.template_data_tests import TemplateDataTest
from library.datalake_storage import DataLakeStorage
from library.qa.utils import (
    get_list_from_widget,
    get_file_from_widget,
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Prepare Parameters

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Debug
# ---
# execution with MINIMUM parameters necessary
# ---
dbutils.widgets.text("schema_bronze_name", "imos_report_api")
dbutils.widgets.text("table_bronze_name", "md_voyagepnl_estimate")
dbutils.widgets.text("schema_silver_name", "imos_report_api")
dbutils.widgets.text("table_silver_name", "md_voyagepnl_estimate")
dbutils.widgets.text("datatypes_definition_file", '/resources/schemas/imos_report_api/md_voyagepnl_estimate.json')

# ---
# execution with ALL parameters
# ---
# dbutils.widgets.text("schema_bronze_name", "imos_datalake_api")
# dbutils.widgets.text("table_bronze_name", "s4user")
# dbutils.widgets.text("schema_silver_name", "imos_datalake_api")
# dbutils.widgets.text("table_silver_name", "s4user")
dbutils.widgets.text("list_pk_cols", "etl_src_pkeys_hash_key")
dbutils.widgets.text("list_order_by_cols", "etl_created_datetime")
# dbutils.widgets.text("list_skipped_cleansing_rules", "")
# dbutils.widgets.text("list_phone_columns", "userphoneno")
# dbutils.widgets.text("list_email_columns", "useremail")
# dbutils.widgets.text("list_exception_corrupted_records", "be84295dc80c961ec28704fc644d01cb9eaa892e, 1162726b9dda758e8b9bcdb19fa2a2c60fca3daa") # etl_hashkey
# dbutils.widgets.text("datatypes_definition_file", '/resources/schemas/imos_datalake_api/s4user.json')

# COMMAND ----------

# DBTITLE 1,Parameters
logger = LoggerProvider.get_logger()

# uc bronze
schema_bronze_name = dbutils.widgets.get("schema_bronze_name")
table_bronze_name = dbutils.widgets.get("table_bronze_name")

# uc silver
schema_silver_name = dbutils.widgets.get("schema_silver_name")
table_silver_name = dbutils.widgets.get("table_silver_name")

# lists
list_pk_cols = get_list_from_widget(dbutils, "list_pk_cols")
list_skipped_cleansing_rules = get_list_from_widget(dbutils, "list_skipped_cleansing_rules")
list_phone_columns = get_list_from_widget(dbutils, "list_phone_columns")
list_email_columns = get_list_from_widget(dbutils, "list_email_columns")
list_order_by_cols = get_list_from_widget(dbutils, "list_order_by_cols")
list_exception_corrupted_records = get_list_from_widget(dbutils, "list_exception_corrupted_records")
list_pk_cols = get_list_from_widget(dbutils, "list_pk_cols")

# datatypes_definition_file
dict_schema_expected = get_file_from_widget(dbutils, "datatypes_definition_file", is_json=True)

# COMMAND ----------

data_tester = BronzeToSilverTestData(
    spark=spark,
    schema_bronze_name=schema_bronze_name,
    table_bronze_name=table_bronze_name,
    schema_silver_name=schema_silver_name,
    table_silver_name=table_silver_name,
    dict_schema_expected=dict_schema_expected,
    list_pk_cols=list_pk_cols,
    list_order_by_cols=list_order_by_cols,
    list_phone_columns=list_phone_columns,
    list_email_columns=list_email_columns,
    list_skipped_cleansing_rules=list_skipped_cleansing_rules,
    list_exception_corrupted_records=list_exception_corrupted_records,
)
data_tester.log_execution_parameters()
data_tester.validate_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC If it is necessary to debug and see the data, use the `df_transformed_bronze` or `df_silver`. 
# MAGIC
# MAGIC | Bronze      | Silver    |
# MAGIC |-------------|-----------|
# MAGIC | Observed    | Expected  |
# MAGIC | Source      | Target    |

# COMMAND ----------

# DBTITLE 1,Transform Bronze and Silver Dataframes
(
  df_transformed_bronze,
  df_silver,
) = data_tester.get_and_prepare_data()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Execute Data Tests

# COMMAND ----------

validation_results = data_tester.execute_data_tests(
    df_expected=df_silver,
    df_expected_name='silver',
    df_observed=df_transformed_bronze,
    df_observed_name='bronze_transformed',
)

# COMMAND ----------

# DBTITLE 1,Show results on HTML format
html_result_gx = data_tester.generate_results_html(validation_results)
display(HTML(html_result_gx))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results at Azure Blob Storage

# COMMAND ----------

# DBTITLE 1,Saving results
data_tester.save_report_tests_azure_storage(
    html_result_gx=html_result_gx,
    container_name=data_tester.container_adls_name,
    step_layer=data_tester.step_layer,
    schema_target_name=data_tester.schema_silver_name,
    table_target_name=data_tester.table_silver_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,Show results report
report = data_tester.display_results(validation_results)
dbutils.notebook.exit(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
