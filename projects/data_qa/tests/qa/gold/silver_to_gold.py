# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold Test Data

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import json
import os

from IPython.display import (
    display,
    HTML,
)

from library.qa.silver_to_gold_test_data import SilverToGoldTestData
from library.qa.utils import (
    get_file_from_widget,
    get_list_from_widget,
    get_dict_from_widget,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Parameters

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Debug: Initial Paramaters
dbutils.widgets.text("catalog_azure_name", "csldw")
dbutils.widgets.text("schema_azure_name", "dw")
dbutils.widgets.text("table_azure_name", "dim_vessel_po_header")
dbutils.widgets.text("schema_gold_name", "dw_procurement")
dbutils.widgets.text("table_gold_name", "dim_ship_po_header")
dbutils.widgets.text("table_type", "dim")
dbutils.widgets.text("query_path", "resources/sql/bi_procurement/dimensions/dim_ship_po_header.sql")  # based on azure
dbutils.widgets.text("datatypes_definition_file", 'resources/schemas/bi_procurement/dimensions/dim_ship_po_header.json') # based on azure

# optional parameters
dbutils.widgets.text("col_prefix", "vessel_po") # the existing prefix in PK column, if not have it, remove parameter
# dbutils.widgets.text("list_skip_cols", "detail_flag, enabled_flag")

# COMMAND ----------

# DBTITLE 1,Parameters
catalog_azure_name = dbutils.widgets.get("catalog_azure_name")
schema_azure_name = dbutils.widgets.get("schema_azure_name")
table_azure_name = dbutils.widgets.get("table_azure_name")
schema_gold_name = dbutils.widgets.get("schema_gold_name")
table_gold_name = dbutils.widgets.get("table_gold_name")
table_type = dbutils.widgets.get("table_type")
list_col_prefix = get_list_from_widget(dbutils, "col_prefix")
query = get_file_from_widget(dbutils, "query_path")
dict_schema_expected = get_file_from_widget(dbutils, "datatypes_definition_file", is_json=True)
list_skip_cols = get_list_from_widget(dbutils, "list_skip_cols")

# COMMAND ----------

data_tester = SilverToGoldTestData(
    spark=spark,
    catalog_azure_name=catalog_azure_name,
    schema_azure_name=schema_azure_name,
    table_azure_name=table_azure_name,
    schema_gold_name=schema_gold_name,
    table_gold_name=table_gold_name,
    table_type=table_type,
    query=query,
    dict_schema_expected=dict_schema_expected,
    list_col_prefix=list_col_prefix,
    list_skip_cols=list_skip_cols,
)
data_tester.log_execution_parameters()
data_tester.validate_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC If it is necessary to debug and see what was received from the sources, use the `df_azure` or `df_uc`.
# MAGIC
# MAGIC | SQL Server  | Unity Catalog |
# MAGIC |-------------|-----------|
# MAGIC | df_azure    | df_uc     |
# MAGIC | Expected    | Observed  |
# MAGIC | Source      | Target    |

# COMMAND ----------

(
    df_uc,
    df_azure,
) = data_tester.get_and_prepare_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Test

# COMMAND ----------

validation_results = data_tester.execute_data_tests(
    df_expected=df_azure,
    df_expected_name='azure',
    df_observed=df_uc,
    df_observed_name='uc',
)

# COMMAND ----------

html_result_gx = data_tester.generate_results_html(validation_results)
display(HTML(html_result_gx))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation of non-null constraints

# COMMAND ----------

data_tester._get_uc_not_null_constraint()
data_tester._get_sqlserver_not_null_constraint()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elapsed time to get the same number of records returned
# MAGIC - Azure
# MAGIC ```sql
# MAGIC DECLARE @StartTime DATETIME;
# MAGIC DECLARE @EndTime DATETIME;
# MAGIC DECLARE @ElapsedTime INT;
# MAGIC
# MAGIC SET @StartTime = GETDATE();
# MAGIC SELECT * FROM csldw.dw.<TABLE_NAME>;
# MAGIC SET @EndTime = GETDATE();
# MAGIC
# MAGIC SET @ElapsedTime = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
# MAGIC
# MAGIC SELECT 
# MAGIC 	@StartTime AS StartTime, 
# MAGIC 	@EndTime AS EndTime,
# MAGIC 	@ElapsedTime AS 'ms';
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Analysis / Debug
# MAGIC - Use these cells to analyze the dataframes, schemas, data_tests, diffs
# MAGIC - Use only to debug! Avoid keep this cell related the debug uncommented because some functions consume a lot of time tu run.

# COMMAND ----------

# DBTITLE 1,Debug: Data Analysis
# from library.qa.utils import (
#     find_df_diff,
#     get_common_cols,
# )

# list_cols = get_common_cols(df_azure, df_uc)
# df_azure_business_cols = df_azure.select(*list_cols)
# df_uc_business_cols = df_uc.select(*list_cols)

# COMMAND ----------

# DBTITLE 1,Debug: Check Diff between DFs
# (
#     df_diff_source_target,
#     df_diff_target_source
# ) = find_df_diff(df_source=df_azure_business_cols.select('desc'), df_target=df_uc_business_cols.select('desc'))

# list_cols_order_by = df_diff_source_target.columns

# print(f'All df_diff: (df_azure_business_cols - df_uc_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_source_target.orderBy(*list_cols_order_by).display()
# else:
#     df_diff_source_target.display()

# print(f'All df_diff: (df_uc_business_cols - df_azure_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_target_source.orderBy(*list_cols_order_by).display()
# else:
#     df_diff_target_source.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results at Azure Blob Storage

# COMMAND ----------

data_tester.save_report_tests_azure_storage(
    html_result_gx=html_result_gx,
    container_name=data_tester.container_adls_name,
    step_layer=data_tester.step_layer,
    schema_target_name=data_tester.schema_gold_name,
    table_target_name=data_tester.table_gold_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

report = data_tester.display_results(validation_results)
dbutils.notebook.exit(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
