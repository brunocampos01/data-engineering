# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold Test Data

# COMMAND ----------

# DBTITLE 1,Auto reload and rehasx
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# DBTITLE 1,Imports
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

# DBTITLE 1,Remove old parameters
dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Debug: Initial Paramaters
# ---
# required parameters
dbutils.widgets.text("sql_database", "") # onpremise, azsql
dbutils.widgets.text("catalog_db_name", "") # XPTOdw
dbutils.widgets.text("schema_db_name", "") # dw
dbutils.widgets.text("table_db_name", "") # fact_voyage_days_agg, fact_cost_center_invoice_lines, fact_vessel_procurement_cumul_snapshot
dbutils.widgets.text("schema_gold_name", "") # dw_commercial, dw_finance, dw_procurement, dw_commercial
dbutils.widgets.text("table_gold_name", "") # fact_voyage_days_agg, fact_ship_procurement_cumul_snapshot
dbutils.widgets.text("table_type", "fact") # fact or dim
# dbutils.widgets.text("query_path", "resources/sql/bi_commercial/facts/fact_voyage_days_agg.sql")  # based on Sql Server => example: resources/sql/bi_procurement/facts/fact_ship_procurement_cumul_snapshot.sql
# dbutils.widgets.text("datatypes_definition_file", 'resources/schemas/bi_commercial/facts/fact_voyage_days_agg.json') # based on Sql Server => example: resources/schemas/bi_procurement/facts/fact_ship_procurement_cumul_snapshot.json
dbutils.widgets.text("dict_rename_cols", '''{
    "vessel": "ship",
    "division": "region"
}''')

# dbutils.widgets.text("dict_rename_cols", '''{
#     "vessel": "ship",
#     "division": "region",
#     "interval_lenght_uom": "interval_length_uom",
#     "interval_lenght_value": "interval_length_value",
#     "pos_type": "type",
#     "pos_status": "status"
# }''')
dbutils.widgets.dropdown("truncate_decimal_values", "False", ["True", "False"])
dbutils.widgets.dropdown("gold_catalog_name", "gold", ["gold", "silver_trsf"])

# ---
# optional parameters
dbutils.widgets.text("col_prefix", "") # fvda, the existing prefix in PK col, if not have it, remove parameter => Example: fvpcs
# dbutils.widgets.text("list_col_keep_prefix", "")
# dbutils.widgets.text("list_skip_cols", "") # skip all tests
# dbutils.widgets.text("list_skip_cols_check_content", "") # skip only the expect_same_content_rows test
# dbutils.widgets.text("list_skip_cols_not_null", "") # skip only the expect_column_values_to_not_be_null test
# dbutils.widgets.text("list_skip_cols_fk_constraint", "") # skip only the expect_col_fk test


# COMMAND ----------

# DBTITLE 1,Parameters
sql_database = dbutils.widgets.get("sql_database")
catalog_db_name = dbutils.widgets.get("catalog_db_name")
schema_db_name = dbutils.widgets.get("schema_db_name")
table_db_name = dbutils.widgets.get("table_db_name")
schema_gold_name = dbutils.widgets.get("schema_gold_name")
table_gold_name = dbutils.widgets.get("table_gold_name")
table_type = dbutils.widgets.get("table_type")
list_col_prefix = get_list_from_widget(dbutils, "col_prefix")
list_col_keep_prefix = get_list_from_widget(dbutils, "list_col_keep_prefix")
query = get_file_from_widget(dbutils, "query_path")
dict_schema_expected = get_file_from_widget(dbutils, "datatypes_definition_file", is_json=True)
list_skip_cols = get_list_from_widget(dbutils, "list_skip_cols")
list_skip_cols_check_content = get_list_from_widget(dbutils, "list_skip_cols_check_content")
dict_rename_cols = dbutils.widgets.get("dict_rename_cols")
list_skip_cols_not_null = get_list_from_widget(dbutils, "list_skip_cols_not_null")
list_skip_cols_fk_constraint = get_list_from_widget(dbutils, "list_skip_cols_fk_constraint")
truncate_decimal_values: bool = "True" == dbutils.widgets.get("truncate_decimal_values")
gold_catalog_name = dbutils.widgets.get("gold_catalog_name")

# COMMAND ----------

data_tester = SilverToGoldTestData(
    spark=spark,
    sql_database=sql_database,
    catalog_db_name=catalog_db_name,
    schema_db_name=schema_db_name,
    table_db_name=table_db_name,
    schema_gold_name=schema_gold_name,
    table_gold_name=table_gold_name,
    table_type=table_type,
    query=query,
    dict_schema_expected=dict_schema_expected,
    list_col_prefix=list_col_prefix,
    list_col_keep_prefix=list_col_keep_prefix,
    list_skip_cols=list_skip_cols,
    list_skip_cols_check_content=list_skip_cols_check_content,
    dict_rename_cols=json.loads(dict_rename_cols),
    list_skip_cols_not_null=list_skip_cols_not_null,
    list_skip_cols_fk_constraint=list_skip_cols_fk_constraint,
    truncate_decimal_values=truncate_decimal_values,
    gold_catalog_name=gold_catalog_name
)
data_tester.log_execution_parameters()
data_tester.validate_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC If it is necessary to debug and see what was received from the sources, use the `df_sql_server` or `df_uc`.
# MAGIC
# MAGIC |   SQL Server  | Unity Catalog |
# MAGIC |---------------|---------------|
# MAGIC | df_sql_server | df_uc         |
# MAGIC | Expected      | Observed      |
# MAGIC | Source        | Target        |

# COMMAND ----------

(
    df_uc,
    df_sql_server,
) = data_tester.get_and_prepare_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Test

# COMMAND ----------

validation_results = data_tester.execute_data_tests(
    df_expected=df_sql_server,
    df_expected_name='sql_database',
    df_observed=df_uc,
    df_observed_name='uc',
)

# COMMAND ----------

html_result_gx = data_tester.generate_results_html(validation_results)
display(HTML(html_result_gx))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Not Null Constraint

# COMMAND ----------

print('uc info_schema:')
data_tester._get_uc_info_schema().display()

print('sql server info_schema:')
data_tester._get_sql_server_info_schema().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Elapsed time to get the same number of records returned
# MAGIC - Sql Server
# MAGIC ```sql
# MAGIC DECLARE @StartTime DATETIME;
# MAGIC DECLARE @EndTime DATETIME;
# MAGIC DECLARE @ElapsedTime INT;
# MAGIC
# MAGIC SET @StartTime = GETDATE();
# MAGIC SELECT * FROM XPTOdw.dw.<TABLE_NAME>;
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
from library.qa.utils import (
    find_df_diff,
    get_common_cols,
    initialize_and_prepare_delta,
)

list_cols = get_common_cols(df_sql_server, df_uc)
df_sql_server_business_cols = df_sql_server.select(*list_cols)
df_uc_business_cols = df_uc.select(*list_cols)

# COMMAND ----------

# df_sql_server.select('vstart').distinct().display()

# COMMAND ----------

# DBTITLE 1,Debug: Check Diff between DFs
# (
#     df_diff_source_target,
#     df_diff_target_source
# ) = find_df_diff(df_source=df_sql_server_business_cols.select('day_start_gmt_date_sk', 'available_SECS_num', 'cargo_days_num'),
#                  df_target=df_uc_business_cols.select('day_start_gmt_date_sk', 'available_SECS_num', 'cargo_days_num'))

# # # UNIT_COST_MT
# # # FUEL_CONSUMPTION_MT
# # # FUEL_COST_MT
# list_cols_order_by = df_diff_source_target.columns

# print(f'All df_diff: (df_sql_server_business_cols - df_uc_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_source_target.distinct().orderBy(*list_cols_order_by).display()
# else:
#     df_diff_source_target.distinct().display()

# print(f'All df_diff: (df_uc_business_cols - df_sql_server_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_target_source.distinct().orderBy(*list_cols_order_by).display()
# else:
#     df_diff_target_source.distinct().display()

# print(f'MAX Value on-prem:')
# df_diff_source_target.agg({'fuel_consumption_mt': 'max'}).display()
# print(f'MIN Value on-prem:')
# df_diff_source_target.agg({'fuel_consumption_mt': 'min'}).display()
# print(f'MAX Value Unity Catalog:')
# df_diff_target_source.agg({'fuel_consumption_mt': 'max'}).display()
# print(f'MIN Value Unity Catalog:')
# df_diff_target_source.agg({'fuel_consumption_mt': 'min'}).display()

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
output = data_tester.path_gold_table + '\n' + report
dbutils.notebook.exit(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
