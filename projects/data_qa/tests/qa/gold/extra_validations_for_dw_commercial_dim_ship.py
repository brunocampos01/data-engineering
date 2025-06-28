# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache

# COMMAND ----------

import json
import os

from IPython.display import (
    display,
    HTML,
)

from library.great_expectations.util import add_custom_result_to_validation
from library.qa.great_expectations_helper import GreatExpectationsHelper
from library.qa.silver_to_gold_test_data import SilverToGoldTestData
from library.qa.utils import (
    get_file_from_widget,
    get_list_from_widget,
    get_dict_from_widget,
)

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, abs,coalesce, lit
from pyspark.sql.types import DecimalType

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# ---
# required parameters
dbutils.widgets.text("sql_database", "onpremise")
dbutils.widgets.text("catalog_db_name", "XPTOdw")
dbutils.widgets.text("schema_db_name", "dw")
dbutils.widgets.text("table_db_name", "dim_vessel")
dbutils.widgets.text("schema_gold_name", "dw_commercial")
dbutils.widgets.text("table_gold_name", "dim_ship")
dbutils.widgets.text("table_type", "dim")
dbutils.widgets.text("query_path", "resources/sql/bi_commercial/dimensions/dim_ship.sql")  # based on Sql Server
dbutils.widgets.text("datatypes_definition_file", 'resources/schemas/bi_commercial/dimensions/dim_ship.json') # based on Sql Server
dbutils.widgets.text("dict_rename_cols", '''{
    "vessel": "ship",
    "division": "region",
    "interval_lenght_uom": "interval_length_uom",
    "interval_lenght_value": "interval_length_value"
}''')

dbutils.widgets.text("business_key_column", "code")
dbutils.widgets.text("col_list_to_be_validated", "beam,capacity_bale,capacity_grain,depth,draft")

# ---
# optional parameters
dbutils.widgets.text("col_prefix", "vessel") # the existing prefix in PK col, if not have it, remove parameter
# dbutils.widgets.text("list_col_keep_prefix", "")
# dbutils.widgets.text("list_skip_cols", "") # skip all tests
# dbutils.widgets.text("list_skip_cols_check_content", "delete_or_inactive_date") # skip only the expect_same_content_rows test
# dbutils.widgets.text("list_skip_cols_not_null", "completed_date_sk, resch_date_sk, resch_reason_sk, started_date_sk") # skip only the expect_column_values_to_not_be_null test
# dbutils.widgets.text("list_skip_cols_fk_constraint", "expected_delivery_date_sk, invoice_date_sk, order_date_sk, received_date_sk, requested_delivery_date_sk") # skip only the expect_col_fk test

# COMMAND ----------

def comma_values_to_list(values: str):
    return [x.strip() for x in values.split(",") if x]

# COMMAND ----------

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
business_key_column = dbutils.widgets.get("business_key_column")
col_list_str = dbutils.widgets.get("col_list_to_be_validated")
col_list_to_be_validated = order_by_columns_list = comma_values_to_list(col_list_str)

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
)
data_tester.log_execution_parameters()
data_tester.validate_parameters()

# COMMAND ----------

(
    df_uc,
    df_sql_server,
) = data_tester.get_and_prepare_data()

# COMMAND ----------

df_validation = df_sql_server.select([business_key_column] + col_list_to_be_validated)

for i in range(len(col_list_to_be_validated)):
    df_validation = df_validation.withColumnRenamed(
        col_list_to_be_validated[i],
        'sql_' + col_list_to_be_validated[i])

df_observed = df_uc.select([business_key_column] + col_list_to_be_validated)\
              .join(df_validation , on = [f'{business_key_column}'], how = 'left')


for column_name  in col_list_to_be_validated:
    df_observed = df_observed\
        .withColumn(f'{column_name}', coalesce(col(f'{column_name}'), lit(0)))\
        .withColumn(f'sql_{column_name}', coalesce(col(f'sql_{column_name}'), lit(0)))\
        .withColumn(f'{column_name}',abs(col(f'{column_name}') - col(f'sql_{column_name}')))

expectations = ExpectationSuite(expectation_suite_name=f"extra-validation-silver-to-gold-dim-ship")
gdf_observed = SparkDFDataset(df_observed)

for column_name  in col_list_to_be_validated:
    for f in df_observed.schema.fields:
        if isinstance(f.dataType, DecimalType) and f.name == column_name:
            [precision,scale]  = [f.dataType.precision, f.dataType.scale]

    max_value = (10 ** - scale)
    exp = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_between",
        kwargs={
            'column': f'{column_name}',
            'min_value': 0,
            'max_value': max_value
        },
    )
    expectations.add_expectation(exp)

validation_results = gdf_observed.validate(
    expectation_suite=expectations,
    catch_exceptions=False,
    result_format='BASIC',
)

# COMMAND ----------

html_result_gx = data_tester.generate_results_html(validation_results)
display(HTML(html_result_gx))

# COMMAND ----------

data_tester.save_report_tests_azure_storage(
    html_result_gx=html_result_gx,
    container_name=data_tester.container_adls_name,
    step_layer=data_tester.step_layer,
    schema_target_name=data_tester.schema_gold_name,
    table_target_name=f'extra_validations_{data_tester.table_gold_name}',
)

# COMMAND ----------

report = data_tester.display_results(validation_results)
dbutils.notebook.exit(report)

# COMMAND ----------


