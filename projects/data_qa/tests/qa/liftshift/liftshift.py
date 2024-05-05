# Databricks notebook source
# MAGIC %md
# MAGIC ## Liftshift Test Data

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache

# COMMAND ----------

import ast
import os

from IPython.display import (
    display,
    HTML,
)

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult

from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    ceil,
    concat_ws,
    round,
    when,
    concat,
    lit,
)

from library.great_expectations.util import add_custom_result_to_validation
from library.logger_provider import LoggerProvider
from library.qa.liftshift_test_data import LiftshiftTestData
from library.qa.template_data_tests import TemplateDataTest
from library.qa.utils import (
    get_and_prepare_data_dim,
    get_business_cols,
    get_col_not_be_null,
    generate_information_schema_query,
    get_sk_fact_names_and_pk_dim_names,
    get_and_prepare_data_for_referential_integraty_test,
    parser_list_cols_to_str,
    find_df_diff,
    prepare_datetime_col,
    extract_timestamp_components,
    get_timestamp_cols,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declare Parameters

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Debug: Initial Paramaters
# -------
# dim
# -------
dbutils.widgets.text("query_path", "sql/procurement/dimensions/dim_maintenance_job.sql")
dbutils.widgets.text("catalog_name", "csldw")
dbutils.widgets.text("schema_name", "dw")
dbutils.widgets.text("table_name", "dim_maintenance_job")
dbutils.widgets.text("table_type", "dim")
dbutils.widgets.text("list_deny_cols", "['MAINT_JOB_CANCEL_DATE']")

# -------
# fact without constraints
# -------
# dbutils.widgets.text("query_path", "sql/finance/facts/fact_fixed_asset_continuity.sql")
# dbutils.widgets.text("catalog_name", "csldw")
# dbutils.widgets.text("schema_name", "dw")
# dbutils.widgets.text("table_name", "fact_fixed_asset_continuity")
# dbutils.widgets.text("table_type", "fact")
# dbutils.widgets.text("dict_dims_relationalships", "")

# -------
# fact with constraints
# -------
# dbutils.widgets.text("query_path", "sql/finance/facts/fact_fixed_asset_continuity.sql")
# dbutils.widgets.text("catalog_name", "csldw")
# dbutils.widgets.text("schema_name", "dw")
# dbutils.widgets.text("table_name", "fact_fixed_asset_continuity")
# dbutils.widgets.text("table_type", "fact")
# dbutils.widgets.text("dict_dims_relationalships",
#    """{'DIM_FIXED_ASSET': 'FIXED_ASSET_SK', 'DIM_ACCOUNTING_COMPANY': 'ACCT_COMPANY_SK', 'DIM_TIME': 'DATE_SK', 'DIM_ACCOUNT': 'ACCOUNT_SK', 'DIM_COST_CENTER': 'COST_CENTER_SK'}"""
# )
# dbutils.widgets.text("skip_dim_check", "False")

# COMMAND ----------

from library.database.azure_sql import AzureSQL
from library.database.customer_portal import CustomerPortalSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql

dbutils = DBUtils(spark)
logger = LoggerProvider.get_logger()

try:
    onpremises_sql_connector = dbutils.widgets.get("onpremises_sql_connector")
    if onpremises_sql_connector == 'CustomerPortalSQL':
        onpremises_sql_connector = CustomerPortalSQL
    elif onpremises_sql_connector == 'AzureSQL':
        onpremises_sql_connector = AzureSQL
    else:
        onpremises_sql_connector = SQLServerDWSql
except:
    onpremises_sql_connector = SQLServerDWSql

try:
    azure_sql_connector = dbutils.widgets.get("azure_sql_connector")
    azure_sql_connector = CustomerPortalSQL if azure_sql_connector == 'CustomerPortalSQL' else AzureSQL
except:
    azure_sql_connector = AzureSQL

try:
    table_name_validation: bool = eval(dbutils.widgets.get("validate_table_name"))
except:
    table_name_validation = True

catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
table_type = dbutils.widgets.get("table_type")
try:
    skip_dim_check: bool = dbutils.widgets.get("skip_dim_check")
except:
    skip_dim_check: bool = False

path = dbutils.widgets.get("query_path")
query_path = "".join(os.getcwd() + "/" + path)
query = open(query_path).read().upper()

if table_type == 'fact':
    str_dims_relationalships = dbutils.widgets.get("dict_dims_relationalships")
    try:
        dict_dims_relationalships = ast.literal_eval(str_dims_relationalships)
    except Exception:
        dict_dims_relationalships = None
        logger.error('The parameter dict_dims_relationalships is None/null')
else:
    dict_dims_relationalships = None

# try to get the list_deny_cols when exits as a parameter
try:
    str_list_deny_cols = dbutils.widgets.get("list_deny_cols")
    list_deny_cols = ast.literal_eval(str_list_deny_cols)
except Exception:
    list_deny_cols = []

# create query_properties
query_properties = generate_information_schema_query(
    table_name=table_name,
    schema_name=schema_name,
    catalog_name=catalog_name,
)

# COMMAND ----------

data_tester = LiftshiftTestData(
    spark=spark,
    table_type=table_type,
    catalog_name=catalog_name,
    schema_name=schema_name,
    table_name=table_name,
    query=query,
    query_properties=query_properties,
    source_connector=onpremises_sql_connector,
    target_connector=azure_sql_connector,
    validate_table_name_in_source_query=table_name_validation
)
data_tester.log_execution_parameters()
data_tester.validate_parameters()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC - If it is necessary to debug and see what was received from the sources, use the `df_onpremises` or `df_azure`. The dataframes `df_onpremises_only_properties` and `df_azure_only_properties` contain only metadata used to validate some tests.
# MAGIC
# MAGIC
# MAGIC | SQL Server  | Azure SQL |
# MAGIC |-------------|-----------|
# MAGIC | On-premises | Azure     |
# MAGIC | Expected    | Observed  |
# MAGIC | Source      | Target    |

# COMMAND ----------

(
    df_onpremises,
    df_onpremises_only_properties,
    df_azure,
    df_azure_only_properties,
) = data_tester.get_and_prepare_data()

# COMMAND ----------

# sec
# df_azure.withColumn("year", year(col_name)) \
#              .withColumn("month", month(col_name)) \
#              .withColumn("day", dayofmonth(col_name)) \
#              .filter(col("year") == '2019').filter(col("month") == '08').filter(col("day") == '13').filter(col("FPA_PERSON_ASSIGN_SEQ") == '1').orderBy('FPA_CREATION_DATE').display()
# df_onpremises.withColumn("year", year(col_name)) \
#              .withColumn("month", month(col_name)) \
#              .withColumn("day", dayofmonth(col_name)) \
#              .filter(col("year") == '2019').filter(col("month") == '08').filter(col("day") == '13').filter(col("FPA_PERSON_ASSIGN_SEQ") == '1').orderBy('FPA_CREATION_DATE').display()
# 2019-08-13T14:37
# min
# df_azure.select('FPA_LAST_UPDATE_DATE').filter(col("FACT_PERSON_ASSIGNMENT_SK") == '1').orderBy('FACT_PERSON_ASSIGNMENT_SK').display()
# df_onpremises.select('FPA_LAST_UPDATE_DATE').filter(col("FACT_PERSON_ASSIGNMENT_SK") == '1').orderBy('FACT_PERSON_ASSIGNMENT_SK').display()

# COMMAND ----------

# Used in test: expect_corresponding_dims
list_azure_cols_timestamp = get_timestamp_cols(df_azure_only_properties)
list_onpremises_cols_timestamp = get_timestamp_cols(df_onpremises_only_properties)
df_azure = prepare_datetime_col(df_azure, list_azure_cols_timestamp)
df_onpremises = prepare_datetime_col(df_onpremises, list_onpremises_cols_timestamp)
logger.info(f'Cols that was truncated the milisec and sync the datetime: {list_azure_cols_timestamp}')

# COMMAND ----------

# Used in test: referential integraty (expect_column_values_to_be_in_set)
# only execute on azure 
if table_type == 'fact' and dict_dims_relationalships is not None:

    # TODO -> func
    list_dims_pk_names = list(dict_dims_relationalships.values())
    str_parsed_with_list_dims_pk_names = parser_list_cols_to_str(list_dims_pk_names)
    df_constraints = get_sk_fact_names_and_pk_dim_names(spark, table_name, catalog_name, str_parsed_with_list_dims_pk_names)

    # not found constraints
    if df_constraints.count() == 0:
        logger.warning(f'The {table_name} fact table doesnt contains SK or SK are not connect into dimesion table')
        df_map_fact_dim_values = None
    else:
        dict_cols_names = get_and_prepare_data_dim(
            spark=spark,
            dict_cols=dict_dims_relationalships,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )
        df_map_fact_dim_values = get_and_prepare_data_for_referential_integraty_test(
            spark=spark,
            dict_cols_names=dict_cols_names,
            df_constraints=df_constraints,
        )

        #df_map_fact_dim_values.display()

else:
    # this parameters is not necessary for dimesions
    df_map_fact_dim_values = None

# COMMAND ----------

# Used in test: expect_same_content_rows
# Remove SK and ETL columns
df_onpremises_business_cols = get_business_cols(df_onpremises, list_deny_cols)
df_azure_business_cols = get_business_cols(df_azure, list_deny_cols)
logger.info(f'Business columns: {df_onpremises_business_cols.columns}')

# Used in test: expect_column_values_to_not_be_null
list_cols_not_be_null = get_col_not_be_null(df_onpremises_only_properties)
logger.info(f'Columns not be null: {list_cols_not_be_null}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Test

# COMMAND ----------

kwargs = {
    "df_onpremises_only_properties": df_onpremises_only_properties,
    "df_azure_only_properties": df_azure_only_properties,
    "df_onpremises_business_cols": df_onpremises_business_cols,
    "df_azure_business_cols": df_azure_business_cols,
    "table_type": table_type,
    "dict_dims_relationalships": dict_dims_relationalships,
}
dict_result_custom_tests = data_tester.execute_custom_data_tests(
    df_expected=df_onpremises,
    df_observed=df_azure,
    df_expected_name="on-premises",
    df_observed_name="azure-sql",
    kwargs=kwargs,
)

# COMMAND ----------

def execute_data_expectations(
    df_expected: DataFrame,
    df_observed: DataFrame,
    dict_result_custom_tests: dict,
    extra_args: dict = None,
) -> dict:
    """
    Returns:
        A JSON-formatted dict containing a list of the validation results.
    """
    expectations = ExpectationSuite(f"{data_tester.step_layer}-{schema_name}.{table_name}")  # fmt: off # noqa
    gdf_observed = SparkDFDataset(df_observed)

    # equal count by row
    logger.info("Executing: expect_table_row_count_to_equal")
    exp = ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal",
        kwargs={"value": int(df_expected.count())},
    )
    expectations.add_expectation(exp)

    # equal count by col
    logger.info("Executing: expect_table_column_count_to_equal")
    exp = ExpectationConfiguration(
        expectation_type="expect_table_column_count_to_equal",
        kwargs={"value": len(df_expected.columns)},
    )
    expectations.add_expectation(exp)

    # col exists
    logger.info("Executing: expect_column_to_exist")
    for col_name in df_expected.columns:
        exp = ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": col_name},
        )
        expectations.add_expectation(exp)

    # type
    logger.info("Executing: expect_column_values_to_be_of_type")
    list_col_names_target = [f.name for f in df_expected.schema]
    list_col_types_target = [str(type(f.dataType).__name__) for f in df_expected.schema]  # fmt: off # noqa
    for col_name, col_type in zip(list_col_names_target, list_col_types_target):
        exp = ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={
                "column": col_name,
                "type_": col_type,
            },
        )
        expectations.add_expectation(exp)

    # not be null
    logger.info("Executing: list_cols_not_be_null")
    list_cols_not_be_null = extra_args["list_cols_not_be_null"]
    for c in list_cols_not_be_null:
        exp = ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={
                "column": c,
            },
        )
        expectations.add_expectation(exp)

    # total unique rows per columns
    # NOTE: great expectations consider unique values by removing null
    logger.info("Executing: check count unique")
    list_business_cols = extra_args["list_business_cols"]
    for c in list_business_cols:
        total = int(df_expected.select(c).filter(col(c).isNotNull()).distinct().count())
        exp = ExpectationConfiguration(
            expectation_type="expect_column_unique_value_count_to_be_between",
            kwargs={
                "column": c,
                "min_value": total,
                "max_value": total,
            },
        )
        expectations.add_expectation(exp)

    # referential integraty
    # This test only execute on Azure and in fact queries
    # It is expected that all values in the "column" from Azure are contained in the "value_set": list_dim_values (dim).
    # This ensures that there are no orphaned rows in df_observed[column].
    table_type = extra_args['table_type']
    if table_type == 'fact':
        df_map_fact_dim_values = extra_args["df_map_fact_dim_values"]

        # if fact not contains constraints
        if df_map_fact_dim_values is None:
            logger.warning('Executing: The fact table doesnt contains SK')
        else:
            logger.info("Executing: referential integraty")
            list_fact_sk = df_map_fact_dim_values.select('fact_sk').rdd.flatMap(lambda x: x).collect()
            list_dim_pk = df_map_fact_dim_values.select('dim_pk').rdd.flatMap(lambda x: x).collect()
            list_within_lists_dim_values = df_map_fact_dim_values.select('dim_values').rdd.flatMap(lambda x: x).collect()
            for fact_sk, dim_pk, list_dim_values in zip(list_fact_sk,
                                                        list_dim_pk,
                                                        list_within_lists_dim_values):
                logger.info(f"FACT: {fact_sk} <-> DIM: {dim_pk}")
                limit_row_html_great_exp = 10000
                if len(list_dim_values) > limit_row_html_great_exp:
                    continue

                exp = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_in_set",
                    kwargs={
                        "column": fact_sk,
                        "value_set": list_dim_values,
                    },
                )
                if skip_dim_check == False:
                    expectations.add_expectation(exp)

    validation_results = gdf_observed.validate(expectation_suite=expectations, catch_exceptions=False, result_format='BASIC')

    # --------------------------
    # complementary expectations
    # --------------------------
    # if fact orchestration, these tests only test fact columns
    
    # fact with FK
    if table_type == 'fact' and dict_dims_relationalships is not None:
        have_corresponding_dims = dict_result_custom_tests["have_corresponding_dims"]
        # dims_associate_measure
        logger.info("Executing: check measure with corresponding dims")
        exp = ExpectationValidationResult(
            success=have_corresponding_dims[0] is True,
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_corresponding_dims",
                kwargs={
                    "result": have_corresponding_dims[0],
                    "msg": have_corresponding_dims[1]
                },
            ),
        )
        if skip_dim_check == False:
            add_custom_result_to_validation(exp, validation_results)

    # same_rows
    # check only business columns (without _SK and ETL_)
    logger.info("Executing: check if the row content are equal (just business columns)")
    are_same_rows = dict_result_custom_tests["are_same_rows"]
    exp = ExpectationValidationResult(
        success=are_same_rows[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_same_content_rows",
            kwargs={"result": are_same_rows[0], "msg": are_same_rows[1]},
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: sqlserver_datatype
    logger.info("Executing: sqlserver property: datatype")
    nullable = dict_result_custom_tests["sqlserver_datatype"]
    exp = ExpectationValidationResult(
        success=nullable[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_sqlserver_datatype",
            kwargs={"result": nullable[0], "msg": nullable[1]},
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: character_maximum_length
    logger.info("Executing: sqlserver property: character_maximum_length")
    character_maximum_length = dict_result_custom_tests["character_maximum_length"]
    exp = ExpectationValidationResult(
        success=character_maximum_length[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_character_maximum_length",
            kwargs={
                "result": character_maximum_length[0],
                "msg": character_maximum_length[1],
            },
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: precision
    logger.info("Executing: sqlserver property: precision")
    precision = dict_result_custom_tests["precision"]
    exp = ExpectationValidationResult(
        success=precision[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_precision",
            kwargs={"result": precision[0], "msg": precision[1]},
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: scale
    logger.info("Executing: sqlserver property: scale")
    scale = dict_result_custom_tests["scale"]
    exp = ExpectationValidationResult(
        success=scale[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_scale",
            kwargs={"result": scale[0], "msg": scale[1]},
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: datetime_precision
    logger.info("Executing: sqlserver property: datetime_precision")
    datetime_precision = dict_result_custom_tests["datetime_precision"]
    exp = ExpectationValidationResult(
        success=datetime_precision[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_datetime_precision",
            kwargs={
                "result": datetime_precision[0],
                "msg": datetime_precision[1]
            },
        ),
    )
    add_custom_result_to_validation(exp, validation_results)

    # sqlserver property: encoding
    logger.info("Executing: sqlserver property: encoding")
    encoding = dict_result_custom_tests["encoding"]
    exp = ExpectationValidationResult(
        success=encoding[0] is True,
        expectation_config=ExpectationConfiguration(
            expectation_type="expect_column_same_encoding",
            kwargs={"result": encoding[0], "msg": encoding[1]},
        ),
    )
    add_custom_result_to_validation(exp, validation_results)
    logger.info("Finished running the tests")

    return validation_results

# COMMAND ----------

extra_args = {
    "list_business_cols": df_onpremises_business_cols.columns,
    "list_cols_not_be_null": list_cols_not_be_null,
    "table_type": "fact",
    "table_type": table_type,
    "df_map_fact_dim_values": df_map_fact_dim_values,
}

validation_results = execute_data_expectations(
    df_expected=df_onpremises,
    df_observed=df_azure,
    dict_result_custom_tests=dict_result_custom_tests,
    extra_args=extra_args,
)

# COMMAND ----------

html_result_gx = data_tester.generate_results_html(validation_results)
display(HTML(html_result_gx))

# COMMAND ----------

# DBTITLE 1,Debug: Data Analysis
# data_tester.generate_data_analysis(df_onpremises_business_cols, df_azure_business_cols)

# COMMAND ----------

# DBTITLE 1,Debug: Check Diff between DFs
# (
#     df_diff_source_target,
#     df_diff_target_source
# ) = find_df_diff(df_source=df_onpremises_business_cols, 
#                  df_target=df_azure_business_cols)

# list_cols_order_by = df_diff_source_target.columns

# logger.warning(f'All df_diff: (df_onpremises_business_cols - df_azure_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_source_target.orderBy(*list_cols_order_by).display()
# else:
#     df_diff_source_target.display()

# logger.warning(f'All df_diff: (df_azure_business_cols - df_onpremises_business_cols) rows:')
# if len(list_cols_order_by) > 0:
#     df_diff_target_source.orderBy(*list_cols_order_by).display()
# else:
#     df_diff_target_source.display()

# COMMAND ----------

# DBTITLE 1,Debug: Check Environments
# from library.database.sqlserver_dw_sql import SQLServerDWSql
# from library.database.azure_sql import AzureSQL

# onpremises_loader = SQLServerDWSql()
# azure_loader = AzureSQL()

# df_onpremises = (
#     spark.read.format("jdbc")
#     .options(**onpremises_loader.options(catalog_name))
#     .option("query", 'SELECT @@SERVERNAME AS servername_onpremises')
#     .load()
# )
# df_onpremises.display()

# df_azure = (
#     spark.read.format(AzureSQL.format)
#     .options(**azure_loader.options(catalog_name))
#     .option("query", 'SELECT @@SERVERNAME AS servername_azure')
#     .load()
# )
# df_azure.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results at Azure Blob Storage

# COMMAND ----------

data_tester.save_report_tests_azure_storage(
    html_result_gx=html_result_gx,
    container_name=data_tester.container_adsl_name,
    step_layer=data_tester.step_layer,
    schema_target_name=data_tester.schema_name,
    table_target_name=data_tester.table_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

report = data_tester.display_results(validation_results)
dbutils.notebook.exit(report)
