# Databricks notebook source
# MAGIC %md
# MAGIC # Silver to Gold View Test Data

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

from library.qa.silver_to_gold_view_test_data import SilverToGoldViewTestData
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
dbutils.widgets.text("sql_database", "onpremise")
dbutils.widgets.text("catalog_db_name", "XPTOdw")
dbutils.widgets.text("schema_db_name", "report")
dbutils.widgets.text("view_db_name", "dim_cost_center")
dbutils.widgets.text("schema_gold_name", "dw_report")
dbutils.widgets.text("view_gold_name", "v_dim_cost_center")

# COMMAND ----------

data_tester = SilverToGoldViewTestData(
    spark=spark,
    sql_database=dbutils.widgets.get("sql_database"),
    catalog_db_name=dbutils.widgets.get("catalog_db_name"),
    schema_db_name=dbutils.widgets.get("schema_db_name"),
    view_db_name=dbutils.widgets.get("view_db_name"),
    schema_gold_name=dbutils.widgets.get("schema_gold_name"),
    view_gold_name=dbutils.widgets.get("view_gold_name"),
)

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
# MAGIC ## Results

# COMMAND ----------

report = data_tester.display_results(validation_results)
output = data_tester.path_gold_view + '\n' + report + '\n'
dbutils.notebook.exit(output)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
