# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI: Validating ammount fields

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# DBTITLE 1,Imports
from library.database.azure_sql import AzureSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql
from pyspark.sql.functions import col
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `FACT_VOYAGE_PNL`, 'FACT_COMM_REVENUE

# COMMAND ----------

# DBTITLE 1,Validation for amount fields
from library.database.sqlserver_dw_sql import SQLServerDWSql
from library.qa.silver_to_gold_test_data import SilverToGoldTestData

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from statistics import mean

def display_unmatched_pairs(df_expected: DataFrame, df_observed: DataFrame, *field_names) -> None:
    """
    Displays unmatched value pairs between columns of two dataframes
    Args:
        df_expected (DataFrame): The DataFrame representing expected data.
        df_observed (str): Name of the DataFrame containing expected data.
        field_names: The name of the columns to be compared.

    """
    import pandas as pd

    for field_name in field_names:
        df_expected_values = sorted([row[field_name] for row in df_expected.select(F.col(field_name)).collect()])
        df_observed_values = sorted([row[field_name] for row in df_observed.select(F.col(field_name)).collect()])

        df_expected_values_set = set(df_expected_values)
        df_observed_values_set = set(df_observed_values)

        missing_in_expected = sorted(list(df_observed_values_set - df_expected_values_set))
        missing_in_observed = sorted(list(df_expected_values_set - df_observed_values_set))

        df = pd.DataFrame({
            'Observed Values': pd.Series(df_expected_values),
            'Missing/Expected Values': pd.Series(df_observed_values)
        })
        df['difference'] = df['Observed Values']-df['Missing/Expected Values']
        max_diff = df['difference'].max()
        min_diff = df['difference'].min()
        diff_threshold: str = '0.0002'
        df = df.query(f'not (difference >= -{diff_threshold} and difference <= {diff_threshold})')
#        df = df.query(' (difference !=0 )')
        mismatch_count = df.shape[0]
        print("=" * 50)
        print(f"{field_name.upper()} COMPARISON => \t||\t MISMATCH (by {diff_threshold} threshold) Count: {mismatch_count} rows \t||\t MaxDifference Observed: {max(abs(min_diff), max_diff)}")
        print("f------EXPECTED")
        print(f"TOTAL count: {len(df_expected_values)} \t UNIQUE count: {len(df_expected_values_set)} \t MinValue: {min(df_expected_values)} \t AvgValue: {mean(df_expected_values)} \t MaxValue: {max(df_expected_values)}")
        print("f------OBSERVED")
        print(f"TOTAL count: {len(df_observed_values)} \t UNIQUE count: {len(df_observed_values_set)} \t MinValue: {min(df_observed_values)} \t AvgValue: {mean(df_observed_values)} \t MaxValue: {max(df_observed_values)}")
        print("=")
        print(f"Unmatched values pairs for field: {field_name.upper()}")
        print(df)
        print("\n")

# CHANGE HERE TO MATCH YOUR TEST CASE
on_prem_table = "dw.fact_comm_revenue"
uc_table = "test_gold.dw_commercial.fact_comm_revenue"
field_names = ['revenue_group_amt','revenue_region_amt'] #, 'orig_amt', 'amt', 'cad_amt', 'quantity']


# DONT NEED TO CHANGE
on_prem_query = f"SELECT * FROM {on_prem_table}"
source_df = spark.read.format(SQLServerDWSql.format).options(**SQLServerDWSql().options(db_name="XPTOdw")).option("query", on_prem_query).load()

# RENAMING PREFIX
new_field_names = {
    field.name: field.name.lower().lstrip("").lstrip('_')
    for field in source_df.schema.fields
}
source_df = source_df.withColumnsRenamed(new_field_names)

target_df = spark.read.table(uc_table)


# ALL AMOUNT FIELDS ?
all_amount_fields = list(filter(lambda x: str(x).endswith("_amt"), target_df.schema.fieldNames() ))
# field_names = all_amount_fields

display_unmatched_pairs(source_df, target_df, *field_names)
