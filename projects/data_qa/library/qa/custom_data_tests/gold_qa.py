import json
import time
import datetime
from typing import (
    List, 
    Dict, 
    Tuple,
)

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult
from pyspark.sql import (
    DataFrame, 
    SparkSession,
)
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import TimestampType

from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.qa.utils import LogTag
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.utils import initialize_and_prepare_delta


class GoldQA:
    def __mapping_col_azure_to_uc(dict_rename_cols: Dict, df_expected_info_schema: DataFrame) -> DataFrame:
        """
        Maps column names from Azure to Unity Catalog based on dict_rename_cols receveid as intial parameter.
        
        Args:
            df_expected_info_schema (DataFrame): The DataFrame with information_schema data.
            dict_rename_cols (Dict): The dictionary containing the mapping between Azure and Unity Catalog.
                e.g.: {'vessel': 'ship',
                       'division': 'region',
                       'interval_lenght_uom': 'interval_length_uom'}
        Returns:
            DataFrame
        """
        for key, value in dict_rename_cols.items():
            df_expected_info_schema = df_expected_info_schema.withColumn('column_name', regexp_replace('column_name', key, value))
        
        return df_expected_info_schema

    @staticmethod
    def __calculate_execution_time(spark: SparkSession, path_table: str, system: str) -> float:
        """Calculates the execution time of a Spark SQL query.

        Args:
            spark (SparkSession): The SparkSession object.
            path_table (str): The path table to query from.
            system (str): The system identifier.

        Returns:
            float: The execution time of the query.
        """
        query = f'SELECT * FROM {path_table}'
        start_time = datetime.timedelta(milliseconds=time.time())
        if system == 'uc':
            df = spark.sql(query).limit(1000)
        else:
            try:
                df = initialize_and_prepare_delta(spark=spark, query=query, catalog_azure_name='csldw').limit(1000)
            except Exception:
                df = spark.createDataFrame([('dummy',)], ['col_dummy'])

        df.count() # execute a action in spark
        end_time = datetime.timedelta(milliseconds=time.time())
        return end_time - start_time

    @staticmethod
    def check_if_table_have_same_elapsed_time(spark: SparkSession, path_table_observed: str, path_table_expected: str) -> Tuple[bool, str]:
        """
        Checks if the elapsed time of running queries on two different systems is within an acceptable tolerance.
        It is accepted 5 seconds by evaluate the difference between systems. By default, the tolerance is 5% of the difference.

        Args:
            spark (SparkSession): The SparkSession object for Spark application.
            path_table_observed (str): The path of the table observed in the system.
            path_table_expected (str): The path of the table expected in the system.

        Returns:
            tuple: A tuple containing a boolean indicating whether the elapsed time is within tolerance and a message.
                The message contains information about the comparison of elapsed times.
        """
        elapsed_observed = GoldQA.__calculate_execution_time(spark, path_table_observed, 'uc')
        elapsed_expected = GoldQA.__calculate_execution_time(spark, path_table_expected, 'azure')

        # great expectations output
        diff_percent = round(abs((elapsed_observed - elapsed_expected) / elapsed_expected) * 100)
        return True, f'The queries were executed in both systems with a {diff_percent}% variance. Azure = {elapsed_expected} msecs  UC = {elapsed_observed} msecs'

    @staticmethod
    def check_if_col_have_equal_not_null_constraint(
        df_observed_info_schema: DataFrame, 
        df_expected_info_schema: DataFrame, 
        col_name: str,
        dict_rename_cols: Dict,
    ) -> Tuple[bool, str]:
        """
        Checks if two cols have the same not null constraint

        Args:
            df_observed_info_schema (DataFrame): The observed DataFrame with information_schema data.
            df_expected_info_schema (DataFrame): The expected DataFrame with information_schema data.
            col_name (str): Name of the column to check constraint.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the two DataFrames have the same default values
            and a message describing the comparison result.
        """
        constraint_observed = df_observed_info_schema \
            .select('column_name', 'is_nullable') \
            .filter(col('column_name') == col_name) \
            .first()['is_nullable']

        df_expected_info_schema = GoldQA.__mapping_col_azure_to_uc(dict_rename_cols, df_expected_info_schema)

        try:
            constraint_expected_original = df_expected_info_schema \
                .select('column_name', 'is_nullable') \
                .filter(col('column_name') == col_name) \
                .first()
            if constraint_expected_original is not None:
                constraint_expected = constraint_expected_original['is_nullable']
            else:
                # df_expected_info_schema_updated = GoldQA.__mapping_col_azure_to_uc(dict_rename_cols, df_expected_info_schema)
                constraint_expected_mapped = df_expected_info_schema \
                    .select('column_name', 'is_nullable') \
                    .filter(col('column_name') == col_name) \
                    .first()
                constraint_expected = constraint_expected_mapped['is_nullable']

        except TypeError as e:
            raise Exception(f'Col {col_name} not found in the info_schema EXPECTED (sqlserver). Add this mapping (sqlserver: uc) in the dict_rename_cols parameter. Showing information_schema (sqlserver): {df_expected_info_schema.display()}')

        # great expectations output
        if constraint_observed == constraint_expected:
            return True, f'Matched, in both the constraint is_nullable is {constraint_expected}'
        else:
            return False, f'Mismatch NOT NULL constraint. Read is_nullable column from information_schema. EXPECTED: {constraint_expected} | OBSERVED: {constraint_observed}'

    @staticmethod
    def check_if_table_have_same_default_values(df_observed: DataFrame, df_expected: DataFrame, list_pk: List[str]) -> Tuple[bool, str]:
        """
        Checks if two DataFrames have the same default values based on the specified primary keys.

        Args:
            df_observed (DataFrame): The observed DataFrame.
            df_expected (DataFrame): The expected DataFrame.
            list_pk (List[str]): A list of primary key column names.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the two DataFrames have the same default values
            and a message describing the comparison result.
        """
        df_expected_defaut_values = df_expected.select(*list_pk).filter(col(list_pk[0]) < 0)
        df_observed_defaut_values = df_observed.select(*list_pk).filter(col(list_pk[0]) < 0)

        return custom_qa.check_if_two_df_contain_the_same_rows(
                df_expected=df_expected_defaut_values,
                df_expected_name='azure',
                df_observed=df_observed_defaut_values,
                df_observed_name='uc',
                tag=LogTag.DEFAULT_VALUES,
            )

    @staticmethod
    def check_same_content_rows(
        df_expected: DataFrame, 
        df_observed: DataFrame, 
        list_timestamp_cols: List[str],
        list_pk: List[str],
        c: str,
    ) -> Tuple[bool, str]:
        """
        This function handle the situation when It has technical values in timestamp col, which can happen the follow situation: 
            Expected
            +---+-------------------+-------------------+
            | id|      creation_date|   last_update_date|
            +---+-------------------+-------------------+
            | -1|2017-04-03 00:00:00|2017-04-03 00:00:00|
            +---+-------------------+-------------------+
            Observed
            +---+-------------------+-------------------+
            | id|      creation_date|   last_update_date|
            +---+-------------------+-------------------+
            | -1|2024-03-11 12:02:00|2024-03-11 12:02:00|
            +---+-------------------+-------------------+
        To resolve this, It was filtered if current col is timestamp and than removed technical values (-1, -2, -3)
        
        Args:
            df_expected (DataFrame): The expected DataFrame.
            df_observed (DataFrame): The observed DataFrame.
            list_timestamp_cols (List[str]): A list of column names considered as timestamps.
            list_pk (List[str]): A list of primary key column names.
            c (str): The column to be checked for row content.

        Returns:
            bool: True if the rows in the selected column contain the same content, False otherwise.
        
        Notes:
            dw_commercial.dim_voyage_itineraries_hist has 0 as technical value.
            test_gold.dw_commercial.fact_voyage_days_port has col PK as timestamp. It was necessary to check the datatype for all PK.
        """
        if c in list_timestamp_cols:
            for pk in list_pk:
                if not isinstance(df_expected.schema[pk].dataType, TimestampType):
                    # remove technical values, as 0, -1, -2
                    df_expected = df_expected.filter(col(pk) >= 0)
                    df_observed = df_observed.filter(col(pk) >= 0)

            return custom_qa.check_if_two_df_contain_the_same_rows(
                    df_expected=df_expected.select(c),
                    df_expected_name='azure',
                    df_observed=df_observed.select(c),
                    df_observed_name='uc',
                    tag=LogTag.ROW_CONTENT,
                )
        else:
            return custom_qa.check_if_two_df_contain_the_same_rows(
                    df_expected=df_expected.select(c),
                    df_expected_name='azure',
                    df_observed=df_observed.select(c),
                    df_observed_name='uc',
                    tag=LogTag.ROW_CONTENT,
                )

    @staticmethod
    def check_if_table_have_same_count_distinct(
        list_pk_cols: List[str], 
        list_sk_cols: List[str], 
        df_expected: DataFrame, 
        df_observed: DataFrame,
    ) -> Tuple[bool, str]:
        """
        Checks if the count of rows in the expected df matches the count of rows in the bronze df.
        #### Args:
            - list_pk_cols (List): The list of primary key columns.
            - list_sk_cols (List): The list of surrogate key columns.
            - df_expected (DataFrame): The expected df whose count is compared.
            - df_observed (DataFrame): The observed df whose count is compared.
        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating if the counts match and a message.
        """
        list_cols = list_pk_cols + list_sk_cols
        total_observed = df_observed.select(*list_cols).distinct().count()
        total_expected = df_expected.select(*list_cols).distinct().count()

        # great expectations output
        if total_observed == total_expected:
            return True, f'total_expected and total_observed = {total_expected}'
        else:
            return False, f'Mismatch in row counts. ' \
                        f'Expected = {total_expected} observed = {total_observed}. Executed a count distinct with these cols: {list_cols}'

    @staticmethod
    def check_if_tables_have_correct_owner(spark: SparkSession, owner_expected: str, path_table: str) -> Tuple[bool, str]:
        """
        Check if the bronze and silver tables have the same owner.

        Args:
            spark (SparkSession): The SparkSession object.
            owner_expected (str): The expected owner of the tables.
            path_table (str): The path to the tables.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the tables have the same owner
            and a message describing the comparison result.
        """
        owner_observed = spark.sql(f"DESCRIBE EXTENDED {path_table}") \
            .filter(col('col_name') == 'Owner') \
            .select('data_type') \
            .collect()[0][0]

        # great expectations output
        if owner_observed == owner_expected:
            return True, f'Bronze and silver have the same owner: {owner_expected}'
        else:
            return False, 'The owner of the tables are not the same! ' \
                          f'EXPECTED: {owner_expected} | OBSERVED: {owner_observed}'
    
    @staticmethod
    def check_if_col_have_same_count_distinct(
        col_name: str, 
        df_expected: DataFrame, 
        df_observed: DataFrame, 
        path_table_observed: str, 
        path_table_expected: str,
    ) -> Tuple[bool, str]:
        """
        Checks if the count of rows by col in the expected df matches the count of rows in the observed df.

        #### Args:
            - col_name (str): the name of the column. This column exists in both dataframes.
            - df_expected (DataFrame): The expected df whose count is compared.
            - df_observed (DataFrame): The observed df whose count is compared.
            - path_table_expected (str): Path of the table.
            - path_table_observed (str): Path of the table.

        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating if the counts match and a message.
        """
        total_observed_col = df_observed \
            .select(col_name) \
            .distinct() \
            .count()
        total_expected_col = df_expected \
            .select(col_name) \
            .distinct() \
            .count()

        # great expectations output
        if total_observed_col == total_expected_col:
            return True, f'total_expected and total_observed = {total_expected_col}'
        else:
            if total_observed_col == 0 and total_expected_col != 0:
                return False, f'Col is empty in {path_table_observed} but not in {path_table_expected} = {total_expected_col}!'
            else:
                return False, f'Mismatch distinct count between {path_table_observed} and {path_table_expected}! ' \
                              f'Expected: {total_expected_col} | Observed: {total_observed_col}'
    
    @staticmethod
    def check_if_column_have_constraint(
        list_expected_constraints: List[str], 
        list_observed_constraints: List[str], 
        path_table_expected: str, 
        path_table_observed: str, 
        col_name: str, 
        constr_type: str,
        list_pk_cols: List[str],
    ) -> Tuple[bool, str]:
        """
        Check if a column has a specified constraint. The source of truth is list_expected_constraints, 
        So, It is necessary to check if a constraint (col_name) exists in list_observed_constraints.
        Every tables in gold must have pk constraint, but fk needs to confirm all cols that end with _sk. It is not possible filter fk constraints that exists in sqlserver or uc and based the on only this because there are some cases that both sides not contains the fk.

        Args:
            list_expected_constraints (List[str]): List of expected constraints.
                e.g.: ['to_currency_sk', 'from_currency_sk']
            list_observed_constraints (List[str]): List containing observed constraints.
                e.g.: ['to_currency_sk', 'from_currency_sk']
            path_table_expected (str): Path of the table with expected constraints.
            path_table_observed (str): Path of the table with observed constraints.
            col_name (str): Name of the column to check constraints for. It is from the list_pk_cols or list_sk_cols.
                This information was taken from expected df
            constr_type (str): Type of constraint to check. e.g.: fk or pk

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating if the column has the constraint,
            and a message indicating the result of the check.
        """
        # great expectations output
        if col_name in list_observed_constraints and col_name not in list_expected_constraints: # if exists in uc but not in azure
            return True, f'WARNING: Not found the {constr_type} in {path_table_expected}. In {path_table_observed} was found {constr_type} for this col: {col_name}'
        elif col_name in list_observed_constraints:
            return True, f'The uc and azure have same {constr_type} constraint in {col_name}.'
        else:
            if col_name in list_expected_constraints: # if constr exists in az but not in uc
                return False, f'''{constr_type} constraint not found associated with {col_name}! list EXPECTED contraints: {list_expected_constraints} | list OBSERVED constraints: {list_observed_constraints}'''
            elif col_name in list_pk_cols: # if not exists in both side, but it is a pk
                return True, f'Not found the {constr_type} contraint in SQLServer and UC for {col_name} col. This col is just a pk.'
            else: # if not exists in both side
                return False, f'Not found the {constr_type} contraint in SQLServer and UC for {col_name} col.'

    @staticmethod
    def check_if_column_have_same_total_nulls(df_expected: DataFrame, df_observed: DataFrame, col_name: str) -> Tuple[bool, str]:
        """
        Checks if the total count of null values for a specified column is the same between two DataFrames.

        Args:
            df_expected (DataFrame): The expected DataFrame.
            df_observed (DataFrame): The observed DataFrame.
            col_name (str): The name of the column to check for null values.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the null value counts are the same,
            and a string providing the result message.
        """
        expected_null_count = df_expected.select(col_name).filter(col(col_name).isNull()).count()
        observed_null_count = df_observed.select(col_name).filter(col(col_name).isNull()).count()

        # great expectations output
        if expected_null_count == observed_null_count:
            return True, f'The null value count is consistent between environments: {expected_null_count}.'
        else:
            return False, f'''Mismatch count nulls! EXPECTED: {expected_null_count} | OBSERVED: {observed_null_count}'''

    @staticmethod
    def check_if_column_have_referential_integrity(
        spark: SparkSession, 
        df_observed_constraints: DataFrame,
        path_gold_table: str, 
        col_name: str,
    ) -> Tuple[bool, str]:
        """
        Check referential integrity between two columns in different tables.

        Args:
            spark (SparkSession): Spark session object.
            df_observed_constraints (DataFrame): DataFrame containing observed constraints.
            path_gold_table (str): Path to the gold table.
            col_name (str): Name of the column to check integrity for.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the integrity check passed and a message describing the result.
        """
        # referential integrity
        if df_observed_constraints.count() > 0:
            list_rows_pk = spark.sql(f'select distinct {col_name} from {path_gold_table}').collect()
            set_pk = {row[col_name] for row in list_rows_pk}

            list_rows_fk = spark.sql(f'select distinct {col_name} from {path_gold_table}').collect()
            set_fk = {row[col_name] for row in list_rows_fk}
            all_exist = set_fk.issubset(set_pk)

            # great expectations output
            if all_exist:
                return True, f'All elements in col: {col_name} exists in both side.'
            else:
                diff = set_fk - set_pk
                return False, f'Some elements in col: {col_name} are missing. (distinct fk - distinct pk): {diff}.'
        else:
            return False, f'A foreign key constraint was not found in the {path_gold_table} associated with {col_name}.'

    @staticmethod
    def check_if_table_exists_metadata_in_data_catalog(env: str, spark: SparkSession, path_gold_table: str) -> Tuple[bool, str]:
        """
        Check if a table exists in the data catalog.

        Args:
            env (str): the environment, if dev or test.
            spark (SparkSession): The Spark session object.
            path_gold_table (str): The path of the table to check in the data catalog.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the table exists in the data catalog
            and a message describing the result.
        """
        query = f"""
        SELECT table_id 
        FROM {env}_data_catalog.gold_data_catalog.dim_tables 
        WHERE table_id = '{path_gold_table}'
        """
        # great expectations output
        if spark.sql(query).take(1):
            return True, f'Found the table_id = {path_gold_table} in Data Catalog dim_tables.'
        else:
            return False, f'Not found {path_gold_table}. Without this table_id is not possible to enrich metadata. Used this query: {query}'
