import json
import time
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
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from library.dataloader.defaults import Defaults as col_bronze
from library.datacleaner.defaults import Defaults as col_silver
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.utils import LogTag
from library.qa.utils import (
    get_common_cols,
    find_df_diff,
    get_cols_with_data_precision,
)


class CommonQA:
    @staticmethod
    def check_if_two_df_contain_the_same_rows(
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        tag: str,
        table_type: str = None,
        df_onpremises_only_properties: str = None,
    ) -> tuple[bool, str]:
        """
        ### Compare two df and log the differences, if exists. DF's schemas must be equal.
        
        #### Args:
            * df_expected (DataFrame): source DataFrame
            * df_expected_name (str): e.g.: `on-premises`
            * df_observed (DataFrame): target DataFrame
            * df_observed_name (str): e.g.: `azure`
            * tag (str):
            * table_type (str):
            * df_onpremises_only_properties (str):

        #### Returns:
            * bool: True if schema matches, False if not
            * str: message
        """
        list_common_cols = get_common_cols(df_expected, df_observed)
        df_diff_source_target, df_diff_target_source = find_df_diff(df_expected.select(*list_common_cols), df_observed.select(*list_common_cols))
        total_df_diff_src_trg = df_diff_source_target.count()
        total_df_diff_trg_src = df_diff_target_source.count()

        if total_df_diff_src_trg > 0 or total_df_diff_trg_src > 0:
            error_msg = f"""Total rows that only exist in {df_expected_name}: {total_df_diff_src_trg} | Total rows that only exist in {df_observed_name}: {total_df_diff_trg_src} | Analysed for: {df_expected.columns}"""
            return False, error_msg
        else:
            return True, CommonQA.__generate_log_msg(tag, table_type, df_expected, df_onpremises_only_properties)

    @staticmethod
    def __generate_log_msg(
        tag: str,
        table_type: str, 
        df_expected: DataFrame, 
        df_onpremises_only_properties: DataFrame = None,
    ) -> str:
        """
        ### Generates log message

        #### Args:
            * tag (str): the option used in LogTag class
            * table_type (str):
            * df_expected (DataFrame): source DataFrame
            * df_onpremises_only_properties (DataFrame): get from information_schema db

        #### Returns:
            * str: log message
        """
        if tag == LogTag.ROW_CONTENT:
            return f"The rows' content match. Analysed for: {df_expected.columns}"
        elif tag == LogTag.DATA_TYPE:
            df = df_onpremises_only_properties.select("COLUMN_NAME", "DATA_TYPE")
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            return f"The data types match in both environments. Verified for these {table_type} cols: {list_cols_name}"
        elif tag == LogTag.CHARACTER_MAXIMUM_LENGTH:
            df = df_onpremises_only_properties \
                .select("COLUMN_NAME", "CHARACTER_MAXIMUM_LENGTH") \
                .filter(col("CHARACTER_MAXIMUM_LENGTH").isNotNull())
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            if len(list_cols_name) > 0:
                return f'All columns have matching character maximum lengths. Verified for these {table_type} cols: {list_cols_name}'
            else:
                return f"This property is empty for all columns. Test skipped."
        elif tag == LogTag.NUMERIC_PRECISION:
            df = df_onpremises_only_properties \
                .select("COLUMN_NAME", "NUMERIC_PRECISION") \
                .filter(col("NUMERIC_PRECISION").isNotNull())
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            if len(list_cols_name) > 0:
                return f'All columns have matching numeric precisions. Verified for these {table_type} cols: {list_cols_name}'
            else:
                return f"This property is empty for all columns. Test skipped."
        elif tag == LogTag.NUMERIC_SCALE:
            df = df_onpremises_only_properties \
                .select("COLUMN_NAME", "NUMERIC_SCALE") \
                .filter(col("NUMERIC_SCALE").isNotNull())
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            if len(list_cols_name) > 0:
                return f'All columns have matching numeric scales. Verified for these {table_type} cols: {list_cols_name}'
            else:
                return f"This property is empty for all columns. Test skipped."
        elif tag == LogTag.DATETIME_PRECISION:
            df = df_onpremises_only_properties \
                .select("COLUMN_NAME", "DATETIME_PRECISION") \
                .filter(col("DATETIME_PRECISION").isNotNull())
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            if len(list_cols_name) > 0:
                return f'All datetime columns have matching precisions. Verified for these {table_type} cols: {list_cols_name}'
            else:
                return f"This property is empty for all columns. Test skipped."
        elif tag == LogTag.COLLATION_NAME:
            df = df_onpremises_only_properties \
                .select("COLUMN_NAME", "COLLATION_NAME") \
                .filter(col("COLLATION_NAME").isNotNull())
            list_cols_name = [c.COLUMN_NAME for c in df.select('COLUMN_NAME').collect()]
            if len(list_cols_name) > 0:
                return f'All columns have matching encodings. Verified for these {table_type} cols: {list_cols_name}'
            else:
                return f"This property is empty for all columns. Test skipped."
        elif tag == LogTag.DEFAULT_VALUES:
            list_default_values = [c for c in df_expected.collect()]
            if len(list_default_values) > 0:
                return f'All technical values are matching. Found = {list_default_values}'
            else:
                return f'All technical values are matching.'
        else:
            return ''

    @staticmethod
    def check_if_table_have_same_count_distinct(
        list_pk: List[str], 
        df_expected: DataFrame, 
        df_observed: DataFrame,
    ) -> Tuple[bool, str]:
        """
        Checks if the count distinct of rows in the expected df matches the count distinct of rows in the observed df.

        #### Args:
            - list_pk (List): The list of primary key columns.
            - df_expected (DataFrame): The expected df whose count is compared.
            - df_observed (DataFrame): The observed df whose count is compared.

        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating if the counts match and a message.
        """
        total_observed = df_observed.select(*list_pk).distinct().count()
        total_expected = df_expected.select(*list_pk).distinct().count()

        # great expectations output
        if total_observed == total_expected:
            return True, f'total_expected and total_observed = {total_expected}'
        else:
            return False, f'Mismatch in row counts. EXPECTED = {total_expected} | OBSERVED = {total_observed}'

    @staticmethod
    def check_if_two_tuple_are_equal(tuple_expected: Tuple, tuple_observed: Tuple) -> Tuple[bool, str]:
        """
        Checks if two dataframes (described by their schemas) contain the same precision or scale for their columns.
        
        Args:
            tuple_expected (Tuple): The expected Tuple col name and precision or scale.
            tuple_observed (Tuple): The observed Tuple col name and precision or scale.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating if the schemas match in precision or scale, and a string message detailing the outcome.
        """
        if tuple_expected == tuple_observed:
            return True, f'{tuple_expected[1]}'
        else:
            return False, f'They are not equal! EXPECTED: {tuple_expected[1]} | OBSERVED: {tuple_observed[1]}'

    @staticmethod
    def check_if_table_data_format_is_correct(spark: SparkSession, path_uc_table: str) -> Tuple[bool, str]:
        """
        Checks if the data format of a table is correct.
        #### Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating if the format
            is correct and a message describing the format or the error encountered.
        """
        data_format_table_silver_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
            .filter(col('col_name') == 'Provider') \
            .select('data_type') \
            .collect()[0][0]

        # great expectations output
        if data_format_table_silver_observed == 'delta':
            return True, 'Delta table'
        else:
            return False, f'The data format for this table is wrong! EXPECTED: DELTA | OBSERVED: {data_format_table_silver_observed}'

    @staticmethod
    def check_if_table_type_is_correct(spark: SparkSession, path_uc_table: str) -> Tuple[bool, str]:
        """
        Check if the table type is correct.
        #### Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the table type is correct
            and a message describing the result.
        """
        type_table_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
            .filter(col('col_name') == 'Type') \
            .filter(col('data_type').isin(['EXTERNAL', 'MANAGED'])) \
            .select('data_type') \
            .collect()[0][0]

        # great expectations output
        if type_table_observed == 'EXTERNAL':
            return True, 'The table is using external storage.'
        else:
            return False, f'The table type is wrong! EXPECTED: EXTERNAL | OBSERVED: {type_table_observed}'

    @staticmethod
    def check_if_data_path_is_correct(spark: SparkSession, path_uc_table: str, azure_folder: str) -> Tuple[bool, str]:
        """
        Check if the data path is correct.
        #### Returns:
            Tuple[bool, str]: A tuple where the first element is a boolean indicating
            whether the path is correct, and the second element is a message describing the result.
        """
        path_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
            .filter(col('col_name') == 'Location') \
            .filter(col('data_type').like('abfss%')) \
            .select('data_type') \
            .collect()[0][0]
        
        # great expectations output
        if azure_folder in path_observed:
            return True, f'The data path in Azure is located correctly at {path_observed}'
        else:
            return False, 'The data path in Azure is not correct! ' \
                        f'EXPECTED: inside the {azure_folder}/ folder | OBSERVED: {path_observed}'

    @staticmethod
    def check_if_two_df_are_not_empty(spark: SparkSession, df_expected: DataFrame, path_uc_observed: str, path_uc_expected: str) -> Tuple[bool, str]:
        """
        Check if two DataFrames are not empty.
        #### Args:
            - df_expected (DataFrame): The expected DataFrame.
        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating whether the DataFrames are not empty and a message.
        """
        total_observed = spark.sql(f'SELECT COUNT(*) FROM {path_uc_observed}').collect()[0][0]
        total_expected = df_expected.count()
        
        # great expectations output
        if total_observed != 0 and total_expected != 0:
            return True, f'Both tables are not empty.'
        else:
            if total_observed == 0:
                empty_table = path_uc_observed
            else:
                empty_table = path_uc_expected
            return False, f'{empty_table} is empty! Check the Unity Catalog and delta tables.'

    @staticmethod
    def check_if_two_df_have_same_schema(schema_expected: StructType, schema_observed: StructType) -> Tuple[bool, str]:
        """
        Check if two Spark DataFrames have the same schema.
        #### Args:
            - schema_expected (pyspark.sql.types.StructType): The expected schema.
            - schema_observed (pyspark.sql.types.StructType): The observed schema.
        #### Returns:
            - Tuple[bool, str]: A tuple where the first element is a boolean indicating if the schemas are equal,
                            and the second element is a message describing the result.
        """
        list_expected = sorted([(field.name, field.dataType) for field in schema_expected.fields])
        list_observed = sorted([(field.name, field.dataType) for field in schema_observed.fields])
        diff_expected_observed = set(list_expected) - set(list_observed)
        diff_observed_expected = set(list_observed) - set(list_expected)
        
        # great expectations output
        if list_expected == list_observed:
            return True, 'The expected schema and schema_json from file are equal.'
        else:
            return False, f'''The schemas are not equal! Check the schema_json declared in file. schema_expected - schema_observed = {diff_expected_observed} __ | __ schema_observed - schema_expected = {diff_observed_expected}'''

    @staticmethod
    def check_if_data_precision_is_equal(
        schema_df_source: StructType,
        df_source_name: str,
        schema_df_target: StructType,
        df_target_name: str,
    ) -> tuple[bool, str]:
        """
        ### Checks if the data precision of DecimalType columns in two df schemas are equal.

        #### Args:
            schema_df_source (StructType): The source of truth, expected
            df_source_name (str): e.g.: `on-premises`
            schema_df_target (StructType): The schema of the target, observed.
            df_target_name (str): e.g.: `azure`

        #### Returns:
            * bool: indicating if data precision is True or False
            * str: error message
        """
        cols_prec_df_source = get_cols_with_data_precision(schema_df_source)
        cols_prec_df_target = get_cols_with_data_precision(schema_df_target)

        if sorted(cols_prec_df_source) == sorted(cols_prec_df_target):
            return True, ""
        else:
            error_msg = (
                f"Mismatch data precision between cols! "
                f"The cols with data precision in {df_source_name}: {cols_prec_df_source} | " # fmt: off # noqa
                f"The cols with data precision in {df_target_name}: {cols_prec_df_target}"
            )
            return False, error_msg
