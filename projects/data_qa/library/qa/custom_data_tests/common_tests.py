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

from library.dataloader.defaults import Defaults as col_bronze
from library.datacleaner.defaults import Defaults as col_silver
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.utils import LogTag
from library.qa.utils import (
    get_common_cols,
    find_df_diff,
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
            error_msg = f"""Total rows that only exist in {df_expected_name}: {total_df_diff_src_trg} 
                        | Total rows that only exist in {df_observed_name}: {total_df_diff_trg_src} 
                        | Analysed for: {df_expected.columns}"""
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
