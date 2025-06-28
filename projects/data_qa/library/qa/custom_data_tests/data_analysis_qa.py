"""
    * check_if_dim_is_not_empty: Checks if dimension table is not empty
    * check_if_two_df_contain_same_schema: Checks if two df have the same schema based on a list of common columns.
    * check_if_two_df_contain_diff_content_rows: Checks if two DataFrames contain different content rows
    * check_if_two_df_contain_diff_content_rows_count: Checks if two DataFrames contain different content rows count
    * check_if_two_df_contain_same_count: Checks if two DataFrames contain same count
    * check_if_two_df_contain_same_count_per_column: Counts total rows per column in two DataFrames and compare them.
    * check_if_two_df_contain_same_unique_count_per_column: Checks if two DataFrames have the same unique count per column
    * check_if_two_df_contain_same_null_count_per_column: Checks if two df have the same null count per column.
"""
import json
import time
from typing import (
    List, 
    Dict, 
    Tuple,
)

from pyspark.sql import (
    DataFrame, 
    SparkSession,
)
from pyspark.sql.functions import col

from library.qa.utils import (
    get_common_cols,
    find_df_diff,
)


logger = LoggerProvider.get_logger()


class DataAnalysisQA:
    """
    Usefull to validate manualy data.
    Contain data tests without great expectations patterns
    """
    def _check_if_two_df_contain_same_schema(df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Checks if two df have the same schema based on a list of common columns.
        
        #### Args:
            * df_expected (DataFrame): expected DataFrame
            * df_observed (DataFrame): observed DataFrame
        """
        if df_observed.schema == df_expected.schema:
            logger.info(f"schema: [OK]")
        else:
            logger.error(f"schema: [FAILED] Expected: {df_expected.schema} | Observed: {df_observed.schema}")

    def _check_if_two_df_contain_diff_content_rows(df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Checks if two DataFrames contain different content rows

        #### Args:
            * df_expected (DataFrame): expected DataFrame
            * df_observed (DataFrame): observed DataFrame
        """
        df_diff_src_tgt, df_diff_tgt_src = find_df_diff(df_expected, df_observed)
        total_df_diff_src_trg = df_diff_src_tgt.count()
        total_df_diff_trg_src = df_diff_tgt_src.count()

        if total_df_diff_src_trg == 0 and total_df_diff_trg_src == 0:
            logger.info(f'diff_content_rows: [OK] Both df have same content rows total')
        else:
            logger.error(f'diff_content_rows: [FAILED] DF have diff content rows !'
                        f' Showing the rows only exists in df_expected:\n {df_diff_tgt_src.display()}'
                        f'|  Showing the rows only exists in df_observed:\n {df_diff_src_tgt.display()}\n')

    def _check_if_two_df_contain_diff_content_rows_count(df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Checks if two DataFrames contain different content rows count
        
        #### Args:
            * df_expected (DataFrame): expected DataFrame
            * df_observed (DataFrame): observed DataFrame
        """
        df_diff_source_target, df_diff_target_source = find_df_diff(df_expected, df_observed)
        total_df_diff_src_trg = df_diff_source_target.count()
        total_df_diff_trg_src = df_diff_target_source.count()

        if total_df_diff_src_trg == 0 and total_df_diff_trg_src == 0:
            logger.info(f'diff_content_rows_count: [OK] Both df have same content rows')
        else:
            logger.error(f'diff_content_rows_count: [FAILED] DF have diff content rows ! '
                        f'Total rows only exists in df_expected: {total_df_diff_src_trg}'
                        f' | Total rows only exists in df_observed: {total_df_diff_trg_src}'
                        f' | Checked for these cols: {df_expected.columns}')

    def _check_if_two_df_contain_same_count(df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Checks if two DataFrames contain same count
        
        #### Args:
            * df_expected (DataFrame): expected DataFrame
            * df_observed (DataFrame): observed DataFrame
        """
        count_df_expected = df_expected.count()
        count_df_observed = df_observed.count()

        if count_df_expected == count_df_observed:
            logger.info(f'count: [OK] Both df have same total: {count_df_expected}')
        else:
            logger.error(f'count:[FAILED] DF have diff total rows !'
                        f' df_expected = {count_df_expected} | df_observed = {count_df_observed}')

    def _check_if_two_df_contain_same_count_per_column(df_expected: DataFrame, df_observed: DataFrame, common_cols: List) -> None:
        """
        ### Counts total rows per column in two DataFrames and compare them.
        
        #### Args:
            * df_source (DataFrame): The source of truth, df_expected
            * df_target (DataFrame): The second DataFrame.
            * common_cols (list): list of common columns
        """
        for c in common_cols:
            total_rows_col_df1 = df_expected.select(c).count()
            total_rows_col_df2 = df_observed.select(c).count()

            if total_rows_col_df1 == total_rows_col_df2:
                logger.info(f"count_per_column: [OK] {total_rows_col_df1} in '{c}'"
                            f" the count is equal in both df")
            else:
                logger.error(f"count_per_column: [FAILED] Column '{c}' has diff total rows! "
                            f"Expected: {total_rows_col_df1} | Observed: {total_rows_col_df2}")

    def _check_if_two_df_contain_same_unique_count_per_column(df_expected: DataFrame, df_observed: DataFrame, common_cols: List) -> None:
        """
        ### Checks if two df have the same unique count per col. 
        This function compares the number of unique values in each column of the provided df

        #### Args:
            * df_expected (DataFrame): The DataFrame representing the expected data.
            * df_observed (DataFrame): The DataFrame representing the observed data.
        """
        for c in common_cols:
            expected_count = df_expected.select(c).distinct().count()
            observed_count = df_observed.select(c).distinct().count()

            if expected_count == observed_count:
                logger.info(f"unique_count_per_column: [OK] {expected_count} in '{c}'"
                            f" the count is equal in both df")
            else:
                logger.error(f"unique_count_per_column: [FAILED] Column '{c}' has diff unique counts:"
                            f" Expected: {expected_count} | Observed: {observed_count}")

    def _check_if_two_df_contain_same_null_count_per_column(df_expected: DataFrame, df_observed: DataFrame, common_cols: List) -> None:
        """
        ### Checks if two df have the same null count per column.
        
        #### Args:
            * df_expected (DataFrame): The expected DataFrame for comparison.
            * df_observed (DataFrame): The observed DataFrame for comparison.
        """
        for c in common_cols:
            expected_null_count = df_expected.select(c).filter(col(c).isNull()).count()
            observed_null_count = df_observed.select(c).filter(col(c).isNull()).count()

            if expected_null_count == observed_null_count:
                logger.info(f"null_count_per_column: [OK] {expected_null_count} in '{c}'"
                            f" the count is equal in both df")
            else:
                logger.error(f"null_count_per_column: [FAILED] Column '{c}' has diff null counts: "
                            f"Expected: {expected_null_count} | Observed: {observed_null_count}")

    def execute_data_analysis(self, df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Generate data analysis based on the comparison between two df.
        ### This function performs various validations and comparisons between two df

        #### Args:
            * df_expected (DataFrame): expected DataFrame
            * df_observed (DataFrame): observed DataFrame
        """
        list_common_cols = get_common_cols(df_expected, df_observed)
        self.logger.info(f'list_common_cols: {list_common_cols}')

        # validations
        DataframeQA._check_if_two_df_contain_same_schema(df_expected, df_observed)
        DataframeQA._check_if_two_df_contain_same_count(df_expected, df_observed)
        DataframeQA._check_if_two_df_contain_same_count_per_column(df_expected, df_observed, list_common_cols)
        DataframeQA._check_if_two_df_contain_same_unique_count_per_column(df_expected, df_observed, list_common_cols)
        DataframeQA._check_if_two_df_contain_same_null_count_per_column(df_expected, df_observed, list_common_cols)
        DataframeQA._check_if_two_df_contain_diff_content_rows_count(df_expected, df_observed)
        DataframeQA._check_if_two_df_contain_diff_content_rows(df_expected, df_observed)
