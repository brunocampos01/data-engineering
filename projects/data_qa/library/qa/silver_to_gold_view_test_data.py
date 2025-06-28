"""
#### Class:
    * SilverToGoldTestData: Class used to test only views
"""
import json
import logging
import time
from typing import (
    List,
    Dict,
    Tuple,
    Union,
)

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult
from pyspark.sql import(
    DataFrame,
    SparkSession,
)
from pyspark.sql.types import DecimalType, TimestampType, DoubleType, FloatType
from pyspark.sql.functions import (
    col,
    regexp_replace,
    regexp_extract,
    split,
    lower,
    concat,
    lit,
    when,
    date_trunc,
    round,
    ceil,
    expr,
    to_timestamp,
    trim,
)

from library.database.sqlserver_dw_sql import SQLServerDWSql

from library.great_expectations.util import add_custom_result_to_validation
from library.dataserve.defaults import Defaults as col_gold
from library.qa.schema_helper import SchemaHelper
from library.qa.template_data_tests import TemplateDataTest
from library.qa.great_expectations_helper import GreatExpectationsHelper
from library.qa.custom_data_tests.gold_qa import GoldQA as gold_test
from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.qa.timestamp_helper import TimestampHelper
from library.qa.utils import (
    get_cols_with_data_precision,
    get_cols_with_data_scale,
    initialize_and_prepare_delta,
    check_if_two_tuple_are_equal,
)

class SilverToGoldViewTestData(TemplateDataTest):
    """
    ### Class for testing the data between azure magellan and Unity Catalog environment. This class following the skeleton define at library.qa.template_data_tests
    """
    def __init__(
        self,
        spark: SparkSession,
        sql_database: str,
        catalog_db_name: str,
        schema_db_name: str,
        view_db_name: str,
        schema_gold_name: str,
        view_gold_name: str,
    ):
        super().__init__()
        self.spark = spark
        self.step_layer = 'gold'
        self.sql_database = sql_database
        self.catalog_db_name = catalog_db_name
        self.schema_db_name = schema_db_name
        self.view_db_name = view_db_name
        self.schema_gold_name = schema_gold_name
        self.view_gold_name = view_gold_name
        self.catalog_gold_name = f'{self.env}_gold'
        self.path_azure_table = f'{self.catalog_db_name}.{self.schema_db_name}.{self.view_db_name}'.strip().lower()
        self.path_gold_view = f'{self.catalog_gold_name}.{self.schema_gold_name}.{self.view_gold_name}'.strip().lower()

    def get_and_prepare_data(self, kwargs: dict = None) -> tuple[DataFrame, DataFrame]:
        df_uc = self.spark.table(self.path_gold_view)
        df_uc.display()
        
        loader = SQLServerDWSql()
        df_onpremises = self.spark.read.format('jdbc')\
            .options(**loader.options(self.catalog_db_name))\
            .option("query", f'select * from {self.schema_db_name}.{self.view_db_name}')\
            .load()
        df_onpremises.display()

        self.logger.info(f'Total rows in df_uc = {df_uc.count()} ')
        self.logger.info(f'Total rows in df_onpremises = {df_onpremises.count()}')
        self.logger.info('Check if in both sides these numbers are correct!')

        return df_uc, df_onpremises

    def execute_great_expectations(
        self,
        df_expected: DataFrame,
        df_observed: DataFrame,
        kwargs: Dict = None,
    ) -> Dict:
        expectations = ExpectationSuite(expectation_suite_name=f"silver-to-gold-view-{self.schema_gold_name}.{self.view_gold_name}")
        gdf_observed = SparkDFDataset(df_observed)
        list_cols_precision_expected = kwargs["list_cols_precision_expected"]
        list_cols_precision_observed = kwargs["list_cols_precision_observed"]
        list_cols_scale_expected = kwargs["list_cols_scale_expected"]
        list_cols_scale_observed = kwargs["list_cols_scale_observed"]

        # col_exists
        for c in df_expected.columns:
            self.logger.info(f"expect_col_to_exist: {c}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": c},
            )
            expectations.add_expectation(exp)

        # type
        list_col_names_observed = [c for c in df_observed.columns]
        list_col_types_observed = [str(type(df_observed.schema[c].dataType).__name__) for c in df_observed.columns]
        for col_name, col_type in zip(list_col_names_observed, list_col_types_observed):
            self.logger.info(f"expect_col_type: {col_name}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={
                    "column": col_name,
                    'type_': col_type,
                },
            )
            expectations.add_expectation(exp)

        # equal count -> table level
        self.logger.info("expect_table_row_count_to_equal")
        exp = ExpectationConfiguration(
            expectation_type="expect_table_row_count_to_equal",
            kwargs={"value": int(df_expected.count())},
        )
        expectations.add_expectation(exp)

        validation_results = gdf_observed.validate(
            expectation_suite=expectations,
            catch_exceptions=False,
            result_format='BASIC',
        )
        # --------------------------
        # complementary expectations
        # --------------------------
        # table_are_not_empty
        self.logger.info("expect_table_are_not_empty")
        tuple_results = custom_qa.check_if_two_df_are_not_empty(self.spark, df_expected, self.path_gold_view, self.path_azure_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_are_not_empty",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        if len(list_cols_scale_expected) > 0:
            # precision
            dict_are_same_precision = {
                c_expected[0]: check_if_two_tuple_are_equal(c_expected, c_observed)
                for c_expected, c_observed in zip(list_cols_precision_expected, list_cols_precision_observed)
            }
            for c in dict_are_same_precision.keys():
                self.logger.info(f"expect_column_same_precision: {c}")
                exp = ExpectationValidationResult(
                    success=(dict_are_same_precision[c][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_precision",
                        kwargs={
                            "column": c,
                            "result": dict_are_same_precision[c][0],
                            "msg": dict_are_same_precision[c][1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

            # scale
            dict_are_same_scale = {
                c_expected[0]: check_if_two_tuple_are_equal(c_expected, c_observed)
                for c_expected, c_observed in zip(list_cols_scale_expected, list_cols_scale_observed)
            }
            for c in dict_are_same_scale.keys():
                self.logger.info(f"expect_column_same_scale: {c}")
                exp = ExpectationValidationResult(
                    success=(dict_are_same_scale[c][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_scale",
                        kwargs={
                            "column": c,
                            "result": dict_are_same_scale[c][0],
                            "msg": dict_are_same_scale[c][1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

        return validation_results

    def execute_data_tests(
        self,
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        kwargs: Dict = None,
    ) -> Dict:
        self.logger.info("========== prepare data test session ==========")
        df_expected.cache()
        df_observed.cache()

        list_cols_precision_expected = get_cols_with_data_precision(df_expected.schema)
        list_cols_precision_observed = get_cols_with_data_precision(df_observed.schema)
        list_cols_scale_expected = get_cols_with_data_scale(df_expected.schema)
        list_cols_scale_observed = get_cols_with_data_scale(df_observed.schema)
        kwargs = {
            'list_cols_precision_expected': list_cols_precision_expected,
            'list_cols_precision_observed': list_cols_precision_observed,
            'list_cols_scale_expected': list_cols_scale_expected,
            'list_cols_scale_observed': list_cols_scale_observed,
        }

        self.logger.info("========== data test session starts ==========")
        validation_results = self.execute_great_expectations(
            df_expected=df_expected,
            df_observed=df_observed,
            kwargs=kwargs,
        )

        self.logger.info("========== results ==========")
        dict_filtered_results = GreatExpectationsHelper().get_dict_gx_result(validation_results)
        for key, value in dict_filtered_results.items():
            self.logger.info(f'{key}: {value}')
        self.logger.info("========== finished running the tests ==========")

        return validation_results
