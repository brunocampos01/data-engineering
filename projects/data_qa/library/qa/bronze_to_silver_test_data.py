"""
### Module - BronzeToSilverTestData for QA Library

#### Class:
    * BronzeToSilverTestData: Tests the data transformation bronze to silver and perform data expectations
"""
import os
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
from pyspark.sql.functions import (
    col,
    size,
    when,
    countDistinct,
)
from pyspark.sql.types import (
    StructField, 
    StructType,
    ArrayType,
    StringType,
)

from library.datacleaner.cleansing_facade import CleansingFacade
from library.datacleaner.validation_facade import ValidationFacade
from library.datacleaner.bronze_to_silver import BronzeToSilver

from library.datacleaner.defaults import Defaults as col_silver
from library.dataloader.defaults import Defaults as col_bronze
from library.datalander.defaults import Defaults as col_landing
from library.great_expectations.util import add_custom_result_to_validation
from delta.tables import DeltaTable
from library.qa.template_data_tests import TemplateDataTest
from library.qa.schema_helper import SchemaHelper
from library.qa.custom_data_tests.silver_qa import SilverQA as silver_test
from library.qa.utils import (
    initialize_and_prepare_delta,
    get_diff_cols_between_df,
    get_cols_with_data_scale,
    get_cols_with_data_precision,
    check_if_two_tuple_are_equal,
    find_df_diff,
    parser_list_cols_to_str,
    check_if_two_df_are_not_empty,
    check_if_two_df_have_same_schema,
    check_if_data_path_is_correct,
    check_if_table_type_is_correct,
    check_if_table_data_format_is_correct,
)


class BronzeToSilverTestData(TemplateDataTest):
    """
    ### Class for testing the data transformation bronze to silver and performing data expectations.

    #### Functions:
        * transform_bronze_to_silver: Transforms data from the bronze table to the silver table
        * execute_data_expectations: Executes data expectations on the transformed bronze DataFrame and the silver DataFrame
    """
    def __init__(
        self,
        spark: SparkSession,
        schema_bronze_name: str,
        table_bronze_name: str,
        schema_silver_name: str,
        table_silver_name: str,
        dict_schema_expected: Dict,
        list_pk_cols: List[str],
        list_order_by_cols: List[str] = None,
        list_phone_columns: List[str] = None,
        list_email_columns: List[str] = None,
        list_skipped_cleansing_rules: List[str] = None,
        list_exception_corrupted_records: List[str] = None,
    ):
        """
        ### Initialize Class

        #### Args:
            * spark (SparkSession): SparkSession
            * schema_bronze_name (str): UnityCatalog schema bronze name
            * table_bronze_name (str): UnityCatalog table bronze name
            * schema_silver_name (str): UnityCatalog schema silver name
            * table_silver_name (str): UnityCatalog table silver name
            * dict_schema_expected (dict): the json that contains the expected schema
            * list_pk_cols (List[str]): list of PrimaryKey columns
            * list_order_by_cols (List[str]): list of OrderBy columns
            * list_skipped_cleansing_rules (List[str]): Rules that shouldn't be applied
        """
        super().__init__()
        self.spark = spark
        self.step_layer = 'silver'
        self.catalog_bronze = f'{self.env}_bronze'
        self.catalog_silver = f'{self.env}_silver'
        self.schema_bronze_name = schema_bronze_name
        self.table_bronze_name = table_bronze_name
        self.schema_silver_name = schema_silver_name
        self.table_silver_name = table_silver_name
        self.path_uc_bronze_table = f'{self.catalog_bronze}.{self.schema_bronze_name}.{self.table_bronze_name}'.strip().lower()
        self.path_uc_silver_table = f'{self.catalog_silver}.{self.schema_silver_name}.{self.table_silver_name}'.strip().lower()
        self.list_pk_cols = list_pk_cols
        self.list_order_by_cols = list_order_by_cols
        self.dict_schema_expected = dict_schema_expected
        self.list_phone_columns = list_phone_columns
        self.list_email_columns = list_email_columns
        self.list_skipped_cleansing_rules = list_skipped_cleansing_rules
        self.list_exception_corrupted_records = list_exception_corrupted_records

    @staticmethod
    def __get_list_silver_cols() -> List[str]:
        """
        Get techical cols + tracking cols that must exists in silver

        #### Returns:
        - List[str]: A list of silver columns.
        """
        tracking_columns_expr: dict = {column.column_name: column.column_expression
                                       for column in col_silver.tracking_columns()}
        list_tracking_column_names = list(tracking_columns_expr.keys())
        list_technical_column_names = col_silver.technical_column_names()

        return list(set(list_tracking_column_names + list_technical_column_names))

    def __get_list_pk_cols(self, df_expected: DataFrame) ->  List[str]:
        """
        If was not declared PK in parameter, the code will check if exists as PK constraint
        but if not, used the list= of business cols as PK.

        NOTES:
            It is not possible to to check the null values.
        """
        if len(self.list_pk_cols) == 0:
            try:
                str_pk = self.spark.sql(f'DESCRIBE EXTENDED {self.path_uc_silver_table}') \
                    .filter("data_type like ('%PRIMARY KEY%')")\
                    .select("data_type") \
                    .collect()[0][0]
                str_pk_cleansed = str_pk.replace('PRIMARY KEY (`', '').replace('`', '').replace(')', '')
                return [x.strip().lower() for x in str_pk_cleansed.split(",")]
            except Exception:
                self.logger.warning('Not found list_pk neither PK as constraints, using list_business_cols as PK.')
                return self.__get_list_business_cols(df_expected)
        else: 
            return self.list_pk_cols

    def __get_list_business_cols(self, df_expected: DataFrame) -> List[str]:
        """Extracts business columns from the expected DataFrame.

        #### Args:
        - df_expected (DataFrame): The DataFrame from which business columns are to be extracted.

        #### Returns:
        - List[str]: A list of business columns extracted from the DataFrame.
        """
        list_cols_expected = [col.lower() for col in df_expected.columns]
        set_cols = set(list_cols_expected) - set(self.__get_list_silver_cols())
        return list(set_cols)

    def __get_list_silver_cols_must_not_be_null(self) -> List[str]:
        """Returns a list of silver columns that are not expected to be null.
        
        Returns:
            List[str]: A list of silver columns that are not expected to be null.

        NOTES: 
            Turriago identified in imos_report_api.cp_coalist, column 'etl_hash_key_pk' can be null.
        """
        set_cols_with_null = {
            col_silver.src_syst_effective_to,
            col_silver.src_syst_effective_from,
            col_silver.etl_none_compliant_pattern,
        }
        if col_silver.etl_hash_key_pk in self.__get_list_silver_cols() and not col_silver.etl_hash_key_pk in self.list_pk_cols:
            set_cols_with_null.add(col_silver.etl_hash_key_pk)
        
        return list(set(self.__get_list_silver_cols()) - set_cols_with_null)

    def __get_list_cols_must_not_be_null(self, df_expected: DataFrame):
        return list(set(self.list_pk_cols + self.__get_list_silver_cols_must_not_be_null()))

    def __get_list_cols_must_exists(self, df_expected: DataFrame) -> List[str]:
        return self.__get_list_silver_cols() + self.__get_list_business_cols(df_expected)

    def __generate_report_comparing_df(self, step: str, df_expected: DataFrame, df_observed: DataFrame) -> None:
        """
        ### Generates a report comparing the columns and rows of two df.
        
        #### Args:
            * step (str): The name of the transformation step. <before | after>
            * df_expected (DataFrame): (source of truth). e.g.: `silver`
            * df_observed (DataFrame): The df that will transform or already transformed. e.g.: `bronze_transformed`
        """
        diff_col_observed_expected, diff_cols_expected_observed = (
            get_diff_cols_between_df(df_expected, df_observed)
        )
        self.logger.info(f"*** {step} transformation *** ")
        self.logger.info(f"Total cols observed:                    {len(df_observed.columns)}")
        self.logger.info(f"Total cols expected:                    {len(df_expected.columns)}")
        self.logger.info(f"Total cols that exist only in bronze:   {diff_col_observed_expected}")
        self.logger.info(f"Total cols that exist only in expected: {diff_cols_expected_observed}")

    def log_execution_parameters(self) -> None:
        """
        Show the logs execution parameters.
        """
        self.logger.info('***** Execution Parameters *****')
        self.logger.info(f"environment:          {self.env}")
        self.logger.info(f'catalog_bronze:       {self.catalog_bronze}')
        self.logger.info(f'catalog_silver:       {self.catalog_silver}')
        self.logger.info(f'schema_bronze_name:   {self.schema_bronze_name}')
        self.logger.info(f'schema_silver_name:   {self.schema_silver_name}')
        self.logger.info(f'table_bronze:         {self.table_bronze_name}')
        self.logger.info(f'table_silver:         {self.table_silver_name}')
        self.logger.info(f'path_uc_bronze_table: {self.path_uc_bronze_table}')
        self.logger.info(f'path_uc_silver_table: {self.path_uc_silver_table}')
        self.logger.info(f'list_pk_cols:         {self.list_pk_cols}')
        self.logger.info(f'list_order_by_cols:   {self.list_order_by_cols}')
        self.logger.info(f'list_phone_columns:   {self.list_phone_columns}')
        self.logger.info(f'list_email_columns:   {self.list_email_columns}')
        self.logger.info(f'list_skipped_cleansing_rules: {self.list_skipped_cleansing_rules}')
        self.logger.info(f'list_exception_corrupted_records: {self.list_exception_corrupted_records}')

    def validate_parameters(self) -> None:
        # Check if the schema file is not empty or if it is a valid string
        try:
            if self.dict_schema_expected is None:
                raise ValueError('The dict_schema_expected file is empty. Check the datatypes_definition_file parameter.')
        except AttributeError:
            # If self.dict_schema_expected does not have a 'strip' method, it is not a valid string
            raise TypeError(f'The dict_schema_expected is not a valid dict, received: {type(self.dict_schema_expected)}. Check the datatypes_definition_file parameter.')

        # if list_exception_corrupted_records > 0, then the list_pk_cols > 0
        if len(self.list_exception_corrupted_records) > 0 and len(self.list_pk_cols) == 0:
            raise ValueError(f"""The list_pk_cols must have one column at least because you are using 
                             {self.list_exception_corrupted_records}. Consider analyzing to use 'etl_hash_key_pk' as a viable option.""")

        # warning msg about list_pks
        if len(self.list_pk_cols) == 0:
            self.logger.warning(f"""Are you sure that {self.path_uc_silver_table} 
                                does not contain PKs? Consider analyzing to use etl_hash_key_pk as a viable option.""")

    def execute_bronze_to_silver(self, df_observed: DataFrame, df_expected: DataFrame) -> DataFrame:
        """
        ### Applies transformations to the source DataFrame and returns the transformed DataFrame
        
        #### Args:
            - df_expected (DataFrame): The expected df (silver).
            - df_observed (DataFrame): The observed df (bronze).

        #### Returns:
            * (DataFrame): UnityCatalog silver table DataFrame with selected columns
        
        ### Notes:
            # added drop(col_landing.etl_metadata_struct_name) because can exists in bronze. e.g.: bronze.oracle_api_fscm.commonlookupslov
        """
        try:
            # follow the library/datacleaner/bronze_to_silver -> apply_transformations()
            cleansed_df = (
                CleansingFacade(
                    target_schema=df_expected.schema, 
                    ignored_rules=self.list_skipped_cleansing_rules
                )
                .apply_rules(df_observed)
            )
            tracking_columns_expr: dict = {column.column_name: column.column_expression
                                           for column in col_silver.tracking_columns()}
            cleansed_df = cleansed_df \
                .withColumns(tracking_columns_expr)\
                .drop(col_bronze.data_collection_datetime) \
                .drop(col_landing.etl_metadata_struct_name)
            cleansed_df = cleansed_df.sort(col(col_silver.src_syst_effective_from).asc())
            
            if len(self.list_pk_cols) > 0:
                cleansed_df = cleansed_df.join(
                    df_expected.select(col_silver.etl_none_compliant_pattern, *self.list_pk_cols), 
                    self.list_pk_cols, 
                    'left',
                )
            else:
                cleansed_df = cleansed_df.join(
                    df_expected.select(col_silver.etl_none_compliant_pattern, *self.__get_list_business_cols(df_expected)), 
                    self.__get_list_business_cols(df_expected), 
                    'left',
                )
            # follow the library/datacleaner/bronze_to_silver -> apply_transformations()
            validated_df = (
                ValidationFacade(
                    target_schema=df_expected.schema,
                    phone_columns=self.list_phone_columns,
                    email_columns=self.list_email_columns,
                )
                .apply_rules(cleansed_df)\
                .persist()
            )
            # sync the log msg
            time.sleep(1)

        except Exception as e:
            self.logger.error(e)
            validated_df = initialize_and_prepare_delta(self.spark, path_delta=self.path_uc_silver_table)

        return validated_df

    def get_and_prepare_data(self) -> Tuple[DataFrame, DataFrame]:
        """
        ### Transforms data from the bronze table to the silver table

        #### Returns:
            * df_observed (DataFrame): UnityCatalog bronze source table after applying cleasing rules
            * df_dest (DataFrame): UnityCatalog silver table after
        """
        df_observed = initialize_and_prepare_delta(self.spark, path_delta=self.path_uc_bronze_table)
        df_expected = initialize_and_prepare_delta(self.spark, path_delta=self.path_uc_silver_table)

        self.__generate_report_comparing_df(step='before', df_expected=df_expected, df_observed=df_observed)
        df_observed = self.execute_bronze_to_silver(df_observed, df_expected)
        self.__generate_report_comparing_df(step='after', df_expected=df_expected, df_observed=df_observed)

        return df_observed, df_expected

    def execute_custom_data_tests(
        self,
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        kwargs: Dict = None,
    ) -> Dict:
        """
        Executes custom data tests comparing two DataFrames.

        #### Args:
            df_expected (DataFrame): The expected DataFrame.
            df_expected_name (str): The name of the expected DataFrame.
            df_observed (DataFrame): The observed DataFrame.
            df_observed_name (str): The name of the observed DataFrame.
            kwargs (Dict, optional): Additional arguments. Defaults to None.

        #### Returns:
            Dict: A dictionary containing the results of various data tests.
        """
        schema_expected = kwargs["schema_expected"]
        schema_observed = kwargs["schema_observed"]
        list_cols_precision_expected = kwargs["list_cols_precision_expected"]
        list_cols_precision_observed = kwargs["list_cols_precision_observed"]
        list_cols_scale_expected = kwargs["list_cols_scale_expected"]
        list_cols_scale_observed = kwargs["list_cols_scale_observed"]

        table_are_not_empty = check_if_two_df_are_not_empty(self.spark, df_expected, self.path_uc_bronze_table, self.path_uc_silver_table)
        have_same_schema = check_if_two_df_have_same_schema(schema_expected, schema_observed)
        is_correct_storage_location = check_if_data_path_is_correct(self.spark, self.path_uc_silver_table, 'cleansed_sources')
        is_correct_table_type = check_if_table_type_is_correct(self.spark, self.path_uc_silver_table)
        is_correct_data_format = check_if_table_data_format_is_correct(self.spark, self.path_uc_silver_table)
        have_same_owner = silver_test.check_if_tables_have_same_owner(self.spark, self.path_uc_bronze_table, self.path_uc_silver_table)
        have_same_count_distinct = silver_test.check_if_table_have_same_count_distinct(self.__get_list_pk_cols(df_expected), df_expected, df_observed)
        have_not_corrupt_rows_bronze = silver_test.check_if_have_not_corrupt_rows_bronze(
            spark=self.spark, 
            path_observed=self.path_uc_bronze_table, 
            list_exception_corrupted_records=self.list_exception_corrupted_records,
        )
        have_valid_flag_bronze = silver_test.check_if_have_valid_flag_bronze(
            spark=self.spark, 
            list_pk=self.__get_list_pk_cols(df_expected), 
            df_observed=df_observed, 
            path_observed=self.path_uc_bronze_table,
        )
        validation_rules_not_issues = silver_test.check_if_validation_rules_not_issues(
            list_pk=self.__get_list_pk_cols(df_expected), 
            df_expected=df_expected, 
            df_observed=df_observed,
            list_email_columns=self.list_email_columns,
            list_phone_columns=self.list_phone_columns,
        )
        dict_col_have_same_count_distinct = {
            c: silver_test.check_if_col_have_same_count_distinct(
                list_pk=self.__get_list_pk_cols(df_expected),
                col_name=c, 
                df_expected=df_expected, 
                df_observed=df_observed,
                catalog_observed=self.catalog_bronze,
                catalog_expected=self.catalog_silver,
            )
            for c in self.__get_list_business_cols(df_expected)
        }
        dict_are_same_precision = {
            c_expected[0]: check_if_two_tuple_are_equal(c_expected, c_observed)
            for c_expected, c_observed in zip(list_cols_precision_expected, list_cols_precision_observed)
        }
        dict_are_same_scale = {
            c_expected[0]: check_if_two_tuple_are_equal(c_expected, c_observed)
            for c_expected, c_observed in zip(list_cols_scale_expected, list_cols_scale_observed)
        }

        return {
            "dict_are_same_precision": dict_are_same_precision,
            "dict_are_same_scale": dict_are_same_scale,
            "dict_col_have_same_count_distinct": dict_col_have_same_count_distinct,
            "table_are_not_empty": table_are_not_empty,
            "have_same_schema": have_same_schema,
            "have_same_owner": have_same_owner,
            "have_same_count_distinct": have_same_count_distinct,
            "is_correct_storage_location": is_correct_storage_location,
            "is_correct_table_type": is_correct_table_type,
            "is_correct_data_format": is_correct_data_format,
            "validation_rules_not_issues": validation_rules_not_issues,
            "have_not_corrupt_rows_bronze": have_not_corrupt_rows_bronze,
            "have_valid_flag_bronze": have_valid_flag_bronze,
        }

    def execute_great_expectations(
        self,
        df_observed: DataFrame,
        df_expected: DataFrame,
        dict_result_custom_tests: Dict,
        kwargs: Dict = None,
    ) -> Dict:
        """
        ### Executes data expectations on the transformed bronze DataFrame and the silver DataFrame.

        #### Args:
            * df_observed (DataFrame): The transformed bronze DataFrame.
            * df_expected (DataFrame): The silver DataFrame.
            * dict_result_custom_tests (Dict): result from custom data tests.
            * kwargs (Dict, optional): Additional arguments. Defaults to None.

        #### Returns:
            * validation_result (dict): A JSON-formatted dictionary containing a list of the validation results.
        """
        schema_expected = kwargs["schema_expected"] # from json file
        list_cols_scale_expected = kwargs['list_cols_scale_expected']
        list_cols_precision_expected = kwargs['list_cols_precision_expected']
        expectations = ExpectationSuite(expectation_suite_name=f"bronze-to-silver-{self.schema_silver_name}.{self.table_silver_name}")
        gdf_observed = SparkDFDataset(df_observed)

        # not_null
        # execute for PKs and silver cols
        self.logger.info("Executing: expect_column_values_to_not_be_null")
        for c in self.__get_list_cols_must_not_be_null(df_expected):
            exp = ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    'column': c,
                },
            )
            expectations.add_expectation(exp)

        # column_to_exist
        self.logger.info("Executing: expect_column_to_exist")
        for c in self.__get_list_cols_must_exists(df_expected):
            exp = ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={
                    'column': c,
                },
            )
            expectations.add_expectation(exp)

        # type
        self.logger.info("Executing: expect_column_values_to_be_of_type")
        list_col_names_target = [f.name.lower() for f in schema_expected]
        list_col_types_target = [str(type(f.dataType).__name__) for f in schema_expected]
        for col_name, col_type in zip(list_col_names_target, list_col_types_target):
            exp = ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={
                    'column': col_name,
                    'type_': col_type,
                },
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
        # precision
        if len(list_cols_precision_expected) > 0:
            self.logger.info("Executing: expect_column_same_precision")
            dict_are_same_precision = dict_result_custom_tests["dict_are_same_precision"]
            for col_name in dict_are_same_precision.keys():
                exp = ExpectationValidationResult(
                    success=(dict_are_same_precision[col_name][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_precision",
                        kwargs={
                            'column': col_name,
                            "result": dict_are_same_precision[col_name][0],
                            "msg": dict_are_same_precision[col_name][1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

        # scale
        if len(list_cols_scale_expected) > 0:
            self.logger.info("Executing: expect_column_same_scale")
            dict_are_same_scale = dict_result_custom_tests["dict_are_same_scale"]
            for col_name in dict_are_same_scale.keys():
                exp = ExpectationValidationResult(
                    success=(dict_are_same_scale[col_name][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_scale",
                        kwargs={
                            'column': col_name,
                            "result": dict_are_same_scale[col_name][0],
                            "msg": dict_are_same_scale[col_name][1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

        # count distinct by col
        self.logger.info("Executing: expect_column_have_same_count_distinct")
        dict_col_have_same_count_distinct = dict_result_custom_tests["dict_col_have_same_count_distinct"]
        list_business_cols = self.__get_list_business_cols(df_expected)
        for c in list_business_cols:
            exp = ExpectationValidationResult(
                success=(dict_col_have_same_count_distinct[c][0] is True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_same_count_distinct",
                    kwargs={
                        'column': c,
                        "result": dict_col_have_same_count_distinct[c][0],
                        "msg": dict_col_have_same_count_distinct[c][1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # count by table
        self.logger.info("Executing: expect_table_have_same_count_distinct")
        have_same_count_distinct = dict_result_custom_tests["have_same_count_distinct"]
        exp = ExpectationValidationResult(
            success=(have_same_count_distinct[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_count_distinct",
                kwargs={
                    "result": have_same_count_distinct[0],
                    "msg": have_same_count_distinct[1],
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # table_are_not_empty
        self.logger.info("Executing: expect_table_are_not_empty")
        table_are_not_empty = dict_result_custom_tests["table_are_not_empty"]
        exp = ExpectationValidationResult(
            success=(table_are_not_empty[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_are_not_empty",
                kwargs={
                    "result": table_are_not_empty[0],
                    "msg": table_are_not_empty[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # schema
        self.logger.info("Executing: expect_table_have_same_schema")
        have_same_schema = dict_result_custom_tests["have_same_schema"]
        exp = ExpectationValidationResult(
            success=(have_same_schema[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_schema",
                kwargs={
                    "result": have_same_schema[0],
                    "msg": have_same_schema[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # owner
        self.logger.info("Executing: expect_table_have_same_owner")
        have_same_owner = dict_result_custom_tests["have_same_owner"]
        exp = ExpectationValidationResult(
            success=(have_same_owner[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_owner",
                kwargs={
                    "result": have_same_owner[0],
                    "msg": have_same_owner[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # data path
        self.logger.info("Executing: expect_table_correct_storage_location")
        is_correct_storage_location = dict_result_custom_tests["is_correct_storage_location"]
        exp = ExpectationValidationResult(
            success=(is_correct_storage_location[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_storage_location",
                kwargs={
                    "result": is_correct_storage_location[0],
                    "msg": is_correct_storage_location[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # table type
        self.logger.info("Executing: expect_table_correct_table_type")
        is_correct_table_type = dict_result_custom_tests["is_correct_table_type"]
        exp = ExpectationValidationResult(
            success=(is_correct_table_type[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_table_type",
                kwargs={
                    "result": is_correct_table_type[0],
                    "msg": is_correct_table_type[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # data format
        self.logger.info("Executing: expect_table_correct_data_format")
        is_correct_data_format = dict_result_custom_tests["is_correct_data_format"]
        exp = ExpectationValidationResult(
            success=(is_correct_data_format[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_data_format",
                kwargs={
                    "result": is_correct_data_format[0],
                    "msg": is_correct_data_format[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # validation facade rules was executed without errors
        self.logger.info("Executing: expect_validation_rules_not_issues")
        validation_rules_not_issues = dict_result_custom_tests["validation_rules_not_issues"]
        exp = ExpectationValidationResult(
            success=(validation_rules_not_issues[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_validation_rules_not_issues",
                kwargs={
                    "result": validation_rules_not_issues[0],
                    "msg": validation_rules_not_issues[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # corrupted records
        self.logger.info("Executing: expect_bronze_have_not_corrupt_rows")
        have_not_corrupt_rows_bronze = dict_result_custom_tests["have_not_corrupt_rows_bronze"]
        exp = ExpectationValidationResult(
            success=(have_not_corrupt_rows_bronze[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_have_not_corrupt_rows_bronze",
                kwargs={
                    "result": have_not_corrupt_rows_bronze[0],
                    "msg": have_not_corrupt_rows_bronze[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # deleted flag
        self.logger.info("Executing: expect_have_valid_deleted_flag_bronze")
        have_valid_flag_bronze = dict_result_custom_tests["have_valid_flag_bronze"]
        exp = ExpectationValidationResult(
            success=(have_valid_flag_bronze[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_have_valid_deleted_flag_bronze",
                kwargs={
                    "result": have_valid_flag_bronze[0],
                    "msg": have_valid_flag_bronze[1]
                },
            ),
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
        self.logger.info("========== data test session starts ==========")
        schema_observed, schema_expected = SchemaHelper.get_and_prepare_schemas(self.dict_schema_expected, df_observed.schema)
        list_cols_precision_expected = get_cols_with_data_precision(schema_expected)
        list_cols_precision_observed = get_cols_with_data_precision(schema_observed)
        list_cols_scale_expected = get_cols_with_data_scale(schema_expected)
        list_cols_scale_observed = get_cols_with_data_scale(schema_observed)
        kwargs = {
            'schema_observed': schema_observed,
            'schema_expected': schema_expected,
            'list_cols_precision_expected': list_cols_precision_expected,
            'list_cols_precision_observed': list_cols_precision_observed,
            'list_cols_scale_expected': list_cols_scale_expected,
            'list_cols_scale_observed': list_cols_scale_observed,
        }
        self.logger.info(f'List PKs: {self.__get_list_pk_cols(df_expected)}')
        self.logger.info(f'List silver cols: {self.__get_list_silver_cols()}')
        self.logger.info(f'List business cols: {self.__get_list_business_cols(df_expected)}')

        dict_result_custom_tests = self.execute_custom_data_tests(
            df_expected=df_expected,
            df_expected_name=df_expected_name,
            df_observed=df_observed,
            df_observed_name=df_observed_name,
            kwargs=kwargs,
        )
        validation_results = self.execute_great_expectations(
            df_expected=df_expected,
            df_observed=df_observed,
            dict_result_custom_tests=dict_result_custom_tests,
            kwargs=kwargs,
        )
        self.logger.info("========== finished running the data tests ==========")
        
        return validation_results
