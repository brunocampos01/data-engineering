"""
### Module - SilverToGoldTestData for QA Library

#### Class:
    * SilverToGoldTestData: Tests the data between azure mallegan and Unity Catalog environment
"""
import json
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
from pyspark.sql.types import DecimalType
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
)


from library.database.azure_sql import AzureSQL
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
    generate_information_schema_query,
    get_cols_with_data_precision,
    get_cols_with_data_scale,
    initialize_and_prepare_delta,
    check_if_two_df_are_not_empty,
    check_if_two_df_have_same_schema,
    check_if_data_path_is_correct,
    check_if_table_type_is_correct,
    check_if_table_data_format_is_correct,
    check_if_two_tuple_are_equal,
)


class SilverToGoldTestData(TemplateDataTest):
    """
    ### Class for testing the data between azure mallegan and Unity Catalog environment. This class following the skeleton define at library.qa.template_data_tests
    """
    def __init__(
        self,
        spark: SparkSession,
        catalog_azure_name: str,
        schema_azure_name: str,
        table_azure_name: str,
        schema_gold_name: str,
        table_gold_name: str,
        table_type: str,
        list_col_prefix: List[str],
        query: str,
        dict_schema_expected: str,
        list_skip_cols: List[str],
    ):
        super().__init__()
        self.spark = spark
        self.step_layer = 'gold'
        self.catalog_azure_name = catalog_azure_name
        self.schema_azure_name = schema_azure_name
        self.table_azure_name = table_azure_name
        self.schema_gold_name = schema_gold_name
        self.table_gold_name = table_gold_name
        self.table_type = table_type
        self.query = query
        self.dict_schema_expected = dict_schema_expected
        self.list_skip_cols = list_skip_cols
        self.list_col_prefix = list_col_prefix
        self.catalog_gold_name = f'{self.env}_gold'
        self.path_azure_table = f'{self.catalog_azure_name}.{self.schema_azure_name}.{self.table_azure_name}'.strip().lower()
        self.path_gold_table = f'{self.catalog_gold_name}.{self.schema_gold_name}.{self.table_gold_name}'.strip().lower()

    @staticmethod
    def __get_list_gold_cols() -> List[str]:
        """
        Get techical cols that must exists in gold

        Returns:
            List[str]: A list of gold columns.
            e.g.: ['etl_created_datetime', 'etl_created_job_id', ...]
        """
        return col_gold.technical_column_names()

    def __get_list_business_cols(self, df_expected: DataFrame) -> List[str]:
        """
        Extracts business columns from the expected DataFrame.

        #### Args:
        - df_expected (DataFrame): The DataFrame from which business columns are to be extracted.

        #### Returns:
            List[str]: A list of business columns extracted from the DataFrame.
            e.g.: ['country_code', 'country_name']
        """
        list_cols_expected = [col.lower() for col in df_expected.columns]
        set_cols = set(list_cols_expected) \
            - set(self.__get_list_gold_cols()) \
            - set(self.__get_list_sk_cols(df_expected)) \
            - set(self.__get_list_pk_cols())        
        all_business_cols = [c for c in list(set_cols) if not c.endswith('sk')]
        filtered_business_cols = [c for c in all_business_cols if c not in self.list_skip_cols]
        return list(set(filtered_business_cols))

    def __get_list_pk_cols(self) ->  List[str]:
        """
        Retrieves a list of primary key columns for the specified table.

        Returns:
            List[str]: A list of primary key column names. e.g.: ['sk']
        
        Notes:
            `dim_data_source` not containts PK in azure.
        """
        list_pk_expected = list(set(self.__get_sqlserver_constraints('pk')))
        if len(list_pk_expected) > 0:
            return list_pk_expected
        else:
            return self.__get_uc_constraints('pk', self.path_gold_table)

    def __get_list_sk_cols(self, df_expected: DataFrame) -> List[str]:
        """
        Returns a list of column names from the DataFrame `df_expected` that end with 'sk'.
        The idea is take the cols that will evaluate the data tests for referential integraty and fk constraint.

        Args:
            df_expected (DataFrame): The DataFrame to extract column names from.

        Returns:
            List[str]: A list of column names ending with '_sk'. e.g.: ['party_sk']
        
        Notes:
            list_exceptions contains cols that ends with _sk but acctualy is only a PK.
            e.g.: date_sk from dim_time
        """
        list_exceptions = ['date_sk']
        return [c for c in df_expected.columns 
                if c.endswith('_sk') and c not in list_exceptions] 

    def __get_list_timestamp_cols(self):
        """
        Get a list of timestamp columns from a specified table in the Azure catalog.

        Returns:
            list: A list of column names that are of type 'datetime' in the specified table.
            e.g.: ['creation_date', etl_created_date', 'etl_updated_date', ...]
        """
        query = f"""
            SELECT 
                column_name,
                data_type
            FROM information_schema.columns
            WHERE
                table_schema = '{self.schema_azure_name}' 
                AND table_name = '{self.table_azure_name}'
        """
        df = initialize_and_prepare_delta(spark=self.spark, query=query, catalog_azure_name=self.catalog_azure_name)
        df = df \
            .filter(col('data_type') == 'datetime') \
            .withColumn("column_name", lower(col('column_name')))
        list_rows = self.__remove_prefix_values(df, "column_name").collect()
        return [c.column_name for c in list_rows]

    def __get_list_gold_cols_must_not_be_null(self) -> List[str]: 
        """Returns a list of gold columns that are not expected to be null.
        
        Returns:
            List[str]: A list of gold columns that are not expected to be null.
        """
        set_cols_with_null = {
            col_gold.etl_updated_datetime,
            col_gold.etl_updated_job_id,
            col_gold.etl_updated_job_run_id,
            col_gold.etl_rec_version_end_date,
        }        
        return list(set(self.__get_list_gold_cols()) - set_cols_with_null)

    def __get_list_cols_must_exists(self, df_expected: DataFrame) -> List[str]:
        """
        Returns a list of columns that must exist based on business, gold, and primary key columns.

        Args:
            df_expected (DataFrame): The DataFrame representing the expected columns.

        Returns:
            List[str]: A list of columns that must exist.
        """
        return self.__get_list_business_cols(df_expected) \
            + self.__get_list_gold_cols() \
            + self.__get_list_pk_cols() \
            + self.__get_list_sk_cols(df_expected)

    def __get_list_cols_must_not_be_null(self, df_expected: DataFrame):
        """
        Get a list of columns that must not be null.
        This function retrieves a list of columns that are either primary key columns
        or columns marked as 'must not be null' in the gold standard.
        
        Args:
            df_expected (DataFrame): The DataFrame representing the expected columns.

        Returns:
            list: A list of column names that must not be null.
        """
        return list(set(self.__get_list_pk_cols() + self.__get_list_sk_cols(df_expected) + self.__get_list_gold_cols_must_not_be_null()))

    def __get_sqlserver_constraints(self, constr_type: str) ->  List[str]:
        """
        Get SQL Server constraints.
            e.g. from query:
            +--------+---------------+---------------+---------------+
            |col_name|constraint_name|constraint_type|     table_name|
            +--------+---------------+---------------+---------------+
            |party_sk|     FK_PARTIES|    FOREIGN KEY|DIM_PARTY_SITES|
            +--------+---------------+---------------+---------------+
        
        Args:
            constr_type (str): Flag to include foreign key constraints. Defaults to True.

        Returns:
            List[str]: a list with column that are associate with some constraints. e.g.: if fk, ['party_sk']
        """
        query = f'''   
        SELECT 
            c.column_name AS column_name,
            tc.constraint_name AS constraint_name,
            tc.constraint_type AS constraint_type,
            tc.table_name AS table_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage c ON tc.constraint_name = c.constraint_name
        '''
        df = initialize_and_prepare_delta(spark=self.spark, query=query, catalog_azure_name=self.catalog_azure_name)
        df = df.filter(col('table_name').isin(self.table_azure_name))

        if constr_type == 'fk':
            df = df.filter(col('constraint_type').isin('FOREIGN KEY'))
        elif constr_type == 'pk':
            df = df.filter(col('constraint_type').isin('PRIMARY KEY'))

        df = df.withColumn("column_name", lower(col('column_name')))
        df = self.__remove_prefix_values(df, 'column_name')

        return [row.column_name for row in df.collect()]

    def __get_uc_constraints(self, constr_type: str, path_uc: str, get_df: bool = None) -> Union[List[str], DataFrame]:
        """
        Get FK constraints from Unity Catalog table.

        Args:
            constr_type (str): Type of constraint ('fk' for Foreign Key, 'pk' for Primary Key).
            path_uc (str): Path of the Unity Catalog table.
            get_df (bool, optional): If True, returns df with FK constraints, otherwise returns list of constraint columns. Defaults to None.

        Returns:
            Union[List[str], DataFrame]: If get_df is True, returns df with FK constraints, otherwise returns list of constraint columns.
        """
        df = self.spark.sql(f'DESCRIBE EXTENDED {path_uc}')

        if constr_type == 'fk':
            pattern = r"REFERENCES\s*`([^`]+)`.`([^`]+)`.`([^`]+)`"
            df = df \
                .filter("data_type like ('%FOREIGN KEY%')") \
                .withColumn("col_name", split("data_type", "`")[1]) \
                .withColumn("layer", regexp_extract(col("data_type"), pattern, 1)) \
                .withColumn("schema", regexp_extract(col("data_type"), pattern, 2)) \
                .withColumn("table", regexp_extract(col("data_type"), pattern, 3)) \
                .withColumn("path_table", concat(lit("`"), "layer", lit("`.`"), "schema", lit("`.`"), "table", lit("`")))
            
            if get_df:
                # used in ref integrity test
                return df
            else:
                return [row.col_name for row in df.collect()]

        elif constr_type == 'pk':
            df = df \
                .filter("data_type like ('%PRIMARY KEY%')")\
                .withColumn('constraint_cols', regexp_replace(col('data_type'), r'PRIMARY KEY \(', "")) \
                .withColumn('constraint_cols', regexp_replace(col('constraint_cols'), r'\)', "")) \
                .withColumn('constraint_cols', regexp_replace(col('constraint_cols'), r'`', "")) \
                .select('constraint_cols')

            return df.collect()[0][0].split(", ")

    def _get_uc_not_null_constraint(self) -> None:
        query = f'''
            SELECT 
                ordinal_position,
                table_name,
                column_name,
                data_type,
                is_nullable
            FROM system.information_schema.columns
            WHERE
                table_catalog = '{self.catalog_gold_name}'
                AND table_schema = '{self.schema_gold_name}'
                AND table_name = '{self.table_gold_name}'
        '''
        self.logger.info(f'Executing this query in Unity Catalog:\n{query}')
        self.spark.sql(query).display()

    def _get_sqlserver_not_null_constraint(self, table_azure_name: str = None) -> None:
        if not table_azure_name:
            table_azure_name = self.table_azure_name

        query = f'''
            SELECT 
                lower(ordinal_position) AS ordinal_position,
                LOWER(table_name) AS table_name,
                LOWER(column_name) AS column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE
                table_catalog = '{self.catalog_azure_name}'
                AND table_schema = '{self.schema_azure_name}'
                AND table_name = '{table_azure_name}'
        '''
        self.logger.info(f'Executing this query in Azure SQL:\n{query}')
        initialize_and_prepare_delta(spark=self.spark, query=query, catalog_azure_name=self.catalog_azure_name).display()

    def __remove_prefix_values(self, df: DataFrame, column_name: str) -> DataFrame:
        '''
        This method removes a given prefix from values in a specific df's column.

        Args:
            df (DataFrame): The input DataFrame. e.g.:
                +--------------------+---------+
                |         column_name|data_type|
                +--------------------+---------+
                |    etl_created_date| datetime| 
                | party_creation_date| datetime|
                +--------------------+---------+
            column_name (str): The name of the column to process.

        Returns:
            DataFrame: A DataFrame with prefix removed from specified column names. e.g.:
                +----------------+---------+
                |     column_name|data_type|
                +----------------+---------+
                |etl_created_date| datetime|
                |   creation_date| datetime| changed
                +----------------+---------+
        '''
        if len(self.list_col_prefix) > 0:
            for col_prefix in self.list_col_prefix:
                df = df \
                    .filter(col(column_name) != col_prefix) \
                    .withColumn("column_name", lower(col("column_name"))) \
                    .withColumn("column_name", regexp_replace(column_name, f'^{col_prefix}s_', '')) \
                    .withColumn("column_name", regexp_replace(column_name, f'^{col_prefix}_', ''))

        return df

    def __remove_prefix_col(self, df: DataFrame) -> DataFrame:
        """
        Removes a dynamically generated prefix from column names in a DataFrame.

        Args:
            df (DataFrame): The input DataFrame to process.
            e.g.: df.columns ['vendor_site_code', 'vendor_sites_sk']

        Returns:
            DataFrame: The DataFrame with column names having the prefix removed.
            e.g.: df.columns ['code', 'sk']
        """
        if len(self.list_col_prefix) > 0:
            for col_prefix in self.list_col_prefix:
                self.logger.info(f'Removing prefix: {col_prefix} in all cols name from {self.path_azure_table}')

                for col_name in df.columns:
                    if col_name.startswith(col_prefix) and col_prefix != col_name:
                        new_col_name = col_name[len(f'{col_prefix}s_'):len(col_name)]
                        new_col_name = col_name[len(f'{col_prefix}_'):len(col_name)]
                        
                        df = df.withColumnRenamed(col_name, new_col_name)
                        
        return df

    def __handle_specific_rows(self, df: DataFrame) -> DataFrame:
        """
        Some rows present errors in expected df, such as:
            df_azure
            +--------+---------------------------+
            |asset_id|asset_description          |
            +--------+---------------------------+
            |5156    |Samsung 17 monitor         |

            but the correct is:
            +--------+---------------------------+
            |asset_id|asset_description          |
            +--------+---------------------------+
            |5152    |Samsung 17'' monitor       |
        
        This function exclude this rows from data tests.

        #### Returns:
            * df (DataFrame): A df with filtered rowss.
        """
        if self.table_gold_name == 'dim_fixed_asset':
            # some rows not contain '' (inch symbol)
            list_values_to_exclude = [5152, 5154, 5156, 5176, 5179, 5180, 5183]
            self.logger.info(f'Removing these rows based on asset_id col = {list_values_to_exclude}')
            df = df.filter(~col('asset_id').isin(list_values_to_exclude))

        elif self.table_gold_name == 'dim_time':
            list_values_to_exclude = [1975, 1976, 2050, 2051]
            self.logger.info(f'Removing these rows based on cal_year_num col = {list_values_to_exclude}')
            df = df.filter(~col('cal_year_num').isin(list_values_to_exclude))
        
        elif self.table_gold_name == 'fact_forecast':
            # the databricks round up some decimal values
            list_cols_to_round = ['amount_region', 'amount_consol']

            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 3)) \
                    .withColumn(c, col(c).cast(DecimalType(19,4)))

        return df

    def log_execution_parameters(self) -> None:
        """
        ### Execute LOG parameters

        #### Args:
            None
        """
        self.logger.info("************ Execution Parameters ************")
        self.logger.info(f"environment:        {self.env}")
        self.logger.info(f"step_layer:         {self.step_layer}")
        self.logger.info(f"table_type:         {self.table_type}")
        self.logger.info(f"catalog_gold_name:  {self.catalog_gold_name}")
        self.logger.info(f"schema_gold_name:   {self.schema_gold_name}")
        self.logger.info(f"table_gold_name:    {self.table_gold_name}")
        self.logger.info(f"path_gold_table:    {self.path_gold_table}")
        self.logger.info(f"catalog_azure_name: {self.catalog_azure_name}")
        self.logger.info(f"schema_azure_name:  {self.schema_azure_name}")
        self.logger.info(f"table_azure_name:   {self.table_azure_name}")
        self.logger.info(f"path_azure_table:   {self.path_azure_table}")
        self.logger.info(f"list_col_prefix:    {self.list_col_prefix}")

    def validate_parameters(self) -> None:
        """
        ### Validate Class parameters

        #### Args:
            None
        """
        # Check if the schema file is not empty or if it is a valid dict
        try:
            if self.dict_schema_expected is None:
                raise ValueError('The schema_json file is empty. Check the datatypes_definition_file parameter.')
            if not isinstance(self.dict_schema_expected, dict):
                raise ValueError("The schema_json does not contain a valid JSON object (dict).")
        except json.JSONDecodeError:
            # If self.schema_json does not have a 'strip' method, it is not a valid string
            raise TypeError(f'The schema_json is not a valid dict, received: {type(self.dict_schema_expected)}. Check the datatypes_definition_file parameter.')

        # Check if the query is not empty or if it is a valid string
        try:
            if self.query is None or not self.query.strip():
                raise ValueError(f'The query file is empty. Check the query file located at query_path.')
        except AttributeError:
            # If self.query does not have a 'strip' method, it is not a valid string
            self.TypeError(f'The query is not a valid string, received: {type(self.query)}!')

        # Check if the query contain alias
        if " AS " not in self.query:
            self.logger.warning(f'Are you sure that this query doesn`t need alias for columns?')

    def get_and_prepare_data(self, kwargs: dict = None) -> tuple[DataFrame, DataFrame]:
        """
        ### Get the data from gold layer in Unity Catalog and Azure Magellan database

        #### Returns:
            * df_uc (DataFrame): UnityCatalog gold source table
            * df_azure (DataFrame): Azure Magellan csldw source table
        """
        df_uc = initialize_and_prepare_delta(spark=self.spark, path_delta=self.path_gold_table)
        df_azure = initialize_and_prepare_delta(spark=self.spark, query=self.query, catalog_azure_name=self.catalog_azure_name)
        df_azure = self.__remove_prefix_col(df_azure)

        # handle float
        df_azure = SchemaHelper.set_correct_datatype(df_azure, self.dict_schema_expected)

        # sync timestamp
        list_cols_azure_timestamp = [c for c in self.__get_list_timestamp_cols() if c in self.__get_list_business_cols(df_azure)]
        df_azure = TimestampHelper.prepare_datetime_col(df_azure, list_cols_azure_timestamp)
        df_uc = TimestampHelper.prepare_datetime_col(df_uc, list_cols_azure_timestamp)

        # spefic cases
        df_azure = self.__handle_specific_rows(df_azure)
        df_uc = self.__handle_specific_rows(df_uc)

        self.logger.info(f'Total rows in df_uc = {df_uc.count()} ')
        self.logger.info(f'Total rows in df_azure = {df_azure.count()}')
        self.logger.info('Check if in both sides these numbers are correct!')

        return df_uc, df_azure

    def execute_great_expectations(
        self,
        df_expected: DataFrame,
        df_observed: DataFrame,
        kwargs: Dict = None,
    ) -> Dict:
        """
        ### Executes data expectations on the gold Unity Catalog and the Azure Magellan.

        #### Args:
            * df_observed (DataFrame): The gold DataFrame.
            * df_expected (DataFrame): The magellan DataFrame.
            * kwargs (Dict, optional): Additional arguments. Defaults to None.

        #### Returns:
            * validation_result (dict): A JSON-formatted dictionary containing a list of the validation results.
        """
        schema_observed = kwargs["schema_observed"] # from gold
        schema_expected = kwargs["schema_expected"] # from json file
        list_cols_precision_expected = kwargs["list_cols_precision_expected"]
        list_cols_precision_observed = kwargs["list_cols_precision_observed"]
        list_cols_scale_expected = kwargs["list_cols_scale_expected"]
        list_cols_scale_observed = kwargs["list_cols_scale_observed"]
        
        expectations = ExpectationSuite(expectation_suite_name=f"silver-to-gold-{self.schema_gold_name}.{self.table_gold_name}")
        gdf_observed = SparkDFDataset(df_observed)

        # not_null
        for c in self.__get_list_cols_must_not_be_null(df_expected):
            self.logger.info(f"Executing: expect_col_not_null: {c}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    'column': c,
                },
            )
            expectations.add_expectation(exp)

        # col_exists
        for c in self.__get_list_cols_must_exists(df_expected):
            self.logger.info(f"Executing: expect_col_to_exist: {c}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={
                    'column': c,
                },
            )
            expectations.add_expectation(exp)

        # type
        list_col_names_target = [f.name.lower() for f in schema_expected]
        list_col_types_target = [str(type(f.dataType).__name__) for f in schema_expected]
        for col_name, col_type in zip(list_col_names_target, list_col_types_target):
            self.logger.info(f"Executing: expect_col_type: {col_name}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={
                    'column': col_name,
                    'type_': col_type,
                },
            )
            expectations.add_expectation(exp)

        # equal count -> table level
        self.logger.info("Executing: expect_table_row_count_to_equal")
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
        # data path
        self.logger.info("Executing: expect_table_correct_storage_location")
        tuple_results = check_if_data_path_is_correct(self.spark, self.path_gold_table, 'BI')
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_storage_location",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results) 

        # owner
        self.logger.info("Executing: expect_table_have_correct_owner")
        tuple_results = gold_test.check_if_tables_have_correct_owner(self.spark, f'{self.env}_uc_catalogs_owners', self.path_gold_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_correct_owner",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # table type
        self.logger.info("Executing: expect_table_correct_table_type")
        tuple_results = check_if_table_type_is_correct(self.spark, self.path_gold_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_table_type",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # data format
        self.logger.info("Executing: expect_table_correct_data_format")
        tuple_results = check_if_table_data_format_is_correct(self.spark, self.path_gold_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_correct_data_format",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # table_are_not_empty
        self.logger.info("Executing: expect_table_are_not_empty")
        tuple_results = check_if_two_df_are_not_empty(self.spark, df_expected, self.path_gold_table, self.path_azure_table)
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

        # schema
        self.logger.info("Executing: expect_table_have_same_schema")
        have_same_schema = check_if_two_df_have_same_schema(schema_expected, schema_observed)
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

        # count by table
        self.logger.info("Executing: expect_table_count_distinct (using PK cols)")
        tuple_results = gold_test.check_if_table_have_same_count_distinct(
            self.__get_list_pk_cols(), self.__get_list_sk_cols(df_expected), df_expected, df_observed,
        )
        exp = ExpectationValidationResult(
            success=(tuple_results[0] is True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_count_distinct",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1],
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # equal count distinct -> col level
        for c in self.__get_list_business_cols(df_expected) + self.__get_list_sk_cols(df_expected):
            self.logger.info(f"Executing: expect_col_count_dist: {c}")
            tuple_results = gold_test.check_if_col_have_same_count_distinct(
                list_pk_cols=self.__get_list_pk_cols(),
                col_name=c, 
                df_expected=df_expected, 
                df_observed=df_observed, 
                path_table_observed=self.path_gold_table, 
                path_table_expected=self.path_azure_table,
            )
            exp = ExpectationValidationResult(
                success=(tuple_results[0] is True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_same_count_distinct",
                    kwargs={
                        'column': c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # equal content -> col level
        # check only business columns (without _SK and ETL_)
        for c in self.__get_list_business_cols(df_expected):
            self.logger.info(f"Executing: expect_same_content_rows: {c}")
            tuple_results = gold_test.check_same_content_rows(
                df_expected=df_expected, 
                df_observed=df_observed, 
                list_timestamp_cols=self.__get_list_timestamp_cols(),
                list_pk=self.__get_list_pk_cols(),
                c=c,
            )
            exp = ExpectationValidationResult(
                success=tuple_results[0] is True,
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_same_content_rows",
                    kwargs={
                        'column': c,
                        "result": tuple_results[0], 
                        "msg": tuple_results[1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # total null -> col level
        for c in self.__get_list_business_cols(df_expected):
            self.logger.info(f"Executing: expect_same_total_nulls: {c}")
            tuple_results = gold_test.check_if_column_have_same_total_nulls(
                df_expected=df_expected,
                df_observed=df_observed,
                col_name=c,
            )
            exp = ExpectationValidationResult(
                success=tuple_results[0] is True,
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_have_same_total_nulls",
                    kwargs={
                        'column': c,
                        "result": tuple_results[0], 
                        "msg": tuple_results[1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # pk_constraints
        for c in self.__get_list_pk_cols():
            self.logger.info(f"Executing: expect_column_have_pk_constraint: {c}")
            tuple_results = gold_test.check_if_column_have_constraint(
                list_expected_constraints=self.__get_sqlserver_constraints('pk'), 
                list_observed_constraints=self.__get_uc_constraints('pk', self.path_gold_table), 
                path_table_expected=self.path_azure_table,
                path_table_observed=self.path_gold_table,
                col_name=c,
                constr_type='pk',
            )
            exp = ExpectationValidationResult(
                success=(tuple_results[0] == True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_pk_constraint",
                    kwargs={
                        'column': c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1],
                    },
                )
            )
            add_custom_result_to_validation(exp, validation_results)

        # exists in data catalog
        self.logger.info("Executing: expect_table_exists_in_data_catalog")
        tuple_results = gold_test.check_if_table_exists_metadata_in_data_catalog(self.env, self.spark, self.path_gold_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] == True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_exists_in_data_catalog",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1],
                },
            )
        )
        add_custom_result_to_validation(exp, validation_results)

        # default values
        self.logger.info("Executing: expect_table_have_same_default_values")
        tuple_results = gold_test.check_if_table_have_same_default_values(df_observed, df_expected, self.__get_list_pk_cols())
        exp = ExpectationValidationResult(
            success=(tuple_results[0] == True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_default_values",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1],
                },
            )
        )
        add_custom_result_to_validation(exp, validation_results)

        # elapsed time
        self.logger.info("Executing: expect_table_have_same_query_elapsed_time")
        tuple_results = gold_test.check_if_table_have_same_elapsed_time(self.spark, self.path_gold_table, self.path_azure_table)
        exp = ExpectationValidationResult(
            success=(tuple_results[0] == True),
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_have_same_query_elapsed_time",
                kwargs={
                    "result": tuple_results[0],
                    "msg": tuple_results[1],
                },
            )
        )
        add_custom_result_to_validation(exp, validation_results)

        # --------------------------
        # spefic situations
        # --------------------------
        if len(list_cols_scale_expected) > 0:
            # precision
            dict_are_same_precision = {
                c_expected[0]: check_if_two_tuple_are_equal(c_expected, c_observed)
                for c_expected, c_observed in zip(list_cols_precision_expected, list_cols_precision_observed)
            }
            for c in dict_are_same_precision.keys():
                self.logger.info(f"Executing: expect_column_same_precision: {c}")
                exp = ExpectationValidationResult(
                    success=(dict_are_same_precision[c][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_precision",
                        kwargs={
                            'column': c,
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
                self.logger.info(f"Executing: expect_column_same_scale: {c}")
                exp = ExpectationValidationResult(
                    success=(dict_are_same_scale[c][0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_same_scale",
                        kwargs={
                            'column': c,
                            "result": dict_are_same_scale[c][0],
                            "msg": dict_are_same_scale[c][1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

        # exists sk, check the constraint and referential_integrity
        if len(self.__get_list_sk_cols(df_expected)) > 0:
            # fk_constraints
            time.sleep(0.5) # sync log
            for c in self.__get_list_sk_cols(df_expected):
                self.logger.info(f"Executing: expect_col_fk: {c}")
                tuple_results = gold_test.check_if_column_have_constraint(
                    list_expected_constraints=self.__get_sqlserver_constraints('fk'), 
                    list_observed_constraints=self.__get_uc_constraints('fk', self.path_gold_table), 
                    path_table_expected=self.path_azure_table,
                    path_table_observed=self.path_gold_table,
                    col_name=c,
                    constr_type='fk',
                )
                exp = ExpectationValidationResult(
                    success=(tuple_results[0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_have_fk_constraint",
                        kwargs={
                            'column': c,
                            "result": tuple_results[0],
                            "msg": tuple_results[1],
                        },
                    )
                )
                add_custom_result_to_validation(exp, validation_results)

            # referential_integrity
            for c in self.__get_list_sk_cols(df_expected):
                tuple_results = gold_test.check_if_column_have_referential_integrity(
                    spark=self.spark,
                    df_observed_constraints=self.__get_uc_constraints('fk', self.path_gold_table, get_df=True),
                    path_gold_table=self.path_gold_table,
                    col_name=c,
                )
                self.logger.info(f"Executing: expect_col_ref_integrity: {c}")
                exp = ExpectationValidationResult(
                    success=(tuple_results[0] == True),
                    expectation_config=ExpectationConfiguration(
                        expectation_type="expect_column_have_referential_integrity",
                        kwargs={
                            'column': c,
                            "result": tuple_results[0],
                            "msg": tuple_results[1],
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
        """
        Executes data tests between expected and observed DataFrames.

        Args:
            df_expected (DataFrame): The DataFrame representing expected data.
            df_expected_name (str): Name of the DataFrame containing expected data.
            df_observed (DataFrame): The DataFrame representing observed data.
            df_observed_name (str): Name of the DataFrame containing observed data.
            kwargs (Dict, optional): Additional keyword arguments.

        Returns:
            Dict: Results of the data tests.
        """
        self.logger.info("========== prepare data test session ==========")
        df_expected.cache()
        df_observed.cache()

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
        self.logger.info(f'List PKs: {self.__get_list_pk_cols()}')
        self.logger.info(f'List SKs: {self.__get_list_sk_cols(df_expected)}')
        self.logger.info(f'List gold cols: {self.__get_list_gold_cols()}')
        self.logger.info(f'List business cols: {self.__get_list_business_cols(df_expected)}\n')
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
