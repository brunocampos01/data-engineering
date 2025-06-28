"""
### Module - LiftShiftTestData for QA Library

#### Class:
    * LiftshiftTestData: Tests the data between azure mallegan and on-premises environment
"""
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
    regexp_replace, 
    collect_list,
)

from library.database.azure_sql import AzureSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.template_data_tests import TemplateDataTest
from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.qa.utils import LogTag


class LiftshiftTestData(TemplateDataTest):
    """
    ### Class for testing the data between azure mallegan and on-premises environment. 
    This class following the skeleton define at library.qa.template_data_tests
    
    #### Functions:
        * log_execution_parameters: Execute LOG parameters
        * validate_parameters: Validate Class parameters
        * get_and_prepare_data: Get the data and metadata
        * execute_custom_data_tests: Compare the data and properties between two df and perform various data tests.
    """
    def __init__(
        self,
        spark: SparkSession,
        table_type: str,
        schema_azure_name: str,
        table_azure_name: str,
        catalog_azure_name: str,
        query: str,
        list_deny_cols: List[str] = [],
        source_connector=SQLServerDWSql,
        target_connector=AzureSQL,
        validate_table_name_in_source_query: bool = True,
    ):
        """
        ### Initialize Class

        #### Args:
            * spark (SparkSession): SparkSession
            * table_type (str): if the table is dim or fact
            * schema_azure_name (str): AzureSQL table schema name
            * table_azure_name (str): AzureSQL table table name
            * catalog_azure_name (str): AzureSQL table catalog name
            * source_connector (SQLServerDWSql): 
            * target_connector (AzureSQL): 
            * validate_table_name_in_source_query (bool): Default: `True`
        """
        super().__init__()
        self.spark = spark
        self.step_layer = 'liftshift'
        self.catalog_azure_name = catalog_azure_name
        self.table_azure_name = table_azure_name
        self.schema_azure_name = schema_azure_name
        self.table_type = table_type
        self.query = query
        self.list_deny_cols
        self._source_connector = source_connector
        self._target_connector = target_connector
        self._onpremises_loader = source_connector()
        self._azure_loader = target_connector()
        self._validate_table_name_in_source_query = validate_table_name_in_source_query
        self.path_azure_table = f'{self.catalog_azure_name}.{self.schema_azure_name}.{self.table_azure_name}'.strip().lower()

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
        self.logger.info(f"catalog_azure_name: {self.catalog_azure_name}")
        self.logger.info(f"schema_azure_name:  {self.schema_azure_name}")
        self.logger.info(f"table_azure_name:   {self.table_azure_name}")
        self.logger.info(f"path_azure_table:   {self.path_azure_table}")
        self.logger.info(f"query az:         \n{self.query}")

    def validate_parameters(self) -> None:
        """
        ### Validate Class parameters

        #### Args:
            None
        """
        table = self.table_azure_name.upper()
        # Check if the query is not empty or if it is a valid string
        try:
            if self.query is None or not self.query.strip():
                raise ValueError(f'The query file is empty.\nCheck the query file located at query_path.')
        except AttributeError:
            # If self.query does not have a 'strip' method, it is not a valid string
            self.TypeError(f'The query is not a valid string, received: {type(self.query)}!')

        # Check if 'table_azure_name' are contained within the query
        if self._validate_table_name_in_source_query:
            if table in self.query:
                self.logger.info(f"Founded '{table}' inside the query. Validation successful!")
            else:
                raise ValueError(f"Parameters mismatch!\n Expected the query from query_path to contain: '{table}' table name inside. Check the query and table_azure_name parameter.")

    def generate_information_schema_query(self) -> str:
        """
        ### Generate a query for fetching properties from the information schema based on the table_type.
        #### Returns:
            str: The query for fetching properties from the information schema.
        """
        return f"""
            SELECT
                COLUMN_NAME,
                IS_NULLABLE,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                COLLATION_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_CATALOG = '{self.catalog_azure_name}' AND
                TABLE_SCHEMA = '{self.schema_azure_name}' AND
                TABLE_NAME = '{self.table_azure_name}'
            """

    def get_business_cols(df: DataFrame) -> DataFrame:
        """
        ### Removes Surrogate keys and ETL columns for specific test: `expect_same_content_rows`
        
        #### Args:
            * df (DataFrame): Input DataFrame
        
        #### Returns:
            * DataFrame: DataFrame after removing SK keys
        """
        filtered_cols = [c for c in df.columns 
                        if not c.startswith('ETL_') 
                        and not c.endswith('_SK') 
                        and c not in self.list_deny_cols]
        return df.select(*filtered_cols)

    def get_sk_fact_names_and_pk_dim_names(self, list_dim_cols_name: List) -> DataFrame:
        """
        ### Prepares DataFrame with constraints `DimensionPK` & `FactSK`

        #### Args:
            * list_dim_cols_name (list): list of Dimension column names
            
        #### Returns:
            * DataFrame: DataFrame with constraints
        """
        query = f'''
        SELECT
            dim_columns.name AS dim_pk,
            fact_columns.name AS fact_sk
        FROM
            sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables fact_table ON fk.parent_object_id = fact_table.object_id
            INNER JOIN sys.columns fact_columns ON fkc.parent_object_id = fact_columns.object_id AND fkc.parent_column_id = fact_columns.column_id
            INNER JOIN sys.tables dim_table ON fk.referenced_object_id = dim_table.object_id
            INNER JOIN sys.columns dim_columns ON fkc.referenced_object_id = dim_columns.object_id AND fkc.referenced_column_id = dim_columns.column_id
        WHERE
            fact_table.name = '{self.table_azure_name}'
            AND fact_table.is_ms_shipped = 0
            AND dim_table.is_ms_shipped = 0
            AND dim_columns.name IN ({list_dim_cols_name})
        '''
        azure_loader = AzureSQL()

        logger.info(f'Executing the query on Azure: {query}')
        return self.spark.read.format('jdbc') \
            .options(**azure_loader.options(self.catalog_azure_name)) \
            .option("query", query) \
            .load()

    def get_and_prepare_data_for_referential_integraty_test(self, dict_cols_names: Dict, df_constraints: DataFrame) -> DataFrame:
        """
        ### Prepares DataFrame with `DimensionPK` & `FactSK` and distinct values

        #### Args:
            * spark (SparkSession): The Spark session.
            * dict_cols_names (dict): dictionary with column names
            * df_constraints (DataFrame): DataFrame with constraints
            
        #### Returns:
            * DataFrame:
            +---------------+--------------------+--------------------+
            |         dim_pk|             fact_sk|          dim_values|
            +---------------+--------------------+--------------------+
            |ACCT_PROJECT_SK|FACCT_ACCT_PROJEC...|[31, 85, 65, 53, ...|
            |    DIVISION_SK|FACCT_COMPANY_REG...|[-1, 1, 6, 3, 5, ...|
        """
        # dim values
        schema_df = ["dim_pk", "dim_values"]
        df_values = self.spark.createDataFrame([
            (k, v)
            for k, values in dict_cols_names.items()
            for v in values
        ], schema_df)
        df = df_constraints.join(df_values, on='dim_pk', how='inner')
        return df.groupBy("fact_sk", "dim_pk").agg(collect_list('dim_values').alias('dim_values'))

    def get_and_prepare_data_dim(self, dict_cols: Dict[str, str], catalog_name: str, schema_name: str) -> Dict[str, list]:
        """
        ### Gets PK from dimension tables and prepare the data for further processing to check referential integrity. This function retrieves distinct data from dimension tables based on the specified columns received in input.

        #### Args:
            * dict_cols (dict): A dictionary with table names as keys and column names as values. The 'FACT' table should be excluded as it is not a dimension table. e.g.: `dict_surrogate_key = {'DIM_ACCOUNT': 'ACCOUNT_CODE','DIM_COST_CENTER': 'COST_CENTER_CODE', ...}`
            * catalog_name (str): The name of the Azure SQL catalog/database. e.g.: `csldw`
            * schema_name (str): The name of the schema containing the dimension tables. e.g.: `dw`

        #### Returns:
            * dict: dictionary with columns names
        """
        azure_loader = AzureSQL()
        dict_cols_names = {}
        for table, column in dict_cols.items():
            if '.' in table:
                schema_name, table_name = table.split('.', 1)
            else:
                schema_name = schema_name
                table_name = table
            query = f'SELECT DISTINCT {column} FROM {schema_name}.{table_name}'
            logger.info(f'Executing the query on Azure: {query}')
            df_values_raw = (
                self.spark.read.format('jdbc')
                .options(**azure_loader.options(catalog_name))
                .option("query", query)
                .load()
            )

            # get distinct values from col and transform in a list
            list_pk = [row[column] for row in df_values_raw.collect()]
            dict_cols_names[column] = list_pk
            logger.info(f'In {column} col founded {len(list_pk)} at {schema_name}.{table}')

        return dict_cols_names

    def get_and_prepare_data(self, kwargs: dict = None) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
        """
        ### Get the data and metadata

        #### Args:
            * kwargs (dict): dictionary of keyword arguments
        
        #### Returns:
            * A tuple containing DataFrames for:
                1. On-premises data.
                2. On-premises data containing only properties.
                3. Azure data.
                4. Azure data containing only properties
        """
        logger.info(f'Cols that was truncated the milisec and sync the datetime: {list_azure_cols_timestamp}')

        # source
        df_onpremises = self.spark.read.format("jdbc") \
            .options(**self._onpremises_loader.options(self.catalog_azure_name)) \
            .option("query", self.query) \
            .load()

        varchar_fields = [
            field for field in df_onpremises.schema.fields
            if field.dataType.typeName() == "string"
        ]
        for field in varchar_fields:
            df_onpremises = df_onpremises.withColumn(field.name, regexp_replace(col(field.name), '�', ''))
        df_onpremises.limit(1).display()

        df_onpremises_only_properties = self.spark.read.format("jdbc") \
            .options(**self._onpremises_loader.options(self.catalog_azure_name)) \
            .option("query", self.generate_information_schema_query()) \
            .load() \
            .filter(col("COLUMN_NAME").isin(df_onpremises.columns))

        # target
        df_azure = self.spark.read.format(self._target_connector.format) \
            .options(**self._azure_loader.options(self.catalog_azure_name)) \
            .option("query", self.query) \
            .load()
        df_azure.limit(1).display()

        df_azure_only_properties = self.spark.read.format(self._target_connector.format) \
            .options(**self._azure_loader.options(self.catalog_azure_name)) \
            .option("query", self.generate_information_schema_query()) \
            .load() \
            .filter(col("COLUMN_NAME").isin(df_azure.columns))

        return df_onpremises, \
            df_onpremises_only_properties, \
            df_azure, \
            df_azure_only_properties

    def execute_custom_data_tests(
        self,
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        kwargs: Dict = None,
    ) -> Dict:
        """
        ### Compare the data and properties between two df and perform various data tests.

        #### Args:
            * df_expected (DataFrame): The expected df.
            * df_expected_name (str): Name of the expected df for reporting.
            * df_observed (DataFrame): The observed df.
            * df_observed_name (str): Name of the observed df for reporting.
            * kwargs: Keyword arguments containing additional DataFrames.
                Required DataFrame keys:
                    - 'df_onpremises_only_properties': DataFrame with on-premises-only properties.
                    - 'df_azure_only_properties': DataFrame with Azure-only properties.
                    - 'df_onpremises_business_cols': DataFrame with on-premises business columns.
                    - 'df_azure_business_cols': DataFrame with Azure business columns.

        #### Returns:
            * dict: A dictionary with test results. 
                e.g.:
                    {'have_dims_associate_measure': (True, ''),
                    'are_same_rows': (True, ''),
                    'datetime_precision': (True, ''),
                    'scale': (True, ''),
                    'precision': (True, ''),
                    'character_maximum_length': (True, ''),
                    'nullable': (True, ''),
                    'sqlserver_datatype': (True, ''),
                    'encoding': (True, ''),
        """
        df_onpremises_only_properties = kwargs["df_onpremises_only_properties"]
        df_azure_only_properties = kwargs["df_azure_only_properties"]
        df_onpremises_business_cols = kwargs["df_onpremises_business_cols"]
        df_azure_business_cols = kwargs["df_azure_business_cols"]
        table_type = kwargs["table_type"]  
        dict_dims_relationalships = kwargs["dict_dims_relationalships"]      

        # if you don't pass the dict_dims_relationalships in parameters orchestrator
        if table_type == 'fact' and dict_dims_relationalships is not None:
            # dims_associate_measure
            list_dim_tables_name = [t for t in list(dict_dims_relationalships.keys()) if not t.startswith('stg')]
            self.logger.info(f"Executing the check_if_checksum_are_equal using these dimensions: {list_dim_tables_name}")
            have_corresponding_dims = check_if_checksum_are_equal(
                spark=self.spark,
                table_name_dim=self.table_azure_name,
                schema_azure_name=self.schema_azure_name,
                catalog_azure_name=self.catalog_azure_name,
                list_dim_tables_name=list(dict_dims_relationalships.keys()),
            )

        # rows content
        are_same_rows = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_business_cols,
            df_expected_name=df_expected_name,
            df_observed=df_azure_business_cols,
            df_observed_name=df_observed_name,
            tag=LogTag.ROW_CONTENT,
        )

        # data_type
        are_same_sqlserver_datatype = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "DATA_TYPE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "DATA_TYPE"),
            df_observed_name=df_observed_name,
            tag=LogTag.DATA_TYPE,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # is_nullable
        have_same_property_nullable = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "IS_NULLABLE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "IS_NULLABLE"),
            df_observed_name=df_observed_name,
            tag=LogTag.IS_NULLABLE,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # character_maximum_length
        have_same_property_character_maximum_length = custom_qa.check_if_two_df_contain_the_same_rows(
                df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "CHARACTER_MAXIMUM_LENGTH"),
                df_expected_name=df_expected_name,
                df_observed=df_azure_only_properties.select("COLUMN_NAME", "CHARACTER_MAXIMUM_LENGTH"),
                df_observed_name=df_observed_name,
                tag=LogTag.CHARACTER_MAXIMUM_LENGTH,
                df_onpremises_only_properties=df_onpremises_only_properties,
            )

        # numeric_precision
        have_same_property_precision = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "NUMERIC_PRECISION"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "NUMERIC_PRECISION"),
            df_observed_name=df_observed_name,
            tag=LogTag.NUMERIC_PRECISION,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # numeric_scale
        have_same_property_scale = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "NUMERIC_SCALE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "NUMERIC_SCALE"),
            df_observed_name=df_observed_name,
            tag=LogTag.NUMERIC_SCALE,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # datetime_precision
        have_same_property_datetime_precision = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "DATETIME_PRECISION"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "DATETIME_PRECISION"),
            df_observed_name=df_observed_name,
            tag=LogTag.DATETIME_PRECISION,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # encoding
        have_same_property_encoding = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "COLLATION_NAME"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "COLLATION_NAME"),
            df_observed_name=df_observed_name,
            tag=LogTag.COLLATION_NAME,
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # if you don't pass the dict_dims_relationalships in parameters orchestrator
        if table_type == 'fact' and dict_dims_relationalships is not None:
            return {
                "have_corresponding_dims": have_corresponding_dims,
                "are_same_rows": are_same_rows,
                "datetime_precision": have_same_property_datetime_precision,
                "scale": have_same_property_scale,
                "precision": have_same_property_precision,
                "character_maximum_length": have_same_property_character_maximum_length,
                "nullable": are_same_sqlserver_datatype,
                "sqlserver_datatype": are_same_sqlserver_datatype,
                "encoding": have_same_property_encoding,
        }
        else:
            # dimesion
            return {
                "are_same_rows": are_same_rows,
                "datetime_precision": have_same_property_datetime_precision,
                "scale": have_same_property_scale,
                "precision": have_same_property_precision,
                "character_maximum_length": have_same_property_character_maximum_length,
                "nullable": are_same_sqlserver_datatype,
                "sqlserver_datatype": are_same_sqlserver_datatype,
                "encoding": have_same_property_encoding,
            }
