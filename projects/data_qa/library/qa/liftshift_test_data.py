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
from pyspark.sql.functions import col, regexp_replace

from library.database.azure_sql import AzureSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.template_data_tests import TemplateDataTest
from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.qa.utils import (
    check_if_dim_is_not_empty,
    check_if_checksum_are_equal,
)

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
        schema_name: str,
        table_name: str,
        catalog_name: str,
        query: str,
        query_properties: str,
        container_adsl_name: str = "quality-assurance",
        source_connector=SQLServerDWSql,
        target_connector=AzureSQL,
        validate_table_name_in_source_query: bool = True,
    ):
        """
        ### Initialize Class

        #### Args:
            * spark (SparkSession): SparkSession
            * table_type (str): if the table is dim or fact
            * schema_name (str): AzureSQL table schema name
            * table_name (str): AzureSQL table table name
            * catalog_name (str): AzureSQL table catalog name
            * query (str): AzureSQL query
            * query_properties (str): 
            * container_adsl_name (str): ADLS container name. Default: `quality-assurance`
            * source_connector (SQLServerDWSql): 
            * target_connector (AzureSQL): 
            * validate_table_name_in_source_query (bool): Default: `True`
        """
        super().__init__()
        self.spark = spark
        self.container_adsl_name = container_adsl_name
        self.step_layer = 'liftshift'
        self.table_type = table_type
        self.table_name = table_name
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.path_table = f'{self.catalog_name}.{self.schema_name}.{self.table_name}'.strip().lower()
        self.query = query
        self.query_properties = query_properties
        self._source_connector = source_connector
        self._target_connector = target_connector
        self._onpremises_loader = source_connector()
        self._azure_loader = target_connector()
        self._validate_table_name_in_source_query = validate_table_name_in_source_query

    def log_execution_parameters(self) -> None:
        """
        ### Execute LOG parameters

        #### Args:
            None
        """
        self.logger.info("************ Execution Parameters ************")
        self.logger.info(f"environment:     {self.env}")
        self.logger.info(f"step_layer:      {self.step_layer}")
        self.logger.info(f"table_type:      {self.table_type}")
        self.logger.info(f"catalog_name:    {self.catalog_name}")
        self.logger.info(f"schema_name:     {self.schema_name}")
        self.logger.info(f"table_name:      {self.table_name}")
        self.logger.info(f"path_table:      {self.path_table}")
        self.logger.info(f"Query that will execute in azure and on-premises:\n {self.query}")

    def validate_parameters(self) -> None:
        """
        ### Validate Class parameters

        #### Args:
            None
        """
        table = self.table_name.upper()
        # Check if the query is not empty or if it is a valid string
        try:
            if self.query is None or not self.query.strip():
                raise ValueError(f'The query file is empty.\nCheck the query file located at query_path.')
        except AttributeError:
            # If self.query does not have a 'strip' method, it is not a valid string
            self.TypeError(f'The query is not a valid string, received: {type(self.query)}!')

        # Check if 'table_name' are contained within the query
        if self._validate_table_name_in_source_query:
            if table in self.query:
                self.logger.info(f"Founded '{table}' inside the query. Validation successful!")
            else:
                raise ValueError(f"Parameters mismatch!\n Expected the query from query_path to contain: '{table}' table name inside. Check the query and table_name parameter.")

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
        # source
        df_onpremises = self.spark.read.format("jdbc") \
            .options(**self._onpremises_loader.options(self.catalog_name)) \
            .option("query", self.query) \
            .load()

        varchar_fields = [
            field 
            for field in df_onpremises.schema.fields
            if field.dataType.typeName() == "string"
        ]
        for field in varchar_fields:
            df_onpremises = df_onpremises.withColumn(field.name, regexp_replace(col(field.name), '�', ''))

        df_onpremises.limit(1).display()

        df_onpremises_only_properties = self.spark.read.format("jdbc") \
            .options(**self._onpremises_loader.options(self.catalog_name)) \
            .option("query", self.query_properties) \
            .load() \
            .filter(col("COLUMN_NAME").isin(df_onpremises.columns))

        # target
        df_azure = self.spark.read.format(self._target_connector.format) \
            .options(**self._azure_loader.options(self.catalog_name)) \
            .option("query", self.query) \
            .load()
        df_azure.limit(1).display()

        df_azure_only_properties = self.spark.read.format(self._target_connector.format) \
            .options(**self._azure_loader.options(self.catalog_name)) \
            .option("query", self.query_properties) \
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
                table_name_dim=self.table_name,
                schema_name=self.schema_name,
                catalog_name=self.catalog_name,
                list_dim_tables_name=list(dict_dims_relationalships.keys()),
            )

        # rows content
        are_same_rows = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_business_cols,
            df_expected_name=df_expected_name,
            df_observed=df_azure_business_cols,
            df_observed_name=df_observed_name,
            tag='ROW_CONTENT',
        )

        # data_type
        are_same_sqlserver_datatype = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "DATA_TYPE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "DATA_TYPE"),
            df_observed_name=df_observed_name,
            tag='DATA_TYPE',
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # is_nullable
        have_same_property_nullable = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "IS_NULLABLE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "IS_NULLABLE"),
            df_observed_name=df_observed_name,
            tag='IS_NULLABLE',
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # character_maximum_length
        have_same_property_character_maximum_length = custom_qa.check_if_two_df_contain_the_same_rows(
                df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "CHARACTER_MAXIMUM_LENGTH"),
                df_expected_name=df_expected_name,
                df_observed=df_azure_only_properties.select("COLUMN_NAME", "CHARACTER_MAXIMUM_LENGTH"),
                df_observed_name=df_observed_name,
                tag='CHARACTER_MAXIMUM_LENGTH',
                df_onpremises_only_properties=df_onpremises_only_properties,
            )

        # numeric_precision
        have_same_property_precision = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "NUMERIC_PRECISION"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "NUMERIC_PRECISION"),
            df_observed_name=df_observed_name,
            tag='NUMERIC_PRECISION',
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # numeric_scale
        have_same_property_scale = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "NUMERIC_SCALE"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "NUMERIC_SCALE"),
            df_observed_name=df_observed_name,
            tag='NUMERIC_SCALE',
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # datetime_precision
        have_same_property_datetime_precision = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "DATETIME_PRECISION"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "DATETIME_PRECISION"),
            df_observed_name=df_observed_name,
            tag='DATETIME_PRECISION',
            table_type=table_type,
            df_onpremises_only_properties=df_onpremises_only_properties,
        )

        # encoding
        have_same_property_encoding = custom_qa.check_if_two_df_contain_the_same_rows(
            df_expected=df_onpremises_only_properties.select("COLUMN_NAME", "COLLATION_NAME"),
            df_expected_name=df_expected_name,
            df_observed=df_azure_only_properties.select("COLUMN_NAME", "COLLATION_NAME"),
            df_observed_name=df_observed_name,
            tag='COLLATION_NAME',
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
