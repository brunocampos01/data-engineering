import os
import time
import json
import warnings
from pprint import pprint
from typing import (
    List, 
    Dict, 
    Tuple,
    Any,
)

import pandas as pd

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult

from pyspark.errors import PySparkTypeError
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    trim,
    concat_ws,
    expr,
    lower,
    coalesce,
    lit,
    regexp_replace,
    initcap,
    explode,
)
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    BooleanType, 
    ArrayType,
    DecimalType,
    DoubleType,
)

from library.dataloader.hash_key_generator import HashKeyGenerator
from library.database.azure_adls import AzureADSL
from library.database.sqlserver_dw_sql import SQLServerDWSql
from library.datalake_storage import DataLakeStorage
from library.qa.great_expectations_helper import GreatExpectationsHelper
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.custom_data_tests.interfaces_qa import InterfacesQA as interfaces_test
from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.datacleaner.defaults import Defaults as col_silver
from library.qa.template_data_tests import TemplateDataTest
from library.qa.utils import LogTag


class InterfacesTestData(TemplateDataTest):
    def __init__(
        self,
        spark: SparkSession,
        list_cols_id: List[str],
        type_validation: str,
        interface_name: str,
        catalog_observed: str = None,
        catalog_expected: str = None,
        schema_observed: str = None,
        schema_expected: str = None,
        table_observed_name: str = None,
        table_expected_name: str = None,
        container_name: str = None,
        landing_container_name: str = None,
        landing_path_data: str = None,
        outbound_container_name: str = None,
        outbound_path_data: str = None,
    ):
        super().__init__()
        self.spark = spark
        self.step_layer = 'interfaces'
        self.list_cols_id = list_cols_id
        self.type_validation = type_validation
        self.interface_name = interface_name
        if self.type_validation in self.__get_list_type_validation():
            if self.type_validation == 'sqlserver-sqlserver':
                self.catalog_observed = catalog_observed
                self.catalog_expected = catalog_expected
            else:
                self.catalog_observed = f'{self.env}_{catalog_observed}'
                self.catalog_expected = f'{self.env}_{catalog_expected}'
            self.schema_observed = schema_observed
            self.schema_expected = schema_expected
            self.table_observed_name = table_observed_name
            self.table_expected_name = table_expected_name
            self.path_observed_table = f'{self.catalog_observed}.{self.schema_observed}.{self.table_observed_name}'.strip().lower()
            self.path_expected_table = f'{self.catalog_expected}.{self.schema_expected}.{self.table_expected_name}'.strip().lower()
            self.container_name = container_name
        else:
            self.landing_container_name = landing_container_name
            self.landing_path_data = landing_path_data
            self.outbound_container_name = outbound_container_name
            self.outbound_path_data = outbound_path_data
            if self.landing_container_name:
                self.path_azure_landing = DataLakeStorage.get_storage_url(self.landing_container_name, self.landing_path_data)
            if self.outbound_container_name:
                self.path_azure_outbound = DataLakeStorage.get_storage_url(self.outbound_container_name, self.outbound_path_data)

    @staticmethod
    def __get_list_type_validation() -> List[str]:
        return ['uc-uc', 'local-uc', 'uc-sqlserver', 'sqlserver-sqlserver']

    @staticmethod
    def __get_list_technical_cols() -> List[str]:
        """
        Get techical cols + tracking cols that must exists in silver

        #### Returns:
            List[str]: A list of silver columns.
        """
        tracking_columns_expr: dict = {column.column_name: column.column_expression
                                       for column in col_silver.tracking_columns()}
        list_tracking_column_names = list(tracking_columns_expr.keys())
        list_technical_column_names = col_silver.technical_column_names()
        list_etl_cols = ['etl_created_job_id', 'etl_updated_datetime', 
                         'etl_updated_job_run_id', 'etl_created_job_run_id', 
                         'etl_updated_job_id', 'etl_load', 'etl_import_result', 
                         'etl_import_result_date', 'ETL_Update_Date']
        return list(set(list_tracking_column_names + list_technical_column_names + list_etl_cols))        

    def __get_list_business_cols(self, df_expected: DataFrame) -> List[str]:
        """Get a list of business-related columns from the expected DataFrame.

        Args:
            df_expected (DataFrame): The DataFrame containing the expected columns.

        Returns:
            List[str]: A list of column names that are considered business-related.
        """
        list_cols_expected = df_expected.columns
        list_cols_to_drop = ['id', 'file_name']
        set_cols = set(list_cols_expected) - set(self.__get_list_technical_cols()) - set(list_cols_to_drop)
        return list(set_cols)

    def __load_officevibe(self, path_observed: str, list_id_filtered: List[str], df_expected: DataFrame) -> DataFrame:
        data = json.load(open(path_observed))
        map_cols = {
            "Date of Birth": "DateofBirth",
            "Hiring Date": "HiringDate",
            "Is Manager": "IsManager",
            "Career Level": "CareerLevel",
            "Business Unit": "BusinessUnit",
            "Employee Category": "EmployeeCategory",
            "Work Hours": "WorkHours",
        }
        for user in data['users']:
            for old_key, new_key in map_cols.items():
                if old_key in user:
                    user[new_key] = user.pop(old_key)
        try:
            schema = StructType([
                StructField('settings', StructType([
                    StructField('syncManagerEmail', StringType(), True),
                    StructField('inviteNewUsers', BooleanType(), True)
                ]), True),
                StructField('users', ArrayType(StructType([
                    StructField('id', StringType(), True),
                    StructField('email', StringType(), True),
                    StructField('firstName', StringType(), True),
                    StructField('lastName', StringType(), True),
                    StructField('jobTitle', StringType(), True),
                    StructField('imageUrl', StringType(), True),
                    StructField('language', StringType(), True),
                    StructField('Gender', StringType(), True),
                    StructField('DateofBirth', StringType(), True),
                    StructField('HiringDate', StringType(), True),
                    StructField('IsManager', StringType(), True),
                    StructField('CareerLevel', StringType(), True),
                    StructField('BusinessUnit', StringType(), True),
                    StructField('Country', StringType(), True),
                    StructField('Location', StringType(), True),
                    StructField('EmployeeCategory', StringType(), True),
                    StructField('WorkHours', StringType(), True),
                    StructField('Function', StringType(), True),
                    StructField('Department', StringType(), True),
                    StructField('Pillar', StringType(), True)
                ]), True), True),
                StructField('groups', ArrayType(StructType([
                    StructField('id', StringType(), True),
                    StructField('name', StringType(), True)
                ]), True), True),
                StructField('mappings', ArrayType(StructType([
                    StructField('GroupId', StringType(), True),
                    StructField('UserId', StringType(), True),
                    StructField('SubGroupId', StringType(), True),
                    StructField('IsMember', BooleanType(), True),
                    StructField('IsManager', BooleanType(), True)
                ]), True), True)
            ])
            df_observed = self.spark.createDataFrame([data], schema)
        except PySparkTypeError:
            raise ValueError(f"Schema mismatched.")
        else:
            df_observed = df_observed \
                .withColumn("user_id", explode("users")) \
                .filter(col("user_id.id").isin(*list_id_filtered)) \
                .select("user_id.*")

            df_expected = df_expected \
                .withColumn("user_id", explode("users")) \
                .filter(col("user_id.id").isin(*list_id_filtered)) \
                .select("user_id.*")

            return df_observed, df_expected

    @staticmethod
    def get_files_from_company(path_complement: str, comp_name: str) -> List[str]:
        """Get a list of files from a company directory based on the provided path complement and company name.
        
        Args:
            path_complement (str): The complementary path to the directory.
            comp_name (str): The name of the company.
            
        Returns:
            List[str]: A list of file paths that match the company code in the specified directory.
        """
        map_companies = {
            "ampol": "801",
            "cana": "102",
            "eueur": "403",
            "eunor": "405",
            "auaus": "304",
        }
        comp_code = map_companies.get(comp_name)
        if 'expected' in path_complement:
            path = "".join(os.getcwd() + f"/{path_complement}")
            list_files = os.listdir(path)
            return [os.path.join(path, file) for file in list_files if comp_code in file]
        else:
            path = "".join(os.getcwd() + f"/{path_complement}/{comp_name}")
            list_files = os.listdir(path)
            return [os.path.join(path, file) for file in list_files]

    def __prepare_uc_uc_load(self, kwargs: Dict = {}) -> Tuple[DataFrame, DataFrame]:
        self.logger.info(f"Using expected and observed data from Unity Catalog")
        df_expected = self.spark.table(self.path_expected_table)
        df_observed = self.spark.table(self.path_observed_table)

        if self.interface_name == 'hsbc':
            df_observed, df_expected = self.__load_hsbc(kwargs)
        elif self.interface_name == 'oracle_employee':
            list_id_filtered = kwargs['list_id_filtered']
            df_observed = df_observed.filter(col("PERSON_ID").isin(*list_id_filtered))
            df_expected = df_expected.filter(col("PERSON_ID").isin(*list_id_filtered)).select(df_observed.columns)
            if self.table_observed_name == 'v_oracle_active_directory_employee':
                list_cols_supervisor = ['SUPERVISOR_LAST_NAME', 'SUPERVISOR_FIRST_NAME', 'SUPERVISOR_EMAIL']
                df_observed = df_observed.drop(*list_cols_supervisor)
        elif self.interface_name == 'imos_o2_voyagelist':
            list_cols_to_drop = kwargs['list_cols_to_drop']
            df_expected = df_expected.drop(*list_cols_to_drop)
            df_observed = df_observed.drop(*list_cols_to_drop)
        elif self.interface_name == 'imos_vendor':
            list_id_filtered = kwargs['list_id_filtered']
            df_observed = df_observed.filter(col("VENDOR_VENDOR_NUMBER").isin(*list_id_filtered)).filter(col('CountryCode') == 'ZZ')
            df_expected = df_expected.filter(col("VENDOR_VENDOR_NUMBER").isin(*list_id_filtered)).select(df_observed.columns)

        return df_observed, df_expected

    def __prepare_local_local_load(self, kwargs: Dict) -> Tuple[DataFrame, DataFrame]:
        if kwargs:
            if 'xml' in kwargs['landing_data']:
                pdf_expected = pd.DataFrame([kwargs['landing_data']])
                pdf_observed = pd.DataFrame([kwargs['outbound_data']])
                df_expected = self.spark.createDataFrame(pdf_expected)
                df_observed = self.spark.createDataFrame(pdf_observed)
        elif 'txt' in self.landing_path_data:
            pdf_expected = pd.read_csv(self.landing_path_data, sep=',', header=None)
            pdf_observed = pd.read_csv(self.outbound_path_data, sep=',', header=None)
        
        df_expected = self.spark.createDataFrame(pdf_expected)
        df_observed = self.spark.createDataFrame(pdf_observed)
        return df_observed, df_expected

    def __prepare_local_uc_load(self, kwargs: Dict) -> Tuple[DataFrame, DataFrame]:
        self.logger.info(f"Using expected data from Unity Catalog")
        df_expected = self.spark.table(self.path_expected_table)
        list_id_filtered = kwargs['list_id_filtered']
        if self.interface_name == 'oracle_employee':
            self.logger.info(f"Using observed data from file (json)")
            path_observed = os.path.join(os.getcwd(), 'oracle_employee/observed/data', kwargs['list_path_observed'][0])
            print(path_observed)
            df_observed, df_expected = self.__load_officevibe(path_observed, list_id_filtered, df_expected)

        # elif 'csv' in data_observed:
        #     self.logger.info(f"Using observed data from file (csv)")
        #     pdf = pd.read_csv(data_observed, sep='|', encoding='latin1')
        #     pdf = pdf.where(pd.notnull(pdf), None)
        #     df_observed = spark.createDataFrame(pdf)

        return df_observed, df_expected

    def __prepare_local_azure_load(self) -> Tuple[DataFrame, DataFrame]:
        if 'csv' in self.path_azure_outbound or 'txt' in self.path_azure_outbound:
            df_expected = self.spark.read.csv(self.path_azure_outbound)
            df_observed = self.spark.read.csv(self.path_azure_landing)
        return df_observed, df_expected
    
    def __prepare_azure_azure_load(self) -> Tuple[DataFrame, DataFrame]:
        if 'csv' in self.path_azure_outbound or 'txt' in self.path_azure_outbound:
            df_expected = self.spark.read.csv(self.path_azure_outbound)
            df_observed = self.spark.read.csv(self.path_azure_landing)

        return df_observed, df_expected

    def __prepare_kyriba_ar_forecast_load(self, kwargs: Dict) -> Tuple[DataFrame, DataFrame]:
        self.logger.info(f"Using expected data from local")
        def concatenate_files(list_files: List[str]):
            self.logger.info(f"Concatenating files: {list_files}")
            dfs = []
            for file in list_files:
                df = pd.read_csv(file, delimiter=",", header=0)
                dfs.append(df)
            return pd.concat(dfs, ignore_index=True)

        pdf_expected = concatenate_files(kwargs['list_expected_files_by_company'])
        df_expected = self.spark.createDataFrame(pdf_expected)
        self.logger.info(f"Dataframe df_expected created.")

        # download observed data
        self.logger.info(f"Downloading observed data from azure")
        comp_name = kwargs["comp_name"]
        file_name = kwargs['file_name']
        adls_conn = AzureADSL()._create_conn()
        blob_client = adls_conn.get_blob_client(self.outbound_container_name, self.outbound_path_data)
        file_path_observed = os.path.join(f'observed/ar_forecast/to_ftp/{comp_name}', file_name)
        with open(file_path_observed, "wb") as blob:
            data = blob_client.download_blob()
            data.readinto(blob)
        pdf_observed = pd.read_csv(file_path_observed, sep=',', header=0)
        df_observed = self.spark.createDataFrame(pdf_observed)
        self.logger.info(f"Saved observed data in {file_path_observed}")

        return df_observed, df_expected

    def __load_hsbc(self, kwargs: Dict) -> Tuple[DataFrame, DataFrame]:
        list_file_name = kwargs['list_file_name']
        self.logger.info("Filtering these files:")
        pprint(list_file_name)
        time.sleep(0.5)
        self.logger.info(f"Using expected data from Unity Catalog")
        df_expected = self.spark.table(self.path_expected_table) \
            .withColumn("file_name", expr("substring_index(xml_file, '/', -1)")) \
            .withColumn("file_name", lower(col('file_name'))) \
            .filter(col("file_name").isin(list_file_name)) \
            .drop('verified')
        self.logger.info(f"Using observed data from Unity Catalog")
        df_observed = self.spark.table(self.path_observed_table) \
            .withColumn("file_name", expr("substring_index(xml_file, '/', -1)")) \
            .withColumn("file_name", lower(col('file_name'))) \
            .filter(col("file_name").isin(list_file_name)) \
            .drop('verified')

        return df_observed, df_expected

    def __prepare_sqlserver_sqlserver_load(self, kwargs: Dict) -> Tuple[DataFrame, DataFrame]:       
        list_cols_to_drop = kwargs['list_cols_to_drop']
        if self.interface_name == 'customer_portal':
            from library.database.customer_portal import CustomerPortalSQL
            conn = CustomerPortalSQL
        else:
            from library.database.iss import IssSQL
            conn = IssSQL

        if self.table_expected_name.startswith('dim'):
            self.logger.info(f"Using expected data from {self.path_expected_table}")
            loader = SQLServerDWSql()
            df_expected = self.spark.read.format('jdbc')\
                .options(**loader.options('csldw'))\
                .option("query", f'select * from {self.schema_expected}.{self.table_expected_name}')\
                .load()
            for col_name in df_expected.columns:
                new_col_name = col_name.title().replace('_', '') # e.g.: CUSTOMER_CODE -> CustomerCode
                df_expected = df_expected.withColumnRenamed(col_name, new_col_name)
            
            df_expected = df_expected.drop(*list_cols_to_drop)

        else:
            self.logger.info(f"Using expected data from {self.path_expected_table}")
            df_expected = conn.get_df_from_sql(
                self.spark, self.catalog_expected, f'select * from {self.table_expected_name}'
            ).drop(*list_cols_to_drop)

        self.logger.info(f"Using observed data from {self.path_observed_table}")
        df_observed = conn.get_df_from_sql(
            self.spark, self.catalog_observed, f'select * from {self.table_observed_name}'
        ).drop(*list_cols_to_drop)

        if 'list_cols_to_trim' in kwargs:
            for c in kwargs['list_cols_to_trim']:
                df_expected = df_expected \
                    .withColumn(c, trim(col(c))) \
                    .withColumn(c, regexp_replace(c, r'\s+', ' ')) # e.g. PortBerthName col in ImosPortBerths(iss)
                df_observed = df_observed \
                    .withColumn(c, trim(col(c))) \
                    .withColumn(c, regexp_replace(c, r'\s+', ' '))
        
        df_expected = df_expected.select(df_observed.columns)

        if self.list_cols_id == []:
            self.logger.info(f'list_cols_id parameter is empty, generating hash kes')
            df_expected = df_expected.transform(HashKeyGenerator.generate_key, df_expected.columns)
            df_observed = df_observed.transform(HashKeyGenerator.generate_key, df_observed.columns)
            self.list_cols_id = ['etl_src_pkeys_hash_key']

        # cases that were updated in snowflake but not in sqlserver
        if 'dwdimport' in self.table_expected_name.lower():
            id_col = self.list_cols_id[0]
            df_expected = df_expected.join(df_observed, on="portno", how="inner").select(df_expected["*"])
            df_observed = df_observed.join(df_expected, on="portno", how="inner").select(df_observed["*"])
            df_expected = df_expected.filter(~col(id_col).isin([3079, 25808]))
            df_observed = df_observed.filter(~col(id_col).isin([3079, 25808]))
        elif 'dwbookings' in self.table_expected_name.lower():
            id_col = self.list_cols_id[0]
            set_values = (33079, 33229, 33261, 33274, 33313, 33322, 33390, 33410, 33436, 33522, 32720)
            df_expected = df_expected.filter(~col(id_col).isin(*set_values))
            df_observed = df_observed.filter(~col(id_col).isin(*set_values))
        elif 'dwcoalist' in self.table_expected_name.lower():
            set_values = (911,)
            df_expected = df_expected.filter(~col('coacontractid').isin(*set_values))
            df_observed = df_observed.filter(~col('coacontractid').isin(*set_values))
        elif 'dim_port' in self.table_expected_name.lower():
            set_values = ('28944',)
            df_expected = df_expected.filter(~col('portno').isin(*set_values))
            df_observed = df_observed.filter(~col('portno').isin(*set_values))
        elif 'imosportcosts' in self.table_expected_name.lower(): # handle precision
            df_expected = df_expected \
                .withColumn('Rate', col('Rate').cast(DecimalType(16, 5))) \
                .withColumn('Rate', col('Rate').cast(DoubleType()))
            df_observed = df_observed \
                .withColumn('Rate', col('Rate').cast(DecimalType(16, 5))) \
                .withColumn('Rate', col('Rate').cast(DoubleType()))
        elif 'imoscoas' in self.table_expected_name.lower(): # handle precision
            df_expected = df_expected \
                .withColumn('Rate', col('Rate').cast(DecimalType(16, 5))) \
                .withColumn('Rate', col('Rate').cast(DoubleType()))
            df_observed = df_observed \
                .withColumn('Rate', col('Rate').cast(DecimalType(16, 5))) \
                .withColumn('Rate', col('Rate').cast(DoubleType()))


        return df_observed, df_expected

    def hsbc_check_view(self, list_file_name: List[str]) -> None:
        query = f'''
        SELECT * FROM {self.catalog_observed}.{self.schema_observed}.v_{self.table_observed_name}
        WHERE lower(substring_index(xml_file, '/', -1)) in ({list_file_name})
        '''.replace('[', '').replace(']', '')
        self.logger.info(f'Executing query: {query}')
        return self.spark.sql(query).display()

    def log_execution_parameters(self) -> None:
        self.logger.info("************ Execution Parameters ************")
        self.logger.info(f"environment:     {self.env}")
        self.logger.info(f"step_layer:      {self.step_layer}")
        self.logger.info(f"interface_name:  {self.interface_name}")
        self.logger.info(f"type_validation: {self.type_validation}")
        self.logger.info(f"list_cols_id:    {self.list_cols_id}")

        if self.type_validation in self.__get_list_type_validation():
            self.logger.info(f"catalog_observed:    {self.catalog_observed}")
            self.logger.info(f"schema_observed:     {self.schema_observed}")
            self.logger.info(f"table_observed_name: {self.table_observed_name}")
            self.logger.info(f"catalog_expected:    {self.catalog_expected}")
            self.logger.info(f"schema_expected:     {self.schema_expected}")
            self.logger.info(f"table_expected_name: {self.table_expected_name}")
            self.logger.info(f"path_observed_table: {self.path_observed_table}")
            self.logger.info(f"path_expected_table: {self.path_expected_table}")
        elif self.type_validation == 'local':
            self.logger.info(f"landing_path_data:  {self.landing_path_data}")
            self.logger.info(f"outbound_path_data: {self.outbound_path_data}")
        elif self.type_validation in ['local-azure', 'azure-azure']:
            if self.landing_container_name:
                self.logger.info(f"path_azure_landing: {self.path_azure_landing}")
            if self.outbound_container_name:
                self.logger.info(f"path_azure_outbound: {self.path_azure_outbound}")

    def get_and_prepare_data(self, kwargs: Dict = None) -> Tuple[DataFrame, DataFrame]:
        if self.interface_name == 'kyriba_ar_forecast':
            df_observed, df_expected = self.__prepare_kyriba_ar_forecast_load(kwargs)
        elif self.type_validation == 'sqlserver-sqlserver':
            df_observed, df_expected = self.__prepare_sqlserver_sqlserver_load(kwargs)
        elif self.type_validation == 'local-local':
            df_observed, df_expected = self.__prepare_local_local_load(kwargs)
        elif self.type_validation == 'uc-uc':
            df_observed, df_expected = self.__prepare_uc_uc_load(kwargs)
        elif self.type_validation == 'local-uc':
            df_observed, df_expected = self.__prepare_local_uc_load(kwargs)
        elif self.type_validation == 'local-azure':
            df_observed, df_expected = self.__prepare_local_azure_load(kwargs)
        elif self.type_validation == 'azure-azure':
            df_observed, df_expected = self.__prepare_azure_azure_load(kwargs) # da_desk
        return df_observed, df_expected

    def __get_expectation_suite(self) -> ExpectationSuite:
        """Generate an ExpectationSuite based on the validation type.

        Returns:
            ExpectationSuite: The generated expectation suite.
        """
        if self.type_validation in ('uc-uc', 'uc-sqlserver', 'local-uc'):
            exp_suite_name = f"silver-{self.step_layer}-{self.schema_observed}.{self.table_observed_name}"
        elif self.type_validation == 'sqlserver-sqlserver':
            exp_suite_name = f"{self.step_layer}-{self.path_observed_table}" \
                .replace('_dbw_initial_load_01', '') \
                .replace('_dbw_initial_load_02', '') \
                .replace('_dbw_initial_load_03', '') \
                .replace('_dbw_initial_load_04', '')
        else:
            exp_suite_name = f"silver-{self.step_layer}-{self.landing_container_name}_to_{self.outbound_container_name}"
        return ExpectationSuite(exp_suite_name)

    def execute_great_expectations(
        self,
        df_expected: DataFrame,
        df_observed: DataFrame,
        kwargs: Dict = None,
    ) -> Dict:
        expectations = self.__get_expectation_suite()
        gdf_observed = SparkDFDataset(df_observed)

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
        # count by table
        self.logger.info("expect_table_count_distinct (using ID cols)")
        tuple_results = custom_qa.check_if_table_have_same_count_distinct(
            list_pk=self.list_cols_id,
            df_expected=df_expected,
            df_observed=df_observed,
        )
        exp = ExpectationValidationResult(
            success=tuple_results[0] is True,
            expectation_config=ExpectationConfiguration(
                expectation_type="expect_table_count_distinct",
                kwargs={
                    "result": tuple_results[0], 
                    "msg": tuple_results[1]
                },
            ),
        )
        add_custom_result_to_validation(exp, validation_results)

        # equal content -> col level
        for c in self.__get_list_business_cols(df_expected):
            self.logger.info(f"expect_same_content: {c}")
            tuple_results = custom_qa.check_if_two_df_contain_the_same_rows(
                df_expected=df_expected.select(*self.list_cols_id, c),
                df_expected_name='interfaces_testing',
                df_observed=df_observed.select(*self.list_cols_id, c),
                df_observed_name='interfaces',
                tag=LogTag.ROW_CONTENT,
            )
            exp = ExpectationValidationResult(
                success=tuple_results[0] is True,
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_same_content_rows",
                    kwargs={
                        "column": c,
                        "result": tuple_results[0], 
                        "msg": tuple_results[1]
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
        self.logger.info("========== prepare data test session ==========")
        df_expected.cache()
        df_observed.cache()

        self.logger.info(f'List IDs: {self.list_cols_id}')
        self.logger.info(f'List cols to validate the content: {self.__get_list_business_cols(df_expected)}')
        self.logger.info("========== data test session starts ==========")
        validation_results = self.execute_great_expectations(df_expected, df_observed)
        self.logger.info("========== results ==========")
        dict_filtered_results = GreatExpectationsHelper().get_dict_gx_result(validation_results)
        for key, value in dict_filtered_results.items():
            self.logger.info(f'{key}: {value}')
        self.logger.info("========== finished running the tests ==========")
        
        return validation_results
