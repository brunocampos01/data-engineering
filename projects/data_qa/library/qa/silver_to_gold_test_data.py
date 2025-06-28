"""
### Module - SilverToGoldTestData for QA Library

#### Class:
    * SilverToGoldTestData: Tests the data between azure magellan and Unity Catalog environment
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

from library.sensitive import (
    decrypt_column,
    _secret_value,
    _mode,
    _secret_key,
)
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

class SilverToGoldTestData(TemplateDataTest):
    """
    ### Class for testing the data between azure magellan and Unity Catalog environment. This class following the skeleton define at library.qa.template_data_tests
    """
    def __init__(
        self,
        spark: SparkSession,
        sql_database: str,
        catalog_db_name: str,
        schema_db_name: str,
        table_db_name: str,
        schema_gold_name: str,
        table_gold_name: str,
        table_type: str,
        list_col_prefix: List[str],
        list_col_keep_prefix: List[str],
        query: str,
        dict_schema_expected: str,
        list_skip_cols: List[str],
        list_skip_cols_check_content: List[str],
        list_skip_cols_not_null: List[str],
        list_skip_cols_fk_constraint: List[str],
        dict_rename_cols: dict = {},
        truncate_decimal_values: bool = False,
        floating_scale=5,
        gold_catalog_name: str = 'gold'
    ):
        super().__init__()
        self.spark = spark
        self.step_layer = 'gold'
        self.sql_database = sql_database
        self.catalog_db_name = catalog_db_name
        self.schema_db_name = schema_db_name
        self.table_db_name = table_db_name
        self.schema_gold_name = schema_gold_name
        self.table_gold_name = table_gold_name
        self.table_type = table_type
        self.query = query
        self.dict_schema_expected = dict_schema_expected
        self.list_skip_cols = list_skip_cols
        self.list_skip_cols_check_content = list_skip_cols_check_content
        self.list_skip_cols_not_null = list_skip_cols_not_null # dw_procurement.fact_ship_work_order has _sk with null values
        self.list_skip_cols_fk_constraint = list_skip_cols_fk_constraint
        self.list_col_prefix = list_col_prefix
        self.list_col_keep_prefix = list_col_keep_prefix
        self.rename_col_dict = dict_rename_cols # dw_procurement.fact_ship_work_order has typo in az cols' names
        self.catalog_gold_name = f'{self.env}_{gold_catalog_name}'
        self.path_azure_table = f'{self.catalog_db_name}.{self.schema_db_name}.{self.table_db_name}'.strip().lower()
        self.path_gold_table = f'{self.catalog_gold_name}.{self.schema_gold_name}.{self.table_gold_name}'.strip().lower()
        self.truncate_decimal_values = truncate_decimal_values
        self.floating_scale = floating_scale

    @staticmethod
    def __get_list_gold_cols() -> List[str]:
        """
        Get technical cols that must exists in gold

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
        list_pk_expected = list(set(self._get_sqlserver_constraints('pk')))
        if len(list_pk_expected) > 0:
            return list_pk_expected
        else:
            return self._get_uc_constraints('pk', self.path_gold_table)

    def __get_list_fk_cols(self) ->  List[str]:
        """
        Retrieves a list of foreign key columns for the specified table.

        Returns:
            List[str]: A list of foreign key column names. e.g.: ['xpto_sk']
        """
        list_fk_expected = list(set(self._get_sqlserver_constraints('fk')))
        if len(list_fk_expected) > 0:
            return list_fk_expected
        else:
            return self._get_uc_constraints('fk', self.path_gold_table)

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
        if self.catalog_gold_name == "test_silver_trsf":
            return []

        list_exceptions = ['date_sk']
        return [c for c in df_expected.columns
                if c.endswith('_sk') and c not in list_exceptions and c not in self.list_skip_cols]

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
                table_schema = '{self.schema_db_name}' 
                AND table_name = '{self.table_db_name}'
        """
        df = initialize_and_prepare_delta(spark=self.spark, query=query, db_catalog_name=self.catalog_db_name, db_name=self.sql_database)
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
        return list(set(self.__get_list_pk_cols()
        + self.__get_list_sk_cols(df_expected) + self.__get_list_gold_cols_must_not_be_null()))

    def _get_sqlserver_constraints(self, constr_type: str) ->  List[str]:
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
            c.table_schema AS table_schema,
            c.table_name AS table_name,
            c.column_name AS column_name,
            tc.constraint_name AS constraint_name,
            tc.constraint_type AS constraint_type
        FROM information_schema.table_constraints tc
        JOIN information_schema.constraint_column_usage c ON tc.constraint_name = c.constraint_name
        WHERE c.table_schema = '{self.schema_db_name}'
        '''
        df = initialize_and_prepare_delta(spark=self.spark, query=query, db_catalog_name=self.catalog_db_name, db_name=self.sql_database)
        df = df.filter(col('table_name').isin(self.table_db_name))

        if constr_type == 'fk':
            df = df.filter(col('constraint_type').isin('FOREIGN KEY'))
        elif constr_type == 'pk':
            df = df.filter(col('constraint_type').isin('PRIMARY KEY'))

        df = df.withColumn("column_name", lower(col('column_name')))
        df = self.__remove_prefix_values(df, 'column_name')
        df = self.__rename_col_values(df.select('column_name'))

        return [row.column_name for row in df.collect()]

    def _get_uc_constraints(self, constr_type: str, path_uc: str, get_df: bool = None) -> Union[List[str], DataFrame]:
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

            try:
                return df.collect()[0][0].split(", ")
            except IndexError:
                # if PK constraints was not defined
                if self.catalog_gold_name == 'test_silver_trsf':
                    logging.warning("Ignoring missing primary key in TRSF Schema")
                    return []

                raise Exception(f"No Primary Key constraint found in table {path_uc}.")

    def _get_uc_info_schema(self) -> DataFrame:
        '''
        Get Unity Catalog information schema for a given table.

        Returns:
            DataFrame: A DataFrame with Unity Catalog information schema for a given table.
            +----------------+------------------+----------------------------+---------+-----------+
            |ordinal_position|table_name        |column_name                 |data_type|is_nullable|
            +----------------+------------------+----------------------------+---------+-----------+
            |17              |dim_ship_po_header|expected_delivery_date_dt   |TIMESTAMP|YES        |
            |38              |dim_ship_po_header|buyer_name                  |STRING   |NO         |
            ...
        '''
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
        return self.spark.sql(query)

    def _get_sql_server_info_schema(self, table_db_name: str = None) -> DataFrame:
        '''
        Get Azure information schema for a given table.

        Returns:
            DataFrame: A DataFrame with Azure information schema for a given table.
            +----------------+--------------------+------------+---------+-----------+
            |ordinal_position|table_name          |column_name |data_type|is_nullable|
            +----------------+--------------------+------------+---------+-----------+
            |1               |dim_vessel_po_header|sk          |int      |NO         |
            |2               |dim_vessel_po_header|coy_code    |varchar  |NO         |
            ...
        '''
        if not table_db_name:
            table_db_name = self.table_db_name

        query = f'''
            SELECT 
                lower(ordinal_position) AS ordinal_position,
                LOWER(table_name) AS table_name,
                LOWER(column_name) AS column_name,
                data_type,
                is_nullable
            FROM information_schema.columns
            WHERE
                table_catalog = '{self.catalog_db_name}'
                AND table_schema = '{self.schema_db_name}'
                AND table_name = '{table_db_name}'
        '''
        df = initialize_and_prepare_delta(spark=self.spark, query=query, db_catalog_name=self.catalog_db_name, db_name=self.sql_database)
        df = self.__remove_prefix_values(df, "column_name")
        return df

    def __remove_prefix_values(self, df: DataFrame, column_name: str) -> DataFrame:
        '''
        This method removes a given prefix from values in a specific df's column.
        Analyze if prefix is not equal that column name. 
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
                if col_prefix:
                    df = df.withColumn("column_name", lower(col("column_name"))) \
                        .withColumn("column_name",
                            when((col("column_name") != col_prefix) & (~col("column_name").isin(self.list_col_keep_prefix)),
                                 regexp_replace(col("column_name"), f'^{col_prefix}s_', ''))
                            .otherwise(col("column_name"))
                        ) \
                        .withColumn("column_name",
                            when((col("column_name") != col_prefix) & (~col("column_name").isin(self.list_col_keep_prefix)),
                                 regexp_replace(col("column_name"), f'^{col_prefix}_', ''))
                            .otherwise(col("column_name"))
                        )

        return df

    def __rename_col_values(self, df: DataFrame) -> DataFrame:
        '''
        This method removes a given prefix from values in a specific df's column.

        Args:
            df (DataFrame): The input DataFrame. e.g.:
                +---------------------+---------+
                |         column_name |data_type|
                +---------------------+---------+
                |    etl_created_date | datetime| 
                | vessel_creation_date| datetime|
                +---------------------+---------+

        Returns:
            DataFrame: A DataFrame with prefix removed from specified column names. e.g.:
                +------------------+---------+
                |     column_name  |data_type|
                +------------------+---------+
                | etl_created_date | datetime|
                |ship_creation_date| datetime| changed
                +------------------+---------+
        '''
        if len(self.rename_col_dict) > 0:
            for old_name, new_name in self.rename_col_dict.items():
                df = df.withColumn('column_name', regexp_replace(col('column_name'), old_name, new_name))

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
                if col_prefix:
                    self.logger.info(f'Removing prefix: {col_prefix} in cols name from {self.path_azure_table}')

                    for col_name in df.columns:
                        if col_name.startswith(col_prefix) and col_prefix != col_name and col_name not in self.list_col_keep_prefix:
                            new_col_name = col_name[len(f'{col_prefix}_'):len(col_name)]
                            df = df.withColumnRenamed(col_name, new_col_name)

        return df

    def __rename_col(self, df: DataFrame) -> DataFrame:
        """
        Renames a dynamically generated set of columns.

        Args:
            df (DataFrame): The input DataFrame to process.
            e.g.: df.columns ['vessel_code', 'vessel_sk']

        Returns:
            DataFrame: The DataFrame with column renamed.
            e.g.: df.columns ['ship_code', 'ship_sk']
        """
        if len(self.rename_col_dict) > 0:
            df_renamed = df
            for old_name, new_name in self.rename_col_dict.items():
                # Filter columns starting with the dictionary key
                matched_columns = [col_name for col_name in df_renamed.columns if col_name.startswith(old_name)]
                # Rename matched columns
                for matched_column in matched_columns:
                    df_renamed = df_renamed.withColumnRenamed(matched_column, matched_column.replace(old_name, new_name))

            return df_renamed
        else:
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

        elif self.table_gold_name == 'dim_person_assignment_info':
            list_cols_to_lower = ['name', 'name_fr']
            for c in list_cols_to_lower:
                df = df.withColumn(c, trim(lower(col(c))))

        elif self.table_gold_name == 'fact_forecast':
            # the databricks round up some decimal values
            list_cols_to_round = ['amount_region', 'amount_consol']

            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 3)) \
                    .withColumn(c, col(c).cast(DecimalType(19,4)))

        elif self.table_gold_name == 'fact_budget':
            # the databricks round up some decimal values
            list_cols_to_round = ['amount_region', 'amount_consol']

            for c in list_cols_to_round:
                df = df.withColumn(c, ceil(col(c) * 100) / 100) \
                    .withColumn(c, col(c).cast(DecimalType(19,4)))

        elif self.table_gold_name == 'fact_voyage_tonnage_load':
            # the databricks round up some decimal values
            list_cols_to_round = ['port_cust_ratio', 'port_coa_ratio', 'cust_ratio', 'cust_coa_ratio']

            for c in list_cols_to_round:
                # df = df.withColumn(c, ceil(col(c) * 100000000) / 100000000) \
                df = df.withColumn(c, round(col(c), 0)) \
                    .withColumn(c, col(c).cast(DoubleType()))

        elif self.table_gold_name == 'fact_voyage_revenues':
            # the databricks round up some decimal values
            list_cols_to_round = ['revenue_amt']

            for c in list_cols_to_round:
                # df = df.withColumn(c, ceil(col(c) * 10000) / 10000) \
                df = df.withColumn(c, round(col(c), 0)) \
                    .withColumn(c, col(c).cast(DecimalType(16,4)))

        elif self.table_gold_name == 'fact_fuel_surcharge':
            # the databricks round up some decimal values
            list_cols_to_round = ['fuel_comp_amt']

            for c in list_cols_to_round:
                # df = df.withColumn(c, ceil(col(c) * 10000) / 10000) \
                df = df.withColumn(c, round(col(c), 0)) \
                    .withColumn(c, col(c).cast(DecimalType(16,4)))

        elif self.table_gold_name == 'fact_fuel_balance_acct':
            # the databricks round up some decimal values
            list_cols_to_round = ['fuel_balance_qty_mt']

            for c in list_cols_to_round:
                df = df.withColumn(c, col(c).cast(DecimalType(16,3)))\
                        .withColumn(c, col(c).cast(DoubleType())) \
                        .withColumn(c, round(col(c), 2))

        elif self.table_gold_name == 'fact_fuel_inventory':
            # the databricks round up some decimal values
            list_cols_to_round = ['fuel_inventory_qty_mt']

            for c in list_cols_to_round:
                df = df.withColumn(c, col(c).cast(DecimalType(16,3)))\
                        .withColumn(c, col(c).cast(DoubleType())) \
                        .withColumn(c, round(col(c), 1))

        elif self.table_gold_name == 'fact_voyage_vol_carried_mthly_agg':
            # the databricks round up some decimal values
            list_cols_to_round = ['voy_cust_month_qty_mt']

            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 0))\
                        .withColumn(c, col(c).cast(DecimalType(16,4)))
        
        elif self.table_gold_name == 'fact_fuel_comp_monthly_ratios':
            # the databricks round up some decimal values
            list_cols_to_round = ['fuel_comp_ratio']

            for c in list_cols_to_round:
                df = df.withColumn(c, col(c).cast(DecimalType(16,3)))\
                        .withColumn(c, col(c).cast(DoubleType())) \
                        .withColumn(c, round(col(c), 2))

        elif self.table_gold_name == 'fact_ship_pool_ownership_mthly':
            # the databricks round up some decimal values
            list_cols_to_round = ['pool_adjusted_vcu_amt', 'ship_adjusted_vcu_amt']
            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 5)) \
                        .withColumn(c, col(c).cast(DecimalType(18, 10)))

            list_cols_to_round = ['ship_adjusted_ownrshp_ratio', 'ship_participation_ratio',
                                  'ship_unadjusted_ownrshp_ratio', 'month_revenue_days_num']
            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 5)) \
                        .withColumn(c, col(c).cast(DoubleType()))

        elif self.table_gold_name == 'fact_ship_vol_carried_mthly_agg':
            # the databricks round up some decimal values
            list_cols_to_round = ['vol_carried_mt']

            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 0)) \
                        .withColumn(c, col(c).cast(DecimalType(18,2)))

#        elif self.table_gold_name == 'fact_ship_pool_share_mthly':
#            # the databricks round up some decimal values
#            list_cols_to_round = ['charter_ead_share_amt', 'core_ead_tce_prior_share_amt', 'core_tce_prior_share_amt', 'share_total_amt']

#            for c in list_cols_to_round:
#                df = df.withColumn(c, round(col(c), 1)) \
#                        .withColumn(c, col(c).cast(DecimalType(16,4)))

        elif self.table_gold_name == 'fact_voyage_pnl':
            # the databricks round up some decimal values
            list_cols_to_round = ['cad_amt']

            for c in list_cols_to_round:
                df = df.withColumn(c, round(col(c), 1)) \
                        .withColumn(c, col(c).cast(DecimalType(16,4)))

        return df

    def __handle_sensitive_cols(self, df: DataFrame) -> DataFrame:
        """
        For HR some columns are encrypted. This function decrypts these columns:
 
        #### Returns:
            * df (DataFrame): A df with filtered rowss.
        """
        # decrypt sensitive columns for HR tables
        if self.table_gold_name == 'dim_person' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['last_name','first_name','middle_name','full_name','title_name','sex_code','marital_status_code','highest_educ_lev_code','email_address']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        trim(decrypt_column(c))
                    ).otherwise(col(c))
                )

            list_date_cols_to_decrypt = ['birth_date','death_date','marital_status_date','award_date','original_hire_date']
            for c in list_date_cols_to_decrypt:
                df = df.withColumn(c,decrypt_column(c).cast(TimestampType()))

            list_for_phone_number = ['phone_number']
            secret_expression = _secret_value(_secret_key)
            for c in list_for_phone_number:
                df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        expr(f"""
                            LTRIM(RTRIM(CONCAT(CAST(aes_decrypt(unbase64(split_part({c},' ',-3)), {secret_expression}, {_mode}) AS STRING),
                            ' ',
                            CAST(aes_decrypt(unbase64(split_part({c},' ',-2)), {secret_expression}, {_mode}) AS STRING),
                            ' ',
                            CAST(aes_decrypt(unbase64(split_part({c},' ',-1)), {secret_expression}, {_mode}) AS STRING)
                            )))
                        """)
                    ).otherwise(col(c))
                )


        elif self.table_gold_name == 'dim_person_eval_header' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['person_full_name','manager_full_name']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        decrypt_column(c)
                    ).otherwise(col(c))
                )

        elif self.table_gold_name == 'dim_person_eval_step_info' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['compl_by_full_name','partic_full_name']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        decrypt_column(c)
                    ).otherwise(col(c))
                )

        elif self.table_gold_name == 'dim_person_assignment_info' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['person_full_name','payroll_emp_no']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        decrypt_column(c)
                    ).otherwise(col(c))
                )

        elif self.table_gold_name == 'dim_person_goal_info' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['assignor_full_name','requester_full_name']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        decrypt_column(c)
                    ).otherwise(col(c))
                )

        elif self.table_gold_name == 'dim_position' and self.schema_gold_name == 'dw_hr':

            list_str_cols_to_decrypt = ['manager_full_name']

            for c in list_str_cols_to_decrypt:
                 df = df.withColumn(c,
                    when(
                        (col(c).isNotNull()) &
                        (col(c) != '') &
                        (col(c) != 'UN') &
                        (col(c) != 'NA') &
                        (col(c) != 'Not applicable') &
                        (col(c) != 'Unknown'),
                        decrypt_column(c)
                    ).otherwise(col(c))
                )
        return df

    def log_execution_parameters(self) -> None:
        """
        ### Execute LOG parameters

        #### Args:
            None
        """
        self.logger.info("************ Execution Parameters ************")
        self.logger.info(f"environment:       {self.env}")
        self.logger.info(f"step_layer:        {self.step_layer}")
        self.logger.info(f"table_type:        {self.table_type}")
        self.logger.info(f"catalog_gold_name: {self.catalog_gold_name}")
        self.logger.info(f"schema_gold_name:  {self.schema_gold_name}")
        self.logger.info(f"table_gold_name:   {self.table_gold_name}")
        self.logger.info(f"path_gold_table:   {self.path_gold_table}")
        self.logger.info(f"catalog_db_name:   {self.catalog_db_name}")
        self.logger.info(f"schema_db_name:    {self.schema_db_name}")
        self.logger.info(f"table_db_name:     {self.table_db_name}")
        self.logger.info(f"path_azure_table:  {self.path_azure_table}")
        self.logger.info(f"list_col_prefix:   {self.list_col_prefix}")
        self.logger.info(f"list_skip_cols:    {self.list_skip_cols}")
        self.logger.info(f"list_skip_cols_content:   {self.list_skip_cols_check_content}")
        self.logger.info(f"list_skip_cols_not_null:  {self.list_skip_cols_not_null}")
        self.logger.info(f"list_skip_cols_fk_constr: {self.list_skip_cols_fk_constraint}")

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
        ### Get the data from gold layer in Unity Catalog and Azure Magellan sql_database

        #### Returns:
            * df_uc (DataFrame): UnityCatalog gold source table
            * df_azure (DataFrame): Azure Magellan csldw source table
        """
        df_uc = initialize_and_prepare_delta(spark=self.spark, path_delta=self.path_gold_table)
        df_azure = initialize_and_prepare_delta(spark=self.spark, query=self.query, db_catalog_name=self.catalog_db_name, db_name=self.sql_database)
        df_azure = self.__remove_prefix_col(df_azure)
        df_azure = self.__rename_col(df_azure)

        # handle sensitive
        df_uc = self.__handle_sensitive_cols(df_uc)

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
            if c not in self.list_skip_cols_not_null:
                self.logger.info(f"expect_col_not_null: {c}")
                exp = ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": c},
                )
                expectations.add_expectation(exp)

        # col_exists
        for c in self.__get_list_cols_must_exists(df_expected):
            self.logger.info(f"expect_col_to_exist: {c}")
            exp = ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": c},
            )
            expectations.add_expectation(exp)

        # type
        list_col_names_target = [f.name.lower() for f in schema_expected]
        list_col_types_target = [str(type(f.dataType).__name__) for f in schema_expected]
        for col_name, col_type in zip(list_col_names_target, list_col_types_target):
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
            kwargs={"value": int(df_expected.select(self.__get_list_pk_cols()).count())},
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
        self.logger.info("expect_table_correct_storage_location")
        tuple_results = custom_qa.check_if_data_path_is_correct(self.spark, self.path_gold_table, 'BI')
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
        self.logger.info("expect_table_have_correct_owner")
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
        self.logger.info("expect_table_correct_table_type")
        tuple_results = custom_qa.check_if_table_type_is_correct(self.spark, self.path_gold_table)
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
        self.logger.info("expect_table_correct_data_format")
        tuple_results = custom_qa.check_if_table_data_format_is_correct(self.spark, self.path_gold_table)
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
        self.logger.info("expect_table_are_not_empty")
        tuple_results = custom_qa.check_if_two_df_are_not_empty(self.spark, df_expected, self.path_gold_table, self.path_azure_table)
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
        self.logger.info("expect_table_have_same_schema")
        have_same_schema = custom_qa.check_if_two_df_have_same_schema(schema_expected, schema_observed)
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
        self.logger.info("expect_table_count_distinct (using PK cols)")
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

        # Creates a dataframe that tolerates floating variation in decimal fields
        df_expected_floating = df_expected
        df_observed_floating = df_observed
        if self.truncate_decimal_values:
            df_expected_floating = self.truncate_decimal_values_from_df(df_expected, self.floating_scale)
            df_observed_floating = self.truncate_decimal_values_from_df(df_observed, self.floating_scale)

        # equal count distinct -> col level
        for c in self.__get_list_business_cols(df_expected) + self.__get_list_sk_cols(df_expected):
            self.logger.info(f"expect_col_count_dist: {c}")
            tuple_results = gold_test.check_if_col_have_same_count_distinct(
                col_name=c,
                df_expected=df_expected_floating,
                df_observed=df_observed_floating,
                path_table_observed=self.path_gold_table,
                path_table_expected=self.path_azure_table,
            )
            exp = ExpectationValidationResult(
                success=(tuple_results[0] is True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_same_count_distinct",
                    kwargs={
                        "column": c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # equal content -> col level
        # check only business columns (without _SK and ETL_)
        for c in self.__get_list_business_cols(df_expected):
            if c not in self.list_skip_cols_check_content:
                self.logger.info(f"expect_same_content_rows: {c}")
                tuple_results = gold_test.check_same_content_rows(
                    df_expected=df_expected_floating,
                    df_observed=df_observed_floating,
                    list_timestamp_cols=self.__get_list_timestamp_cols(),
                    list_pk=self.__get_list_pk_cols(),
                    c=c,
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

        # total null -> col level
        for c in self.__get_list_business_cols(df_expected):
            self.logger.info(f"expect_same_total_nulls: {c}")
            tuple_results = gold_test.check_if_column_have_same_total_nulls(
                df_expected=df_expected,
                df_observed=df_observed,
                col_name=c,
            )
            exp = ExpectationValidationResult(
                success=tuple_results[0] is True,
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_same_total_nulls",
                    kwargs={
                        "column": c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1]
                    },
                ),
            )
            add_custom_result_to_validation(exp, validation_results)

        # not null constraint
        for c in self.__get_list_business_cols(df_expected) + self.__get_list_sk_cols(df_expected):
            self.logger.info(f"expect_col_have_equal_not_null_constraint: {c}")
            tuple_results = gold_test.check_if_col_have_equal_not_null_constraint(
                df_observed_info_schema=self._get_uc_info_schema(),
                df_expected_info_schema=self._get_sql_server_info_schema(),
                dict_rename_cols=self.rename_col_dict,
                col_name=c,
            )
            exp = ExpectationValidationResult(
                success=(tuple_results[0] == True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_equal_not_null_constraint",
                    kwargs={
                        "column": c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1],
                    },
                )
            )
            add_custom_result_to_validation(exp, validation_results)

        # pk_constraint
        for c in self.__get_list_pk_cols():
            self.logger.info(f"expect_column_have_pk_constraint: {c}")
            tuple_results = gold_test.check_if_column_have_constraint(
                list_expected_constraints=self._get_sqlserver_constraints('pk'),
                list_observed_constraints=self._get_uc_constraints('pk', self.path_gold_table),
                path_table_expected=self.path_azure_table,
                path_table_observed=self.path_gold_table,
                col_name=c,
                constr_type='pk',
                list_pk_cols=self.__get_list_pk_cols(),
            )
            exp = ExpectationValidationResult(
                success=(tuple_results[0] == True),
                expectation_config=ExpectationConfiguration(
                    expectation_type="expect_column_have_pk_constraint",
                    kwargs={
                        "column": c,
                        "result": tuple_results[0],
                        "msg": tuple_results[1],
                    },
                )
            )
            add_custom_result_to_validation(exp, validation_results)

        # exists in data catalog
        if self.catalog_gold_name == "test_silver_trsf":
            self.logger.info("SKIPPING expect_table_exists_in_data_catalog")
        else:
            self.logger.info("expect_table_exists_in_data_catalog")
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

        # elapsed time
        self.logger.info("expect_table_have_same_query_elapsed_time")
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
        # default values
        if self.table_type == 'dim':
            self.logger.info("expect_table_have_same_default_values")
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

        # exists sk, check the constraint and referential_integrity
        if len(self.__get_list_sk_cols(df_expected)) > 0:
            # fk_constraints
            time.sleep(0.2) # sync log
            for c in self.__get_list_sk_cols(df_expected):
                if c not in self.list_skip_cols_fk_constraint:
                    self.logger.info(f"expect_col_fk: {c}")
                    tuple_results = gold_test.check_if_column_have_constraint(
                        list_expected_constraints=self._get_sqlserver_constraints('fk'),
                        list_observed_constraints=self._get_uc_constraints('fk', self.path_gold_table),
                        path_table_expected=self.path_azure_table,
                        path_table_observed=self.path_gold_table,
                        col_name=c,
                        constr_type='fk',
                        list_pk_cols=self.__get_list_pk_cols(),
                    )
                    exp = ExpectationValidationResult(
                        success=(tuple_results[0] == True),
                        expectation_config=ExpectationConfiguration(
                            expectation_type="expect_column_have_fk_constraint",
                            kwargs={
                                "column": c,
                                "result": tuple_results[0],
                                "msg": tuple_results[1],
                            },
                        )
                    )
                    add_custom_result_to_validation(exp, validation_results)

            if c not in self.list_skip_cols_fk_constraint:
                # referential_integrity
                for c in self.__get_list_fk_cols():
                    tuple_results = gold_test.check_if_column_have_referential_integrity(
                        spark=self.spark,
                        df_observed_constraints=self._get_uc_constraints('fk', self.path_gold_table, get_df=True),
                        path_gold_table=self.path_gold_table,
                        col_name=c,
                    )
                    self.logger.info(f"expect_col_ref_integrity: {c}")
                    exp = ExpectationValidationResult(
                        success=(tuple_results[0] == True),
                        expectation_config=ExpectationConfiguration(
                            expectation_type="expect_column_have_referential_integrity",
                            kwargs={
                                "column": c,
                                "result": tuple_results[0],
                                "msg": tuple_results[1],
                            },
                        )
                    )
                    add_custom_result_to_validation(exp, validation_results)

        return validation_results

    @staticmethod
    def truncate_decimal_values_from_df(df: DataFrame, floating_scale=5) -> DataFrame:
        """
        Applies decimal truncation to dataframes, in order to be compare numeric values between another dataframe
        """
        for field in df.schema.fields:
            if isinstance(field.dataType, DecimalType):
                new_precision = field.dataType.precision
                # keep a minimum of 3 decimal digits
                new_scale = max(3, field.dataType.scale - 2)
                df = df.withColumn(field.name, col(field.name).cast(DecimalType(new_precision, new_scale)))
            elif isinstance(field.dataType, DoubleType) or isinstance(field.dataType, FloatType):
                df = df.withColumn(field.name, col(field.name).cast(DecimalType(25, floating_scale)))
        return df

    @staticmethod
    def display_unmatched_pairs(df_expected: DataFrame, df_observed: DataFrame, *field_names) -> None:
        """
        Displays unmatched value pairs between columns of two dataframes
        Args:
            df_expected (DataFrame): The DataFrame representing expected data.
            df_observed (str): Name of the DataFrame containing expected data.
            field_names: The name of the columns to be compared.

        """
        import pandas as pd
        for field_name in field_names:
            df_expected_values = set([row[field_name] for row in df_expected.select(field_name).distinct().collect()])
            df_observed_values = set([row[field_name] for row in df_observed.select(field_name).distinct().collect()])
            missing_in_expected = sorted(list(df_observed_values - df_expected_values))
            missing_in_observed = sorted(list(df_expected_values - df_observed_values))

            df = pd.DataFrame({
                'Non-matching Values': pd.Series(missing_in_expected),
                'Missing/Expected Values': pd.Series(missing_in_observed)
            })
            print("=" * 50)
            print(f"{field_name.upper()} comparison:\n")
            print(f"unique values EXPECTED: {len(df_expected_values)}")
            print(f"unique values OBSERVED: {len(df_observed_values)}")
            print("=")
            print(f"Unmatched values pairs for field: {field_name.upper()}")
            print(df)
            print("\n")

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
