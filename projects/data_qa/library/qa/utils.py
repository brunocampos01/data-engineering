"""
### Module - Utils for QA Library

#### Functions:
    * get_business_cols: Removes Surrogate keys and ETL columns for speficif test: `expect_same_content_rows`
    * parser_list_cols_to_str: Parse list's elements into a single string
    * generate_information_schema_query: Generate a query for fetching properties from the information schema based on the table_type.
    * check_if_checksum_are_equal: Check if checksums are equal for a list of dimension tables.
    * generate_checksum_col: Generate checksum column for a dimension table.
    * get_and_prepare_data_dim: Gets PK from dimension tables and prepare the data for further processing to check referential integrity. This function retrieves distinct data from dimension tables based on the specified columns received in input.
    * get_and_prepare_data_for_referential_integraty_test: Prepares DataFrame with `DimensionPK` & `FactSK` and distinct values
    * get_sk_fact_names_and_pk_dim_names: Prepares DataFrame with constraints `DimensionPK` & `FactSK`
    * df_to_dict: Converts DataFrame rows to a dict
    * get_cols_with_data_precision: Gets a list of column names and their corresponding data precision (scale).
    * check_if_data_precision_is_equal: Checks if the data precision of DecimalType columns in two df schemas are equal.
    * find_df_diff: Finds the differences between two DataFrames and return them.
    * get_col_not_be_null: Gets not nullable columns from DataFrame
    * lowercase_field_names_in_struct_type: Converts the field names in a StructType to lowercase.
    * remove_struct_field_in_struct_type: Removes specific fields from a StructType schema.
    * get_data_type_mapping: Get a dict mapping data type strings to their corresponding PySpark data types.
    * get_diff_cols_between_df: Calculates the column differences between two DataFrames (df1 and df2).
    * initialize_and_prepare_delta: Initializes a df and prepare: lower, order, and strip columns
"""
import json
import ast
import os
from typing import (
    List,
    Dict,
    Tuple,
    Any,
)
from pprint import pprint
from enum import Enum

from dbruntime.dbutils import DBUtils

from pyspark.sql import (
    DataFrame,
    SparkSession,
    Row,
)
from pyspark.sql.functions import (
    col, 
    lit,
)
from pyspark.sql.types import (
    DecimalType,
    DataType,
    StructType,
)

from library.database.azure_sql import AzureSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql
from library.qa.great_expectations_helper import GreatExpectationsHelper
from library.logger_provider import LoggerProvider

logger = LoggerProvider.get_logger()


class LogTag(Enum):
    """
    Enumeration representing different types of tags for log messages and keep type safety.
    These tags are used in test_data.py file and in some custom data tests.
    """
    ROW_CONTENT = 'ROW_CONTENT'
    DATA_TYPE = 'DATA_TYPE'
    CHARACTER_MAXIMUM_LENGTH = 'CHARACTER_MAXIMUM_LENGTH'
    NUMERIC_PRECISION = 'NUMERIC_PRECISION'
    NUMERIC_SCALE = 'NUMERIC_SCALE'
    DATETIME_PRECISION = 'DATETIME_PRECISION'
    COLLATION_NAME = 'COLLATION_NAME'
    DEFAULT_VALUES = 'DEFAULT_VALUES'
    IS_NULLABLE = 'IS_NULLABLE'


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


def get_common_cols(df_expected: DataFrame, df_observed: DataFrame) -> List[str]:
    expected_cols = set(df_expected.columns)
    observed_cols = set(df_observed.columns)
    return list(expected_cols.intersection(observed_cols))


def get_file_from_widget(dbutils: DBUtils, path_with_file: str, is_json: bool = None):
    """
    Args:
        path_with_file (str): The path in Databricks.
        e.g.: 'resources/sql/bi_commercial/dimensions/dim_vessel.sql'

    """
    path = dbutils.widgets.get(path_with_file)
    file_path = "".join(os.getcwd() + "/" + path)
    if is_json:
        return json.loads(open(file_path).read())
    else:
        return open(file_path).read()


def get_list_from_widget(dbutils: DBUtils, input_string: str) -> List[str]:
    """
    Function to process a comma-separated string by stripping whitespaces and converting to lowercase.

    Args:
        input_string (str): The comma-separated string to be processed.

    Returns:
        list: A list containing stripped and lowercased elements.
    """
    try:
        str_list = dbutils.widgets.get(input_string)
        return [x.strip().lower() for x in str_list.split(",")]
    except:
        return []


def get_dict_from_widget(dbutils: DBUtils, input_string: str) -> Dict:
    """
    Args:
        input_string (str): The dict inputed as str to be processed.
        e.g. '{'dim_parties': 'party_sk'}'

    Returns:
        Dict: A dict with informations about the relationalship.
        e.g.: {'dim_parties': 'party_sk'}
    """
    try:
        str_dict = dbutils.widgets.get(input_string).lower()
        return ast.literal_eval(str_dict)
    except Exception:
        return None


def parser_list_cols_to_str(list_cols: list) -> str:
    """
    ### Parse list's elements into a single string

    #### Args:
        * list_cols (list): list of columns. Ex: `['FACCT_ACCTD_DR_AMOUNT', 'FACCT_REGION_NET_AMOUNT']`
    
    #### Returns:
        * str: Ex: `'FACCT_ACCTD_DR_AMOUNT', 'FACCT_REGION_NET_AMOUNT'`
    """
    output_str = ', '.join(list_cols)
    # Add single quotes around each element
    return ', '.join(f"`{item}`" for item in list_cols)


def row_to_dict(row):
    """
    Convert a spark Row object into a Python dict.
    """
    d = row.asDict()
    for key, value in d.items():
        if isinstance(value, Row):
            d[key] = row_to_dict(value)
        elif isinstance(value, list):
            d[key] = [row_to_dict(x) if isinstance(x, Row) else x for x in value]
    return d


def df_to_dict(df: DataFrame, key_column: list, value_column: list) -> dict:
    """
    ### Converts DataFrame rows to a dict

    #### Args:
        * df (DataFrame): DataFrame to be converted into dict
        * key_column (list): list of columns
        * value_column (list): list of columns
        
    #### Returns:
        * dict: dictionary with DataFrame values
    """
    row_list = df.select(key_column, value_column).collect()
    
    return {row[key_column]: row[value_column] for row in row_list}


def get_cols_with_data_precision(schema: StructType) -> List[Tuple[str, int]]:
    """
    ### Gets a list of column names and their corresponding data precision (scale). This function looks for columns with a DecimalType dataType and returns a list of tuples
    containing the column name and its data precision.

    #### Args:
        * schema (StructType): pyspark DataFrame StructType

    #### Returns:
        * A list of tuples, where each tuple contains: column name (str) and its data precision (int). # fmt: off # noqa. e.g.: `[('column1', 10), ('column2', 8)]`
    """
    return sorted([
        (field.name, field.dataType.precision)
        for field in schema.fields
        if isinstance(field.dataType, DecimalType) and field.dataType.hasPrecisionInfo
    ])


def get_cols_with_data_scale(schema: StructType) -> List[Tuple[str, int]]:
    """
    ### Gets a list of column names and their corresponding data precision (scale). This function looks for columns with a DecimalType dataType and returns a list of tuples
    containing the column name and its data precision.

    #### Args:
        * schema (StructType): pyspark DataFrame StructType

    #### Returns:
        * A list of tuples, where each tuple contains: column name (str) and its data precision (int). # fmt: off # noqa. e.g.: `[('column1', 0), ('column2', 8)]`
    """
    return sorted([
        (field.name, field.dataType.scale)
        for field in schema.fields
        if isinstance(field.dataType, DecimalType) and field.dataType.hasPrecisionInfo
    ])


def find_df_diff(df_source: DataFrame, df_target: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    ### Finds the differences between two DataFrames and return them.

    #### Args:
        * df_source (DataFrame): expected data (source of truth) (e.g., `df_onpremises`)
        * df_target (DataFrame) : observed data (e.g., `df_azure`)

    #### Returns:
        * A tuple of DataFrames containing the rows that only exist in `df_source` and `df_target`, respectively.
    """
    df_diff_source_target = df_source.exceptAll(df_target)
    df_diff_target_source = df_target.exceptAll(df_source)
    
    return df_diff_source_target, df_diff_target_source


def get_col_not_be_null(df_properties: DataFrame) -> list:
    """
    ### Gets not nullable columns from DataFrame
    
    #### Args:
        * df_properties (DataFrame): DataFrame with AzureSQL table properties

    #### Returns:
        * list: list with column names not nullable
    """
    df_properties = df_properties.select('COLUMN_NAME', 'IS_NULLABLE').filter(col('IS_NULLABLE') == 'NO')
    
    return [row['COLUMN_NAME'] 
            for row in df_properties.select('COLUMN_NAME').collect()]


def get_diff_cols_between_df(df_left: DataFrame, df_right: DataFrame) -> Tuple[List[str], List[str]]:
    """
    ### Calculates the column differences between two DataFrames (df1 and df2).
    
    #### Args:
        * df_left (DataFrame): DataFrame
        * df_right (DataFrame): DataFrame
    
    #### Returns:
        * List[str]: list of columns that exist only in df1.
        * List[str]: list of columns that exist only in df2.
    """
    cols_df1 = set(df_left.columns)
    cols_df2 = set(df_right.columns)

    return list(cols_df1.difference(cols_df2)), list(cols_df2.difference(cols_df1))


def auto_analysis(
    list_cols_id: List[str], 
    validation_results: Dict, 
    df_observed: DataFrame, 
    df_expected: DataFrame,
    observed_name: str = '',
    expected_name: str = '', 
) -> None:
    list_success_and_column = GreatExpectationsHelper.get_results_from_expectation_type(validation_results, 'expect_same_content_rows')
    list_col_with_diff = []
    for i in list_success_and_column:
        if i[2] is False:
            list_col_with_diff.append(i[1])
        
    if list_col_with_diff:
        logger.info(f'Cols with diff in content: {list_col_with_diff}')
    else:
        logger.info('No differences were found in the content of the rows.')
        return None
    
    for c in list_col_with_diff[:5]:
        print(f'\n\nChecking the diff for {c}:')
        list_cols = [*list_cols_id, c]
        (
            df_diff_source_target,
            df_diff_target_source
        ) = find_df_diff(df_source=df_observed.select(*list_cols), 
                        df_target=df_expected.select(*list_cols))
        list_cols_order_by = df_diff_source_target.columns

        print(f'{observed_name}(observed) - {expected_name}(expected) rows:')
        if len(list_cols_order_by) > 0:
            df_diff_source_target.orderBy(*list_cols_order_by).display()
        else:
            df_diff_source_target.display()

        print(f'{expected_name}(expected) - {observed_name}(observed) rows:')
        if len(list_cols_order_by) > 0:
            df_diff_target_source.orderBy(*list_cols_order_by).display()
        else:
            df_diff_target_source.display()


def initialize_and_prepare_delta(
    spark: SparkSession, list_drop_cols: list = [], path_delta: str = None, query: str = None, db_catalog_name: str = None, db_name: str = None
) -> DataFrame:
    """
    ### Initializes a df and prepare: lower, order, and strip columns
    
    #### Args:
        * spark (SparkSession): used for reading the Delta table.
        * path_delta (str): The path to the Delta table. e.g.: `<catalog>.<schema>.<table>`
        * list_drop_cols (list, optional): list of columns not passing through GreatExpectations

    #### Returns:
        df (DataFrame): A df with columns ordered alphabetically (case-insensitive)
    """
    if db_name:
        if db_name.lower() == "azsql":
            from library.database.azure_sql import AzureSQL
            df = AzureSQL.get_df_from_sql(spark=spark, catalog_azure_name=db_catalog_name, query=query)
        elif db_name.lower() =="onpremise":
            from library.database.sqlserver_dw_sql import SQLServerDWSql
            loader = SQLServerDWSql()
            df = spark.read.format('jdbc')\
            .options(**loader.options(db_catalog_name))\
            .option("query", query)\
            .load()
    else:
        try:
            df = spark.read.format("delta").table(path_delta)
            if len(list_drop_cols) > 0:
                df = df.drop(*list_drop_cols)
        except AttributeError as e:
            raise AttributeError(f'path_delta = {path_delta} not found!\n{e}')

    # handle when col's name startwith '_'
    sorted_cols_filtered =[c for c in df.columns if not c.startswith('_')]
    cols_underscore =[c for c in df.columns if c.startswith('_')]
    ordered_columns = [col(c).alias(c.lower()) for c in sorted_cols_filtered]
    ordered_columns += cols_underscore

    return df.select(*ordered_columns)
