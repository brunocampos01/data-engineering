
import json
import ast
import os
from typing import (
    List,
    Dict,
    Tuple,
)
from pprint import pprint
from enum import Enum

from dbruntime.dbutils import DBUtils

from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
)
from pyspark.sql.functions import (
    col,
    sha2,
    concat_ws,
    lit,
)
from pyspark.sql.types import (
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    LongType,
    DateType,
    StructField,
    StructType,
    ArrayType,
    BooleanType,
    ByteType,
    FloatType,
    BinaryType,
    base64,
    DoubleType,
    ShortType,
)

from library.database.azure_sql import AzureSQL
from library.database.sqlserver_dw_sql import SQLServerDWSql

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
        return False, 'They are not equal! EXPECTED: {tuple_expected[1]} | OBSERVED: {tuple_observed[1]}'


def check_if_table_data_format_is_correct(spark: SparkSession, path_uc_table: str) -> Tuple[bool, str]:
    """
    Checks if the data format of a table is correct.
    #### Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating if the format
        is correct and a message describing the format or the error encountered.
    """
    data_format_table_silver_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
        .filter(col('col_name') == 'Provider') \
        .select('data_type') \
        .collect()[0][0]

    # great expectations output
    if data_format_table_silver_observed == 'delta':
        return True, f'Delta table'
    else:
        return False, 'The data format for this table is wrong! ' \
                      f'EXPECTED: DELTA | OBSERVED: {data_format_table_silver_observed}'


def check_if_table_type_is_correct(spark: SparkSession, path_uc_table: str) -> Tuple[bool, str]:
    """
    Check if the table type is correct.
    #### Returns:
        Tuple[bool, str]: A tuple containing a boolean indicating whether the table type is correct
        and a message describing the result.
    """
    type_table_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
        .filter(col('col_name') == 'Type') \
        .filter(col('data_type').isin(['EXTERNAL', 'MANAGED'])) \
        .select('data_type') \
        .collect()[0][0]

    # great expectations output
    if type_table_observed == 'EXTERNAL':
        return True, f'The table is using external storage.'
    else:
        return False, 'The table type is wrong! ' \
                      f'EXPECTED: EXTERNAL | OBSERVED: {type_table_observed}'


def check_if_data_path_is_correct(spark: SparkSession, path_uc_table: str, azure_folder: str) -> Tuple[bool, str]:
    """
    Check if the data path is correct.
    #### Returns:
        Tuple[bool, str]: A tuple where the first element is a boolean indicating
        whether the path is correct, and the second element is a message describing the result.
    """
    path_observed = spark.sql(f"DESCRIBE EXTENDED {path_uc_table}") \
        .filter(col('col_name') == 'Location') \
        .filter(col('data_type').like('abfss%')) \
        .select('data_type') \
        .collect()[0][0]

    # great expectations output
    if azure_folder in path_observed:
        return True, f'The data path in Azure is located correctly at {path_observed}'
    else:
        return False, 'The data path in Azure is not correct! ' \
                      f'EXPECTED: inside the {azure_folder}/ folder | OBSERVED: {path_observed}'


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


def get_business_cols(df: DataFrame, list_deny_cols: List) -> DataFrame:
    """
    ### Removes Surrogate keys and ETL columns for speficif test: `expect_same_content_rows`

    #### Args:
        * df (DataFrame): Input DataFrame
        * list_deny_cols (list): cols that we need to exclude from tests

    #### Returns:
        * DataFrame: DataFrame after removing SK keys
    """
    filtered_cols = [c
                     for c in df.columns
                     if not c.startswith('ETL_') and not c.endswith('_SK') and c not in list_deny_cols]

    return df.select(*filtered_cols)


def parser_list_cols_to_str(list_cols: List) -> str:
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


def generate_information_schema_query(table_name: str, schema_name: str, catalog_name: str) -> str:
    """
    ### Generate a query for fetching properties from the information schema based on the table_type.

    #### Args:
        * table_name (str): e.g.: `dim_ledger`
        * schema_name (str): e.g.: `dw`
        * catalog_name (str): e.g.: `csldw`

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
        FROM
            INFORMATION_SCHEMA.COLUMNS
        WHERE
            TABLE_CATALOG = '{catalog_name}' AND
            TABLE_SCHEMA = '{schema_name}' AND
            TABLE_NAME = '{table_name}'
        """


def check_if_checksum_are_equal(spark: SparkSession,
                                table_name_dim: str,
                                schema_name: str,
                                catalog_name: str,
                                list_dim_tables_name: List) -> tuple[bool, str]:
    """
    ### Check if checksums are equal for a list of dimension tables.

    #### Args:
        * spark (SparkSession): The Spark session.
        * table_name_dim (str): The name of the dimension table.
        * schema_name (str): The schema name of the table.
        * catalog_name (str): The catalog name of the table.
        * list_dim_tables_name (list): List of dimension table names.

    #### Returns:
        * bool: boolean indicating whether the checksums are equal
        * str: message describing the result.
    """
    for table_name in list_dim_tables_name:
        # if '.' in table_name:
        #     schema_name, table_name_dim = table_name.split('.', 1)
        # else:
        #     schema_name = schema_name
        #     table_name_dim = table_name

        # if schema_name == 'stg':
        #     logger.warning(f"Skipping the {schema_name}.{table_name_dim} table for checksum")
        #     continue

        df_dim_source = generate_checksum_col(
            spark=spark,
            schema_name=schema_name,
            table_name_dim=table_name_dim,
            catalog_name=catalog_name,
            env_name='azure',

        )
        df_dim_target = generate_checksum_col(
            spark=spark,
            schema_name=schema_name,
            table_name_dim=table_name_dim,
            catalog_name=catalog_name,
            env_name='onpremises',
        )
    df_diff_source_target, df_diff_target_source = find_df_diff(df_dim_source, df_dim_target)
    total_df_diff_src_trg = df_diff_source_target.count()
    total_df_diff_trg_src = df_diff_target_source.count()

    if total_df_diff_src_trg > 0 or total_df_diff_trg_src > 0:
        return False, f'The measures was tested with corresponding dims business key column(s) for each involved dimensions. ' \
                      f'Make sure that dimensions are tested first and pass the data tests. ' \
                      f'Checked for these dimesions table: {list_dim_tables_name}'
    else:
        return True, f'The measures was tested with corresponding dims business key column(s) for each involved dimensions. ' \
                     f'Checked for these dimesions table: {list_dim_tables_name}'


def generate_checksum_col(
    spark: SparkSession,
    table_name_dim: str,
    schema_name: str,
    catalog_name: str,
    env_name: str,
    list_deny_cols: List = [],
) -> DataFrame:
    """
    ### Generate checksum column for a dimension table.

    #### Args:
        * spark (SparkSession): The Spark session.
        * table_name_dim (str): The name of the dimension table.
        * schema_name (str): The schema name of the table.
        * catalog_name (str): The catalog name of the table.
        * env_name (str): Environment. `[dev, test, prd]`
        * list_deny_cols (list): List of Deny columns.

    #### Returns:
        * DataFrame: DataFrame with checksum generated column
    """
    if env_name == 'azure':
        loader = AzureSQL()
    else:
        loader = SQLServerDWSql()

    if '.' in table_name_dim:
        schema_name, table_name = table_name_dim.split('.', 1)
    else:
        schema_name = schema_name
        table_name = table_name_dim

    query = f'SELECT * FROM {schema_name}.{table_name}'
    df_dim = (
        spark.read.format('jdbc')
        .options(**loader.options(catalog_name))
        .option("query", query)
        .load()
    )
    # Get the list of columns to keep
    cols_to_keep = [c for c in df_dim.columns
                    if not c.startswith('ETL_')
                    and not c.endswith('_SK')
                    and not c.endswith("DATE")
                    and not c.endswith("DT")
                    and c not in list_deny_cols]
    # remove unnecessary cols and order
    if len(cols_to_keep) > 0:
        df_dim = df_dim.select(*cols_to_keep).orderBy(*cols_to_keep)
    else:
        df_dim = df_dim.select(*cols_to_keep)

    # Concatenate the selected columns into a single string
    concatenated_col = concat_ws("", *[col(c)
                                       for c in cols_to_keep])
    # Calculate the checksum using SHA-256
    return df_dim \
        .withColumn("CHECK_SUM", sha2(concatenated_col, 256))


def get_and_prepare_data_dim(
    spark: SparkSession,
    dict_cols: Dict[str, str],
    catalog_name: str,
    schema_name: str,
) -> Dict[str, List]:
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
            spark.read.format('jdbc')
            .options(**azure_loader.options(catalog_name))
            .option("query", query)
            .load()
        )

        # get distinct values from col and transform in a list
        list_pk = [row[column] for row in df_values_raw.collect()]
        dict_cols_names[column] = list_pk
        logger.info(f'In {column} col founded {len(list_pk)} at {schema_name}.{table}')

    return dict_cols_names


def get_and_prepare_data_for_referential_integraty_test(
    spark: SparkSession,
    dict_cols_names: Dict,
    df_constraints: DataFrame,
) -> DataFrame:
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
    df_values = spark.createDataFrame([
        (k, v)
        for k, values in dict_cols_names.items()
        for v in values
    ], schema_df)
    df = df_constraints.join(df_values, on='dim_pk', how='inner')

    return df.groupBy("fact_sk", "dim_pk").agg(F.collect_list('dim_values').alias('dim_values'))


def get_sk_fact_names_and_pk_dim_names(spark: SparkSession,
                                       table_name: str,
                                       catalog_name: str,
                                       list_dim_cols_name: List) -> DataFrame:
    """
    ### Prepares DataFrame with constraints `DimensionPK` & `FactSK`

    #### Args:
        * spark (SparkSession): The Spark session.
        * table_name (str): The name of the Azure SQL Fact table
        * catalog_name (str): The name of the Azure SQL catalog/database. e.g.: `csldw`
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
    INNER JOIN
        sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
    INNER JOIN
        sys.tables fact_table ON fk.parent_object_id = fact_table.object_id
    INNER JOIN
        sys.columns fact_columns ON fkc.parent_object_id = fact_columns.object_id
        AND fkc.parent_column_id = fact_columns.column_id
    INNER JOIN
        sys.tables dim_table ON fk.referenced_object_id = dim_table.object_id
    INNER JOIN
        sys.columns dim_columns ON fkc.referenced_object_id = dim_columns.object_id
        AND fkc.referenced_column_id = dim_columns.column_id
    WHERE
        fact_table.name = '{table_name}'
        AND fact_table.is_ms_shipped = 0
        AND dim_table.is_ms_shipped = 0
        AND dim_columns.name IN (
            {list_dim_cols_name}
        )
    '''
    azure_loader = AzureSQL()

    logger.info(f'Executing the query on Azure: {query}')
    return (
        spark.read.format('jdbc')
        .options(**azure_loader.options(catalog_name))
        .option("query", query)
        .load()
    )


def df_to_dict(df: DataFrame, key_column: List, value_column: List) -> Dict:
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


def check_if_two_df_are_not_empty(
    spark: SparkSession,
    df_expected: DataFrame,
    path_uc_observed: str,
    path_uc_expected: str,
) -> Tuple[bool, str]:
    """
    Check if two DataFrames are not empty.
    #### Args:
        - df_expected (DataFrame): The expected DataFrame.
    #### Returns:
        - Tuple[bool, str]: A tuple containing a boolean indicating whether the DataFrames are not empty and a message.
    """
    total_observed = spark.sql(f'SELECT COUNT(*) FROM {path_uc_observed}').collect()[0][0]
    total_expected = df_expected.count()

    # great expectations output
    if total_observed != 0 and total_expected != 0:
        return True, f'Both tables are not empty.'
    else:
        if total_observed == 0:
            empty_table = path_uc_observed
        else:
            empty_table = path_uc_expected
        return False, f'{empty_table} is empty! Check the Unity Catalog and delta tables.'


def check_if_two_df_have_same_schema(schema_expected: StructType, schema_observed: StructType) -> Tuple[bool, str]:
    """
    Check if two Spark DataFrames have the same schema.
    #### Args:
        - schema_expected (pyspark.sql.types.StructType): The expected schema.
        - schema_observed (pyspark.sql.types.StructType): The observed schema.
    #### Returns:
        - Tuple[bool, str]: A tuple where the first element is a boolean indicating if the schemas are equal,
                        and the second element is a message describing the result.
    """
    list_expected = sorted([(field.name, field.dataType) for field in schema_expected.fields])
    list_observed = sorted([(field.name, field.dataType) for field in schema_observed.fields])
    diff_expected_observed = set(list_expected) - set(list_observed)
    diff_observed_expected = set(list_observed) - set(list_expected)

    # great expectations output
    if list_expected == list_observed:
        return True, 'The expected schema and schema_json from file are equal.'
    else:
        return False, f'''The schemas are not equal! Check the schema_json declared in file. schema_expected - schema_observed = {diff_expected_observed} __ | __ schema_observed - schema_expected = {diff_observed_expected}'''


def check_if_data_precision_is_equal(
    schema_df_source: StructType,
    df_source_name: str,
    schema_df_target: StructType,
    df_target_name: str,
) -> tuple[bool, str]:
    """
    ### Checks if the data precision of DecimalType columns in two df schemas are equal.

    #### Args:
        schema_df_source (StructType): The source of truth, expected
        df_source_name (str): e.g.: `on-premises`
        schema_df_target (StructType): The schema of the target, observed.
        df_target_name (str): e.g.: `azure`

    #### Returns:
        * bool: indicating if data precision is True or False
        * str: error message
    """
    cols_prec_df_source = get_cols_with_data_precision(schema_df_source)
    cols_prec_df_target = get_cols_with_data_precision(schema_df_target)

    if sorted(cols_prec_df_source) == sorted(cols_prec_df_target):
        return True, ""
    else:
        error_msg = (
            f"Mismatch data precision between cols! "
            f"The cols with data precision in {df_source_name}: {cols_prec_df_source} | " # fmt: off # noqa
            f"The cols with data precision in {df_target_name}: {cols_prec_df_target}"
        )
        return False, error_msg


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


def get_col_not_be_null(df_properties: DataFrame) -> List:
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


def check_if_dim_is_not_empty(df: DataFrame, df_name: str, df_default_values: DataFrame) -> tuple[bool, str]:
    """
    ### Checks if dimension table is not empty

    #### Args:
        * df (DataFrame): DataFrame with AzureSQL dimension table
        * df_name (str): AzureSQL dimension table name
        * df_default_values (DataFrame): DataFrame with AzureSQL table default values

    #### Returns:
        * bool: True if table not empty, False if empty
        * str: message
    """
    col_name = df.columns[0]
    df_result = df.select(col_name).exceptAll(df_default_values)
    total_rows = df_result.count()

    if total_rows > 0:
        return True, f'Checked for {df_name}'
    else:
        msg = f"Business rows without found! There are only defaults values in {df_name}."
        return False, msg


def check_if_two_df_contain_same_schema(df_expected: DataFrame, df_observed: DataFrame) -> None:
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


def check_if_two_df_contain_diff_content_rows(df_expected: DataFrame, df_observed: DataFrame) -> None:
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
                     f' Showing the rows only exists in df_expected:\n {df_diff_tgt_src.display()}' # fmt: off # noqa
                     f'|  Showing the rows only exists in df_observed:\n {df_diff_src_tgt.display()}\n') # fmt: off # noqa


def check_if_two_df_contain_diff_content_rows_count(df_expected: DataFrame, df_observed: DataFrame) -> None:
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
                     f' | '
                     f'Total rows only exists in df_observed: {total_df_diff_trg_src}'
                     f' | '
                     f'Checked for these cols: {df_expected.columns}')


def check_if_two_df_contain_same_count(df_expected: DataFrame, df_observed: DataFrame) -> None:
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
               f' df_expected = {count_df_expected} '
               f'|  df_observed = {count_df_observed}')


def check_if_two_df_contain_same_count_per_column(
    df_expected: DataFrame,
    df_observed: DataFrame,
    common_cols: List,
) -> None:
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
                        f"Expected: {total_rows_col_df1}"
                        f" | Observed: {total_rows_col_df2}")


def check_if_two_df_contain_same_unique_count_per_column(df_expected: DataFrame,
                                                         df_observed: DataFrame,
                                                         common_cols: List) -> None:
    """
    ### Checks if two DataFrames have the same unique count per column. This function compares the number of unique values in each column of the provided df

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


def check_if_two_df_contain_same_null_count_per_column(df_expected: DataFrame,
                                                       df_observed: DataFrame,
                                                       common_cols: List) -> None:
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


def initialize_and_prepare_delta(
    spark: SparkSession,
    list_drop_cols: List = [],
    path_delta: str = None,
    query: str = None,
    catalog_azure_name: str = None,
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
    if query is not None:
        from library.database.azure_sql import AzureSQL
        df = AzureSQL.get_df_from_sql(spark=spark, catalog_azure_name=catalog_azure_name, query=query)
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
