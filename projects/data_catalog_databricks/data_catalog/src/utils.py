import os
from typing import (
    Dict,
    List,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col

from library.logger_provider import LoggerProvider

logger = LoggerProvider.get_logger()


def get_list_statements(df: DataFrame, type_statement_col_name: str) -> List[str]:
    return df.filter(col(type_statement_col_name) != '') \
        .select(type_statement_col_name) \
        .rdd.flatMap(lambda x: x) \
        .collect()


def get_changes_origin(df_dest: DataFrame, df_origin: DataFrame) -> DataFrame:
    """
    Get changes from the original df by comparing it with another df.
    Args:
        df_dest (DataFrame): The df to compare with. DF from Unity Catalog.
            e.g.: df_uc_fields
        df_origin (DataFrame): The origin DataFrame to compare against. DF from Excel.
            e.g.: df_origin_fields
    Returns:
        It returns a df containing the rows that are present in
        `df_origin` but not in `df_uc`.
            +--------+--------------+-----------------+----------+--------+
        ... data_type|sensitive_info|business_relevant|is_derived|category  ...
        ... +--------+--------------+-----------------+----------+--------+ ...
        ...    string|          NULL|             NULL|      NULL|    NULL  ...
    """
    return df_origin.subtract(df_dest)


def get_tag_names(df: DataFrame) -> List:
    """
    Extracts column names from a DataFrame
    that start with 'tag_' and returns them as a list.

    Parameters:
        e.g.: df_uc_tables

    Returns:
    List: A list containing column names that start with 'tag_'.
        e.g.: ['tag_table_data_steward', 'tag_table_frequency_ingestion']
    """
    return [c for c in df.columns if c.startswith('tag_')]


def count_values_dict(dict_map_sources: Dict[str, List]) -> dict[str, int]:
    """
    Count the number of elements in each
    value list of the dictionary and log the results.

    Args:
        dict_map_sources (Dict[str, list]): A dictionary mapping keys to lists.
            e.g.: {'a': [1, 2, 3], 'b': [4, 5], 'c': []})
    Return:
        e.g.: {
            a: 3
            b: 2
            c: 0
        }
    """
    count_dict = {key: len(value_list)
                  for key, value_list in dict_map_sources.items()}

    for key, count in count_dict.items():
        logger.info(f'{key}: {count}')

    return count_dict


# def get_obj_type(spark: SparkSession, path_uc: str) -> str:
#     """
#     Get the type of an object (table or view) from Unity Catalog
#
#     Parameters:
#     - spark (SparkSession): The Spark session.
#     - path_uc (str): The path in UC.
#         e.g.: dev_silver.shipsure_api.crewrank
#
#     Returns:
#         The type of the object, which can be 'VIEW' or 'TABLE'.
#         If the object does not exist or an error occurs, it returns 'NULL'.
#     """
#     try:
#         desc_table = spark \
#             .sql(f"describe table extended {path_uc}") \
#             .filter(col('col_name') == 'Type') \
#             .collect()[0]
#     except Exception as e:
#         logger.error(f'The table/view {path_uc} not found. '
#                      f'Check if the origin (excel file) is updated.\n{e}')
#         return 'null'
#     else:
#         if desc_table.data_type == 'VIEW':
#             return 'VIEW'
#         else:
#             # in Unity catalog is called: EXTERNAL
#             return 'TABLE'


def get_common_cols_between_df(df_one: DataFrame, df_two: DataFrame) -> List:
    """
    Get common columns between two Spark DataFrames.

    Args:
        df_one (pyspark.sql.DataFrame): First DataFrame.
        df_two (pyspark.sql.DataFrame): Second DataFrame.

    Returns:
        list: List of common columns between the two DataFrames.
    """
    cols_one = set(df_one.columns)
    cols_two = set(df_two.columns)
    return list(cols_one.intersection(cols_two))


def generate_list_cols_to_orderby(
    list_cols_groupby: List,
    list_fields_tags_names_cols: List,
) -> List:
    """
    Generates a list of columns to be used for ordering in a SQL query.
    This function takes two lists of column names and combines them to create a
    single list for ordering. It concatenates the list_cols_groupby and
    list_fields_tags_names_cols lists.

    Args:
        list_cols_groupby (List[str]): A list of columns used for grouping.
        list_fields_tags_names_cols (List[str]): A list of columns for ordering.

    Returns:
        Example:
            list_cols_groupby = ['category', 'year']
            list_fields_tags_names_cols = ['sales', 'product_name']
            generate_list_cols_to_orderby(list_cols_groupby, list_fields_tags_names_cols)
            ['category', 'year', 'sales', 'product_name']
    """
    list_cols = []
    list_cols.extend(list_cols_groupby)
    list_cols.extend(list_fields_tags_names_cols)
    return list_cols


def get_list_tables(df: DataFrame, layer: str, uc_db_name: str) -> List:
    """
    Get a list of tables from a df. Used to at dataCatalog.xlx tab: Tables
    """
    df = df.filter(col("layer") == layer) \
           .filter(col("source_raw") == uc_db_name) \
           .select(col('table'))
    return [row['table'] for row in df.collect()]


def get_remote_spark() -> SparkSession:
    """
    Create a SparkSession for a remote Databricks cluster.

    Returns:
        SparkSession: The Spark session connected to the remote Databricks cluster.
    """
    try:
        # Retrieve environment variables
        HOST_DATABRICKS = os.getenv('HOST_DATABRICKS')
        TOKEN_DATABRICKS = os.getenv('TOKEN_DATABRICKS')
        CLUSTER_ID_DATABRICKS = os.getenv('CLUSTER_ID_DATABRICKS')

        return SparkSession \
            .builder \
            .remote(f'{HOST_DATABRICKS};'
                    f'token={TOKEN_DATABRICKS};'
                    f'use_ssl=true;'
                    f'x-databricks-cluster-id={CLUSTER_ID_DATABRICKS}') \
            .appName('spark_remote_connect') \
            .getOrCreate()
    except Exception:
        return SparkSession.builder.getOrCreate()


# ---------------------------------
# dbutils for handle remote spark
# ---------------------------------
class DbUtils:
    """
    Class to encapsulate the dbutils
    """
    def __init__(self, spark_session: SparkSession, dbutils):
        self.spark = spark_session
        try:
            # self.dbutils = dbutils.widgets

            import IPython
            ipython = IPython.get_ipython()
            self.dbutils = ipython.user_ns["dbutils"]
        except AttributeError as e:
            print(e)
            self.dbutils = dbutils

    def removeAll(self):
        try:
            dbutils.removeAll()
        except Exception:
            logger.warning('remote mode')

    def text(self, key_name: str, value: str):
        try:
            self.dbutils.text(key_name, value)
        except Exception:
            logger.warning('remote mode')
            path_tmp_file = ''.join('/tmp' + f'/{key_name}.txt')

            if os.path.exists(path_tmp_file):
                os.remove(path_tmp_file)

            with open(path_tmp_file, 'w') as file:
                file.write(value)

    def get(self, key_name: str):
        try:
            return self.dbutils.get(key_name)
        except Exception:
            logger.warning('remote mode')
            path_tmp_file = ''.join('/tmp' + f'/{key_name}.txt')
            with open(path_tmp_file, 'r') as file:
                return file.read()
