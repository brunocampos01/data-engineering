import os

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col

from library.logger_provider import LoggerProvider

logger = LoggerProvider.get_logger()


def get_list_statements(df: DataFrame, type_statement_col_name: str) -> list[str]:
    return (
        df.filter(col(type_statement_col_name) != '')
        .select(type_statement_col_name)
        .rdd.flatMap(lambda x: x)
        .collect()
    )


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


def get_tag_names(df: DataFrame) -> list[str]:
    """
    Extracts column names from a DataFrame
    that start with 'tag_' and returns them as a list.

    Parameters:
        e.g.: df_uc_tables

    Returns:
    list[str]: A list containing column names that start with 'tag_'.
        e.g.: ['tag_table_data_steward', 'tag_table_frequency_ingestion']
    """
    return [c for c in df.columns if c.startswith('tag_')]


def count_values_dict(dict_map_sources: dict[str, list]) -> dict[str, int]:
    """
    Count the number of elements in each
    value list of the dictionary and log the results.

    Args:
        dict_map_sources (dict[str, list]): A dictionary mapping keys to lists.
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


def get_common_cols_between_df(df_one: DataFrame, df_two: DataFrame) -> list[str]:
    """
    Get common columns between two Spark DataFrames.

    Args:
        df_one (pyspark.sql.DataFrame): First DataFrame.
        df_two (pyspark.sql.DataFrame): Second DataFrame.

    Returns:
        list[str]: Common columns between the two DataFrames.
    """
    return list(set(df_one.columns) & set(df_two.columns))


def generate_list_cols_to_orderby(
    list_cols_groupby: list[str],
    list_fields_tags_names_cols: list[str],
) -> list[str]:
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
    return list_cols_groupby + list_fields_tags_names_cols


def get_list_tables(df: DataFrame, layer: str, uc_db_name: str) -> list[str]:
    """
    Get a list of tables from a df. Used to at dataCatalog.xlx tab: Tables
    """
    rows = (
        df.filter(col("layer") == layer)
        .filter(col("source_raw") == uc_db_name)
        .select(col('table'))
        .collect()
    )
    return [row['table'] for row in rows]


def get_remote_spark() -> SparkSession:
    """
    Create a SparkSession for a remote Databricks cluster.

    Returns:
        SparkSession: The Spark session connected to the remote Databricks cluster.
    """
    try:
        host = os.getenv('HOST_DATABRICKS')
        token = os.getenv('TOKEN_DATABRICKS')
        cluster_id = os.getenv('CLUSTER_ID_DATABRICKS')

        return (
            SparkSession.builder
            .remote(f'sc://{host}:443/;token={token};use_ssl=true;x-databricks-cluster-id={cluster_id}')
            .appName('spark_remote_connect')
            .getOrCreate()
        )
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
            self.dbutils.removeAll()
        except Exception:
            logger.warning('remote mode')

    def text(self, key_name: str, value: str):
        try:
            self.dbutils.text(key_name, value)
        except Exception:
            logger.warning('remote mode')
            path_tmp_file = f'/tmp/{key_name}.txt'

            if os.path.exists(path_tmp_file):
                os.remove(path_tmp_file)

            with open(path_tmp_file, 'w') as file:
                file.write(value)

    def get(self, key_name: str):
        try:
            return self.dbutils.get(key_name)
        except Exception:
            logger.warning('remote mode')
            path_tmp_file = f'/tmp/{key_name}.txt'
            with open(path_tmp_file, 'r') as file:
                return file.read()
