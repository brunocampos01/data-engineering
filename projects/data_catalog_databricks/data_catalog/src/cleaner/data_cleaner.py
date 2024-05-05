from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    when,
)
from pyspark.sql.types import (
    StringType,
    TimestampType,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog


class DataCleaner(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def _get_list_cols_to_standard(table_name: str, is_uc: bool) -> List[str]:
        """
        Get a list of column names to be standardized based on the table name.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            list: A list of column names.
        """
        if table_name == 'layers':
            return ['layer']
        elif table_name == 'sources':
            return ['source']
        elif table_name == 'tables':
            if is_uc:
                return ['layer', 'source_raw', 'source', 'table', 'tag_table_source_system']
            else:
                return ['layer', 'source_raw', 'source', 'table', 'source_system']
        elif table_name == 'fields':
            return ['layer', 'source_raw', 'source', 'table']
        elif table_name == 'data_stewards':
            return ['data_steward']

    @staticmethod
    def remove_unnamed_cols(df: DataFrame) -> DataFrame:
        """
        In origin, like Excel, the user can add some data in a col just for yourself.
        This action will generate a new column in Excel starts with the name: unnamed:42
        So, the columns that starts with unnamed will remove.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The DataFrame with 'unnamed' columns removed.
        """
        return df.select([c for c in df.columns
                          if not c.startswith('unnamed')])

    @staticmethod
    def standardize_cols_names(df: DataFrame) -> DataFrame:
        """
        Standardizes column names in a DataFrame by converting them to lowercase,
        stripping whitespace, and replacing spaces with underscores.

        Args:
            df (DataFrame): The input DataFrame.

        Return:
            +-----------+-----+
            |full_name  |  age|
            +-----------+-----+
            |Alice Smith|  25 |
            +-----------+-----+
        """
        new_cols_name = [col(c).alias(c.lower().strip().replace(' ', '_'))
                         for c in df.columns]
        return df.select(*new_cols_name)

    @staticmethod
    def standardize_col_content(df: DataFrame, list_cols: List) -> DataFrame:
        """
        Standardize the content of specified columns in a PySpark DataFrame.
        This func trims leading & trailing spaces & converts the content of specified
        columns to lowercase.

        Returns:
            e.g.:
                data = [("  Hello  ", "  WoRLd  "), ("  SPARK  ", "  PytHoN  ")]
                +------+------+
                |  col1|  col2|
                +------+------+
                |hello |world |
                |spark |python|
                +-------------+
        """
        for col_name in list_cols:
            df = df.withColumn(col_name, trim(lower(col(col_name)))) \
                   .withColumn(col_name, col(col_name).cast(StringType()))
        return df

    @staticmethod
    def standardize_col_datatype(df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            if col_name.endswith('created_in_uc_at') or col_name.endswith('updated_at'):
                df = df.withColumn(col_name, col(col_name).cast(TimestampType()))
            else:
                df = df.withColumn(col_name, col(col_name).cast(StringType()))

        return df

    @staticmethod
    def standardize_empty_value(df: DataFrame) -> DataFrame:
        """
         This function iterates over all columns in a DataFrame
         and replaces null or empty string values with None.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
             DataFrame:
        """
        for c in df.columns:
            df = df.withColumn(c, when(col(c).isNull(), None).otherwise(col(c)))

        return df

    def execute(self, df: DataFrame, table_name: str, is_uc: bool = None) -> DataFrame:
        """
        Process a DataFrame and return it standardized.

        This function standardizes the column names and content of a DataFrame,
        removes unnamed columns, and returns the processed DataFrame.

        Args:
            df (DataFrame): The DataFrame to be processed.
            table_name (str): The name of the table. Example: 'sources'

        Returns:
            DataFrame: The processed DataFrame.
        """
        list_cols_to_standard = self._get_list_cols_to_standard(table_name, is_uc)

        df = self.standardize_cols_names(df)
        df = self.standardize_col_content(df, list_cols_to_standard)
        df = self.standardize_col_datatype(df)
        df = self.remove_unnamed_cols(df)
        df = self.standardize_empty_value(df)

        return df
