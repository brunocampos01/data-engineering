from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    first,
)

from data_catalog.src.transformer.base_transformer import BaseTransformer
from data_catalog.src.utils import get_tag_names


class UCTransformer(BaseTransformer):
    """
    UCTransformer class is used to format data from Unity Catalog
    and generate df that has the same schema from Excel.
    """
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def _pivot_df_tags(df: DataFrame, list_to_group_by: List[str]) -> DataFrame:
        """
        Pivot DataFrame on tag_name.

        Args:
            df (DataFrame): Input DataFrame. Example:
                +----------+-------+-------------------+---------+
                |     layer| source|           tag_name|tag_value|
                +----------+-------+-------------------+---------+
                |    silver|   xref|csl_internal_system|        _|
                |    silver|   xref|      active_system|        _|
            list_to_group_by (List[str]): List of columns to group by.
                Example: ['layer', 'source']

        Returns:
            DataFrame: Pivoted DataFrame. Example:
            +----------+--------+-------------+-------------------+
            |     layer|  source|active_system|csl_internal_system|
            +----------+--------+-------------+-------------------+
            |    bronze|    xref|          yes|                 No|
            |      gold|    xref|            _|                  _|
        """
        return df \
            .groupBy(list_to_group_by) \
            .pivot('tag_name') \
            .agg(first(col("tag_value")))

    def __process_sources(self, df_info_schema: DataFrame, df_tags: DataFrame) -> DataFrame:
        """
        Process sources DataFrame.

        Args:
            df_info_schema (DataFrame): Information schema DataFrame.
            df_tags (DataFrame): Tags DataFrame.

        Returns:
            DataFrame: Processed sources DataFrame.
        """
        list_to_group_by = ['layer', 'source']
        df_tags = self._pivot_df_tags(df_tags, list_to_group_by)
        list_tags = get_tag_names(df_tags)
        self.logger.info(f'Tags in UC: {list_tags}')

        return df_info_schema.join(df_tags, how='left', on=list_to_group_by)

    def __process_tables(self, df_info_schema: DataFrame, df_tags: DataFrame) -> DataFrame:
        """
        Process tables DataFrame.

        Args:
            df_info_schema (DataFrame): Information schema DataFrame.
            df_tags (DataFrame): Tags DataFrame.

        Returns:
            DataFrame: Processed tables DataFrame.
        """
        list_to_group_by = ['layer', 'source', 'table']
        df_tags = self._pivot_df_tags(df_tags, list_to_group_by)
        list_tags = get_tag_names(df_tags)
        self.logger.info(f'Tags in UC: {list_tags}')

        return df_info_schema.join(df_tags, how='left', on=list_to_group_by)

    def __process_fields(self, df_info_schema: DataFrame, df_tags: DataFrame) -> DataFrame:
        """
        Process fields DataFrame.

        Args:
            df_info_schema (DataFrame): Information schema DataFrame.
            df_tags (DataFrame): Tags DataFrame.

        Returns:
            DataFrame: Processed fields DataFrame.
        """
        list_to_group_by = ['layer', 'source', 'table', 'field']
        df_tags = self._pivot_df_tags(df_tags, list_to_group_by)
        list_tags = get_tag_names(df_tags)
        self.logger.info(f'Tags in UC: {list_tags}')

        return df_info_schema.join(df_tags, how='left', on=list_to_group_by)

    def execute(
        self,
        table_name: str,
        file_path_sql_info_schema: str,
        file_path_sql_tags: str,
    ) -> DataFrame:
        """
        Execute the transformation process. In this case, generate a formatted df
        with the same schema from Excel.

       Args:
           table_name (str): Name of the table to process. Example: 'sources'
           file_path_sql_info_schema (str): Path to the SQL file.
                Example: .../DataTeam-Databricks/data_catalog/jobs/../src/loader/queries/get_tables_info_schema.sql
           file_path_sql_tags (str): Path to the SQL file for the tags.
                Example: .../DataTeam-Databricks/data_catalog/jobs/../src/loader/queries/get_tables_tags.sql

       Returns:
           DataFrame: Return a df with the same origin excel structure
                Example:
                +------+---------+----------+...+-----------------------+
                | layer|   source| layer_raw|...|tag_source_data_steward|
                +------+---------+----------+...+-----------------------+
                |bronze|   oracle|dev_bronze|...|                 qwerty|
                |bronze|   oracle|dev_bronze|...|                   xpto|
       Notes:
           In the first plan, it was executed df.filter(col('source').isin(list_sources))
           to keep exactly the same sources thanExcel file.
           After that in the last step it was getting the new values in UC.
           This plan was changed, and now we will get all sources since bronze layer
        """
        df_info_schema = self._load_delta_table_from_query(file_path_sql_info_schema)
        df_info_schema = self.add_col_layer(df_info_schema)
        df_info_schema = self.add_col_source(df_info_schema)

        df_tags = self._load_delta_table_from_query(file_path_sql_tags)
        df_tags = self.add_col_layer(df_tags)
        df_tags = self.add_col_source(df_tags)
        
        if table_name == 'sources':
            return self.__process_sources(df_info_schema, df_tags)

        elif table_name == 'tables':
            return self.__process_tables(df_info_schema, df_tags)

        elif table_name == 'fields':
            return self.__process_fields(df_info_schema, df_tags)

        else:
            raise ValueError(f"Unsupported table_name: {table_name}")
