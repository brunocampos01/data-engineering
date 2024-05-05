from typing import (
    Dict,
    List,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lower,
)

from data_catalog.src.transformer.base_transformer import BaseTransformer


class LandingTransformer(BaseTransformer):
    """A transformer for processing landing data."""
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def _get_rows_exists_in_origin_and_uc(self, df_origin: DataFrame, df_uc: DataFrame, id_col: str) -> DataFrame:
        # Rename the conflicting columns in df_uc
        df_uc_renamed = df_uc.select([col(c).alias(f"uc_{c}" if c != id_col else c) 
                                      for c in df_uc.columns])
        return df_origin \
            .join(df_uc_renamed, on=id_col, how='left') \
            .select(*df_origin.columns, 'uc_obj_type') \
            .drop('obj_type') \
            .withColumnRenamed('uc_obj_type', 'obj_type')

    def _filter_layer(self, df: DataFrame) -> DataFrame:
        return df.filter(col("layer") == self.layer_name)

    def __process_sources(self, df_origin: DataFrame, list_origin_tags_names: List[str]) -> DataFrame:
        """
        Process source data.

        Args:
            df_origin (DataFrame): The original DataFrame.
            list_origin_tags_names (List[str]): List of origin tags names.

        Returns:
            DataFrame: Processed DataFrame.
        """
        df_origin = df_origin.withColumn('source', lower(col('source')))
        
        return self.add_col_sql_statement(
            df=df_origin,
            list_origin_tags_names=list_origin_tags_names,
            table_name='sources',
        )

    def __process_tables(
        self, df_origin: DataFrame,  df_uc: DataFrame, list_origin_tags_names: List[str]
    ) -> DataFrame:
        """
        Process table data.

        Args:
            df_origin (DataFrame): The original DataFrame.
            list_origin_tags_names (List[str]): List of origin tags names.

        Returns:
            DataFrame: Processed DataFrame.
        """
        self.logger.info(f'{self.layer_name} before cleaned: {df_origin.count()}')
        df_origin = df_origin.withColumn('table', lower(col('table')))

        df_origin = self.add_id_cols(df_origin, 'tables', place_df='origin')
        df_origin = self.add_col_id_with_backticks(df_origin)
        df_uc = self.add_id_cols(df_uc, 'tables', place_df='UC')
        df_uc = self.add_col_id_with_backticks(df_uc)

        df_origin = self._get_rows_exists_in_origin_and_uc(
            df_origin=df_origin,
            df_uc=df_uc,
            id_col='table_id',
        )
        df_origin = self._filter_layer(df_origin)
        self.logger.info(f'{self.layer_name} after cleaned: {df_origin.count()}')

        df_origin = self.add_col_sql_statement(
            df=df_origin,
            list_origin_tags_names=list_origin_tags_names,
            table_name='tables',
        )

        return df_origin

    def __process_fields(
        self, df_origin: DataFrame, df_uc: DataFrame, list_origin_tags_names: List[str]
    ) -> DataFrame:
        """
        Process field data.

        Args:
            df_origin (DataFrame): The original DataFrame.
            list_origin_tags_names (List[str]): List of origin tags names.

        Returns:
            DataFrame: Processed DataFrame.
        """
        self.logger.info(f'{self.layer_name} before cleaned: {df_origin.count()}')
        df_origin = self.add_id_cols(df_origin, 'fields', place_df='origin')
        df_origin = self.add_col_id_with_backticks(df_origin)
        df_uc = self.add_id_cols(df_uc, 'fields', place_df='UC')
        df_uc = self.add_col_id_with_backticks(df_uc)
        df_origin = self._get_rows_exists_in_origin_and_uc(
            df_origin=df_origin,
            df_uc=df_uc,
            id_col='field_id',
        )
        df_origin = self._filter_layer(df_origin)
        self.logger.info(f'{self.layer_name} after cleaned: {df_origin.count()}')

        return self.add_col_sql_statement(
            df=df_origin,
            list_origin_tags_names=list_origin_tags_names,
            table_name='fields',
        )

    def execute(
        self,
        df_origin: DataFrame,
        table_name: str,
        dict_map_sources_tables: Dict,
        list_origin_tags_names: List[str],
        df_uc: DataFrame = None,
    ) -> DataFrame:
        """
        Execute the transformation based on the provided table name.

        Args:
            df_origin (DataFrame): The original DataFrame.
            df_uc (DataFrame): The Unity Catalog DataFrame.
            table_name (str): The name of the table to process.
            dict_map_sources_tables (Dict): A dictionary mapping sources to tables.
            list_origin_tags_names (List[str]): List of origin tags names.

        Returns:
            DataFrame: Processed DataFrame.
        """
        if table_name == 'sources':
            return self.__process_sources(df_origin, list_origin_tags_names)

        elif table_name == 'tables':
            return self.__process_tables(
                df_origin=df_origin,
                df_uc=df_uc,
                list_origin_tags_names=list_origin_tags_names,
            )

        elif table_name == 'fields':
            return self.__process_fields(
                df_origin=df_origin,
                df_uc=df_uc,
                list_origin_tags_names=list_origin_tags_names,
            )

        else:
            raise ValueError(f"Unsupported table_name: {table_name}")
