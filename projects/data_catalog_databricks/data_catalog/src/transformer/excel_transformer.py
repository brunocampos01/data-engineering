from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.transformer.base_transformer import BaseTransformer


class ExcelTransformer(BaseTransformer):
    """A transformer for generating Excel file updated."""
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def __join(df_uc, df_landing, join_cond: List, table_name: str):
        return df_uc \
            .join(df_landing, on=join_cond, how='left') \
            .select(df_uc['*'], df_landing[f'{table_name}_description']) \
            .orderBy('layer', 'source_raw', 'source', 'table')

    def __process_data_stewards(self):
        return self.spark.sql("""
            SELECT * FROM `data_stewards` 
            ORDER BY `data_steward`
        """)

    def __process_layers(self):
        return self.spark.sql("""
            SELECT * FROM `layers` 
            ORDER BY `layer`
        """)

    def __process_sources(self):
        df = self.spark.sql("""
            SELECT * FROM `sources` 
            ORDER BY `source`
        """)
        list_to_exclude = ['source_created_in_uc_at', 'source_last_updated_at']
        return self.clean_col_name(
            df=df, 
            table_name='source', 
            list_to_exclude=list_to_exclude,
        )

    def __process_tables(self, place: str = None, df_uc: DataFrame = None, df_landing: DataFrame = None):
        if place != None:
            if place == 'uc':
                df = self.spark.sql("""
                    SELECT * FROM `tables` 
                    ORDER BY `layer`, `source_raw`, `source`, `table`
                """)
                df = df.drop('table_description')

            elif place == 'landing':
                df = self._load_delta_table('tables_only_views', 'landing', 'data_catalog_processed')

            list_to_exclude = ['table_created_in_uc_at', 'table_last_updated_at', 
                               'table_last_data_updated_at', 'table_last_schema_updated_at' ,
                               'tag_table_last_data_steward', 'layer_raw', 
                               'array_tag_table_source_system', 'table_id']
            df = self.clean_col_name(
                df=df, 
                table_name='table', 
                list_to_exclude=list_to_exclude,
            )
            return df
        else:
            join_cond = [
                df_uc['layer'] ==      df_landing['layer'],
                df_uc['source_raw'] == df_landing['source_raw'],
                df_uc['table'] ==      df_landing['table'],
            ]
            return self.__join(df_uc, df_landing, join_cond, 'table')

    def __process_fields(self, place: str = None, df_uc: DataFrame = None, df_landing: DataFrame = None):
        if place != None:
            if place == 'uc':
                df = self.spark.sql("""
                    SELECT * FROM `fields` 
                    ORDER BY `layer`, `source_raw`, `source`, `table`, `field`
                """)
                df = df.drop('field_description')

            elif place == 'landing':
                df = self._load_delta_table('fields_only_views', 'landing', 'data_catalog_processed')
                
            list_to_exclude = ['layer_raw', 'table_id', 'field_id', 
                               'field_created_in_uc_at', 'field_last_updated_at',
                               'array_tag_field_data_usage']
            df = self.clean_col_name(
                df=df, 
                table_name='field', 
                list_to_exclude=list_to_exclude,
            )

            return df

        else:
            join_cond = [
                df_uc['layer'] ==      df_landing['layer'],
                df_uc['source_raw'] == df_landing['source_raw'],
                df_uc['table'] ==      df_landing['table'],
                df_uc['field'] ==      df_landing['field']
            ]
            return self.__join(df_uc, df_landing, join_cond, 'field')

    def execute(
        self,
        table_name: str,
        place: str = None,
        df_uc: DataFrame = None,
        df_landing: DataFrame = None,
    ) -> DataFrame:
        df = self._load_delta_table(table_name, 'silver')
        df.createOrReplaceTempView(table_name)

        if table_name == 'sources':
            return self.__process_sources()

        elif table_name == 'tables':
            return self.__process_tables(place, df_uc, df_landing)

        elif table_name == 'fields':
            return self.__process_fields(place, df_uc, df_landing)
        
        elif table_name == 'layers':
            return self.__process_layers()

        elif table_name == 'data_stewards':
            return self.__process_data_stewards()

        else:
            raise ValueError(f"Unsupported table_name: {table_name}")
