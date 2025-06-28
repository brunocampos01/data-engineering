from typing import (
    List,
    Dict,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.transformer.add_dw_cols import AddDWCols
from data_catalog.src.writer.delta_table_writer import DeltaTableWriter


class GoldDimTransformer(AddDWCols):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def execute(self, df_origin: DataFrame, table_name: str) -> DataFrame:
        if table_name == 'sources_tables':
            df_dim = self.add_sk_col(df_origin, 'table')
            df_dim = self.apply_default_values_in_dim(df_dim, 'table')
            return df_dim

        try:
            list_cols = getattr(self, f'get_list_cols_dim_{table_name}s')()
        except AttributeError:
            list_cols = df_origin.columns

        df_dim = df_origin.select(*list_cols).distinct()
        df_dim = self.add_sk_col(df_dim, table_name)
        df_dim = self.apply_default_values_in_dim(df_dim, table_name)
        return df_dim

    def execute_creation_relationalship(
        self,         
        df_left: DataFrame, 
        id_left: str, 
        df_right: DataFrame, 
        id_right: str,
    ) -> DataFrame:
        """
        This function create relationalship between dimesions 
        and return a dimesion already to connection with fact.

        Follow snowflake data warehousing schema.
        """
        df_right = df_right \
            .drop(*['source']) \
            .withColumnRenamed('source_exploded', 'source') \
            .withColumnRenamed('sources_table_id', 'table_id') \
            .select(*df_left.columns)

        # report ---
        df_new_rows = df_right.select('table_id').subtract(df_left.select('table_id'))
        self.logger.info(f'Showing ID of the new rows:')
        df_new_rows.display()
        # report ---

        df = df_left.union(df_right).dropDuplicates(['table_id'])
        df = self.add_sk_col(df, 'table')
        df = self.apply_default_values_in_dim(df, 'table')

        return df_left

    def execute_by_tags(
        self,
        df: DataFrame,
        list_tags_names: List,
        writer_delta_table: DeltaTableWriter,
        list_tags_not_dim: List = [],
    ) -> Dict:
        """
        Return:
            A dict that will use to generate the fact table
                e.g.:
                {
                    'tag_field_business_relevant': DataFrame[tag_field_rr: string, ...],
                    'tag_field_category': DataFrame[tag_field_category: string, ...
                }
        """
        dict_df_dim_tags = {}
        list_tags_names = [tag_name for tag_name in list_tags_names
                           if tag_name not in list_tags_not_dim]
        for tag in list_tags_names:
            df_dim = self.execute(df_origin=df.select(tag), table_name=tag)
            dict_df_dim_tags[tag] = df_dim
            writer_delta_table.execute(
                df=df_dim,
                data_format='delta',
                table_name=f'dim_{tag}',
            )

            self.logger.info(f'Total {tag} = {df_dim.count()}')
            df_dim.limit(10).show(truncate=False)

        return dict_df_dim_tags
