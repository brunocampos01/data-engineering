from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

from data_catalog.src.base_data_catalog import BaseDataCatalog


class AddTagsCols(BaseDataCatalog):
    """
    Add new tags found in Excel. These tags will add as empty values in df_uc.
    This code is necessary to analyse what is new in excel.
    """
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def _remove_prefix_tag_cols(df: DataFrame, table_name: str) -> DataFrame:
        for col_name in df.columns:
            if col_name.startswith(f'tag_{table_name}_'):
                df = df.withColumnRenamed(col_name,
                                          col_name.replace(f'tag_{table_name}_', ''))
        return df

    @staticmethod
    def add_new_tag(df: DataFrame, list_new_tags_tables: List[str]) -> DataFrame:
        """
        Create a empty column in df from Azure ADLS.
        This column will use in transformation step.
        The new tag means that a user add a new column in Excel (origin).
        If this column starts without value, the spark will understand like a void datatype,
        so it is necessary to cast to string.

        Args:
            df: df from Azure ADLS
                e.g.: df_uc
            list_new_tags_tables:
                e.g.: ['data_certified']
        Return:
                +------+ ... +-------------------------+
                | layer| ... |tag_source_data_certified|
                +------+ ... +-------------------------+
                |bronze| ... |                     NULL|
                |bronze| ... |                     NULL|
        """
        for tag_name in list_new_tags_tables:
            df = df.withColumn(tag_name, lit(None).cast(StringType()))
        return df

    def execute(
        self,
        df_uc: DataFrame,
        df_origin: DataFrame,
        table_name: str,
    ) -> DataFrame:
        singular_table_name = table_name[:-1]
        df_uc = self._remove_prefix_tag_cols(df=df_uc, table_name=singular_table_name)
        list_new_tags_tables = [c for c in df_origin.columns if c not in df_uc.columns]

        if len(list_new_tags_tables) > 0:
            self.logger.info(f'New tags to add in {table_name}: {list_new_tags_tables}')
            df_uc = self.add_new_tag(df=df_uc, list_new_tags_tables=list_new_tags_tables)

        return df_uc
