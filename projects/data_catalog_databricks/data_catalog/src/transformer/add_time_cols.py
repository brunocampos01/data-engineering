from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

from data_catalog.src.base_data_catalog import BaseDataCatalog


class AddTimeCols(BaseDataCatalog):
    """
    Generate created_at and last_updated_at columns for Fields table
    """
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def _get_df_table_info_schema(self, list_cols_selected: List) -> DataFrame:
        return self.spark \
            .sql('SELECT * FROM `system`.`information_schema`.`tables`') \
            .select(list_cols_selected)

    def _add_col_last_updated_at(self, df: DataFrame, table_name: str) -> DataFrame:
        self.logger.info('Adding new column: last_updated_at')
        return df \
            .withColumn('last_altered', col(f'{table_name}_created_at').cast(TimestampType())) \
            .withColumnRenamed('last_altered', f'{table_name}_last_updated_at')

    def _add_col_created_at(self, df: DataFrame, table_name: str) -> DataFrame:
        self.logger.info('Adding new column: created_at')
        return df \
            .withColumnRenamed('created', f'{table_name}_created_at') \
            .withColumn(f'{table_name}_created_at', col(f'{table_name}_created_at').cast(TimestampType()))

    def execute(
        self,
        df: DataFrame,
        table_name: str,
        is_first_execution: bool = False
    ) -> DataFrame:
        """
        Add new time columns in Field table.
        - These columns are set up using `system`.`information_schema`.`tables`
          and append in Field df in the first execution
        - After first execution, the column: created_at keep using the same
          value from `system`.`information_schema`.`tables`
        - Used only to Fields tables because there are no how to get this columns
          (created and updated) directly from information_schema.
        """
        list_cols_selected = ['table_catalog', 'table_schema', 'table_name', 'created']
        df_table_info_schema = self._get_df_table_info_schema(list_cols_selected)
        df_table_info_schema = self._add_col_created_at(df_table_info_schema, table_name)

        if is_first_execution:
            df_table_info_schema = self._add_col_last_updated_at(df_table_info_schema, table_name)

        join_condition = [
            df["layer_raw"] == df_table_info_schema["table_catalog"],
            df["source_raw"] == df_table_info_schema["table_schema"],
            df["table"] == df_table_info_schema["table_name"]
        ]
        return df \
            .join(df_table_info_schema, how='inner', on=join_condition) \
            .drop(*['table_catalog', 'table_schema', 'table_name'])
