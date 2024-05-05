from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog
from data_catalog.src.transformer.uc_transformer import UCTransformer


class UCFormattedLoader(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def execute(
        self,
        file_path_sql_info_schema: str,
        file_path_sql_tags: str,
        table_name: str,
    ) -> DataFrame:
        """
        Load the data from Unity Catalog and format the schema to
        following the same schema from origin (Excel).
        """
        uc_transformer = UCTransformer(self.spark, self.layer_name)

        # load from UC based on the queries
        df = uc_transformer.execute(
            table_name=table_name,
            file_path_sql_info_schema=file_path_sql_info_schema,
            file_path_sql_tags=file_path_sql_tags,
        )

        return df
