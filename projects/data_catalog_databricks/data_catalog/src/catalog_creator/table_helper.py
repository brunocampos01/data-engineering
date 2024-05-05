from pyspark.sql import SparkSession

from data_catalog.src.catalog_creator.base_creator import BaseCreator


class TableHelper(BaseCreator):
    def __init__(self, spark: SparkSession, layer_name: str, owner: str, storage_account: str) -> None:
        super().__init__(spark, layer_name, owner)
        self.storage_account = storage_account
    
    def use_catalog(self) -> None:
        if self.env == 'prod':
            return self.spark.sql("USE CATALOG `data_catalog`")
        else:
            return self.spark.sql(f"USE CATALOG `{self.env}_data_catalog`")

    def drop(self, db: str, table_name: str) -> None:
        return self.spark.sql(f"DROP TABLE IF EXISTS `{db}`.`{table_name}`")

    def create(self, db: str, table_name: str) -> None:
        return self.spark.sql(f"""
            CREATE TABLE `{db}`.`{table_name}`
            USING DELTA
            LOCATION 'abfss://{db}@{self.storage_account}.dfs.core.windows.net/data_catalog/{table_name}/'
        """)

    def alter_owner(self, db: str, table_name: str) -> None:
        return self.spark.sql(f"ALTER TABLE `{db}`.`{table_name}` OWNER TO `{self.owner}`")

    def alter_tags(self, db: str, table_name: str):
        return self.spark.sql(f"ALTER TABLE `{db}`.`{table_name}` SET TAGS ('data_governance')")

    def comment_on_table(self, db: str, table_name: str) -> None:
        return self.spark.sql(f"""
        COMMENT ON TABLE `{db}`.`{table_name}` IS 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'""")
