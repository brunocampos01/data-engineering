from pyspark.sql import SparkSession

from data_catalog.src.catalog_creator.base_creator import BaseCreator


class DBHelper(BaseCreator):
    def __init__(self, spark: SparkSession, layer_name: str, owner: str):
        super().__init__(spark, layer_name, owner)

    def use_catalog(self) -> None:
        if self.env == 'prod':
            return self.spark.sql("USE CATALOG `data_catalog`")
        else:
            return self.spark.sql(f"USE CATALOG `{self.env}_data_catalog`")

    def create(self, db: str) -> None:
        return self.spark.sql(f"CREATE DATABASE IF NOT EXISTS `{db}`")

    def alter_owner(self, db: str) -> None:
        return self.spark.sql(f"ALTER DATABASE `{db}` OWNER TO `{self.owner}`")

    def grant_privileges(self, db: str) -> None:
        if self.env == 'dev':
            self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{db}` TO `{self.env}_datalayer_support`")     
            self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{db}` TO `{self.env}_datalayer_hrsupport`")     

        elif self.env == 'test':
            self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{db}` TO `testers`")     

        elif self.env == 'prod':
            self.spark.sql(f"GRANT USE SCHEMA ON SCHEMA `{db}` TO `{self.env}_datalayer_support`")     

    def alter_tags(self, db: str, tags: str) -> None:
        return self.spark.sql(f'ALTER DATABASE `{db}` SET TAGS ("{tags}")')

    def comment_on_database(self, db: str) -> None:
        return self.spark.sql(f"COMMENT ON DATABASE `{db}` IS 'Data Governance: metadata from {self.env}_{db} layer'")
