from pyspark.sql import SparkSession

from data_catalog.src.catalog_creator.base_creator import BaseCreator


class CatalogHelper(BaseCreator):
  def __init__(self, spark: SparkSession, layer_name: str, owner: str):
      super().__init__(spark, layer_name, owner)

  def drop(self) -> None:
    return self.spark.sql(f"DROP CATALOG IF EXISTS `{self.env}_data_catalog` CASCADE")

  def create(self, catalog_name: str) -> None:
    return self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")

  def alter_owner(self, catalog_name: str) -> None:
    return self.spark.sql(f"ALTER CATALOG `{catalog_name}` OWNER TO `{self.owner}`")

  def alter_tags(self, catalog_name: str) -> None:
    return self.spark.sql(f"ALTER CATALOG `{catalog_name}` SET TAGS ('data_governance')")

  def grant_privileges(self) -> None:
    if self.env == 'dev':
      self.spark.sql(f"GRANT USE CATALOG ON CATALOG `{self.env}_data_catalog` TO `{self.env}_datalayer_support`")     
      self.spark.sql(f"GRANT USE CATALOG ON CATALOG `{self.env}_data_catalog` TO `{self.env}_datalayer_hrsupport`")
      self.spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG `{self.env}_data_catalog` TO `administrators`")
      self.spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG `{self.env}_data_catalog` TO `development`")

    elif self.env == 'test':
      self.spark.sql(f"GRANT USE CATALOG ON CATALOG `{self.env}_data_catalog` TO `testers`")     

    elif self.env == 'prod':
      self.spark.sql(f"GRANT USE CATALOG ON CATALOG `data_catalog` TO `{self.env}_datalayer_support`")     

  def drop_schema(self, catalog_name: str) -> None:
    return self.spark.sql(f"DROP SCHEMA IF EXISTS `{catalog_name}`.`default`")

  def comment_on_catalog(self, catalog_name: str) -> None:
    return self.spark.sql(f"""COMMENT ON CATALOG `{catalog_name}` IS 'Catalog used by data governance. It enables quality and documentation, ensuring data consistency and reliability for CSL. The databases follow the Medallion architecture.'""") 
