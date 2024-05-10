from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
import os


class CatalogManager:
    """
    Manages table in catalog, like Hive or Unity Catalog
    """
    @staticmethod
    def add_unmanaged_table(spark: SparkSession, table_name: str, delta_location: str) -> None:
        """Create a new unmanaged table in catalog if it not exists

        Args:
            spark (SparkSession): Spark Session
            table_name (str): Table name to be created
            delta_location (str): Path where the table is located
        """
        stmt = f"CREATE EXTERNAL TABLE if not exists {table_name} USING DELTA LOCATION '{delta_location}'"
        spark.sql(stmt)

    @staticmethod
    def get_location_for_delta_table(delta_table: DeltaTable) -> str:
        """
        Get the storage location of a table from its properties.
        Args:
            delta_table (DeltaTable): todo

        Returns:
            (str) The location path in the storage
        """
        location = delta_table.detail().head()['location']
        return location

    @staticmethod
    def get_location_for_delta_table_name(spark: SparkSession, table_name: str) -> str:
        """
        Get the storage location of a table from its properties.
        Args:
            spark (SparkSession):
            table_name (str): The existing table name

        Returns:
            (str). The location path in the storage
        """
        delta_table = DeltaTable.forName(spark, table_name)
        return CatalogManager.get_location_for_delta_table(delta_table)

    @staticmethod
    def unity_catalog_is_enabled(spark: SparkSession) -> bool:
        """
        Returns:
            (bool). Indicating if unity catalog is available for the cluster.
        """
        is_test_app = spark.conf.get('spark.app.name','') == 'test-application'
        if is_test_app:
            return False
        
        uc_enabled = spark.conf.get('spark.databricks.unityCatalog.enabled', 'false')
        return uc_enabled == 'true'

    @staticmethod
    def bronze_catalog_name(spark: SparkSession) -> str:
        """
        Get the bronze catalog name for the current environment.
        Args:
            spark (SparkSession):

        Returns:
            (str). The name of the bronze catalog
        """
        if CatalogManager.unity_catalog_is_enabled(spark):
            env = os.getenv('Environment', '').lower()
            if env in ['dev', 'test']:
                return f'{env}_bronze'
            return 'bronze'
        return ''

    @staticmethod
    def silver_catalog_name(spark: SparkSession) -> str:
        """
        Get the silver catalog name for the current environment.
        Args:
            spark (SparkSession):

        Returns:
            (str). The name of the silver catalog
        """
        if CatalogManager.unity_catalog_is_enabled(spark):
            env = os.getenv('Environment', '').lower()
            if env in ['dev', 'test']:
                return f'{env}_silver'
            return 'silver'
        return ''

    @staticmethod
    def monitoring_catalog_name(spark: SparkSession) -> str:        
        """
        Get the monitoring catalog name for the current environment.
        Args:
            spark (SparkSession):

        Returns:
            (str). The name of the monitoring catalog
        """
        if CatalogManager.unity_catalog_is_enabled(spark):
            env = os.getenv('Environment', '').lower()
            if env in ['dev', 'test']:
                return f'{env}_monitoring'
            return 'monitoring'
        return ''

    @staticmethod
    def table_exists(spark: SparkSession, table_name: str) -> bool:
        """
        Verify if a table exists in catalog
        Args:
            spark (SparkSession):
            table_name (str):

        Returns:
            bool
        """
        try:
            spark.sql(f"DESCRIBE {table_name}")
        except AnalysisException as e:
            # table not found
            return False
        return True
