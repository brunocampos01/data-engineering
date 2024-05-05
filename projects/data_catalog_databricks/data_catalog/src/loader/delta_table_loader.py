from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog
from library.datalake_storage import DataLakeStorage


class DeltaTableLoader(BaseDataCatalog):
    def __init__(
        self,
        spark: SparkSession,
        layer_name: str,
        container_name: str,
        folder_name: str,
    ):
        super().__init__(spark, layer_name)
        self.container_name = container_name
        self.folder = folder_name
        self.delta_path = DataLakeStorage \
            .get_storage_url(self.container_name, self.folder)
        self.spark.conf.set("spark.sql.broadcastTimeout", "600")

    # TODO: change to uc_loader
    def load_data_from_query(self, query_file_path: str) -> DataFrame:
        """
        Load data from a SQL query and return it as a DataFrame.

        Args:
            query_file_path (str): Path to the SQL query file.
                e.g.: /Repos/<user>@cslships.com/AG-DataTeam-Databricks/data_catalog/src/loader/queries/

        Returns:
            pyspark DataFrame
            +--------------------+---------------+--------------------+
            |              source|Type Extraction|         description|
            +--------------------+---------------+--------------------+
            |           bloomberg|       database|                    |
            |           bloomberg|       database|                None|
        """
        environment = self.env
        if environment == 'prod':
            environment = ''
        else:
            environment = f'{environment}_'

        try:
            query = open(query_file_path).read()
            query = query.replace('<env>', environment)
        except OSError:
            raise OSError(f"File not found! Check if is correct: {query_file_path}")
        except Exception as e:
            Exception(f"Error loading data from {query_file_path}:\n{str(e)}")
        else:
            df = self.spark.sql(query)
            return df.cache()

    def execute(self, table_name: str) -> DataFrame:
        """
        This function loads data from a Delta Lake table into a DataFrame.

        Args:
            table_name (str): The name of the Delta Lake table to load.

        Returns:
            DataFrame: A DataFrame containing the data
        """
        return self.spark.read \
            .format("delta") \
            .option("mergeSchema", "true") \
            .load(f'{self.delta_path}/{table_name}/')
