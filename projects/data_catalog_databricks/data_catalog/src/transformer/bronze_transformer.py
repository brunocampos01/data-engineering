from pyspark.errors import AnalysisException
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
    lower,
)

from data_catalog.src.transformer.base_transformer import BaseTransformer


class BronzeTransformer(BaseTransformer):
    """A transformer for processing bronze step ETL."""
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def _append_changes_and_new_rows(self, df_updates, df_historical, table_name):
        """
        Compare using all columns in both df
        """
        df_updates.createOrReplaceTempView("new_data")
        df_historical.createOrReplaceTempView("old_data")

        common_columns = set(df_historical.columns).intersection(df_updates.columns)
        on_condition = "\t\t    AND ".join(f"target.{col} = source.{col}\n" for col in common_columns)

        self.spark.sql(f"""
            MERGE INTO `old_data` AS target
            USING (SELECT * FROM `new_data`) AS source
            ON
                {on_condition}
            WHEN NOT MATCHED THEN
                INSERT *
        """)
        df = self._load_delta_table(table_name).dropDuplicates()
        return df

    def __process_df(self, df_origin: DataFrame, table_name: str) -> DataFrame:
        """
        Processes table data.

        Args:
            df (DataFrame): The DataFrame to process.

        Returns:
            DataFrame: The resulting DataFrame after processing.

        Tests (tmp):
            test change row:
                df_updates = self.add_id_cols(df_origin, 'tables')
                df_updates.filter(col("table_id") == "dev_silver.oracle_api_hcm.jobslov").display()
                df_updates = df_updates.withColumn("table_description", 
                                    when(col("table_id") == "dev_silver.oracle_api_hcm.jobslov", "42").otherwise(col("table_description")))
                df_updates.filter(col("table_id") == "dev_silver.oracle_api_hcm.jobslov").display()
            
            test new row:
                from pyspark.sql import Row
                df_updates.filter(col("table_id") == "dummy.dummy.dummy").display()
                new_data = Row(
                    layer="dummy",
                    source='dummy',
                    table='dummy',
                    obj_type="dummy",
                    layer_raw='dummy',
                    source_raw="dummy",
                    table_description='dummy',
                    table_created_at='2023-11-27T15:09:14.172+00:00',
                    table_last_updated_at='2023-11-27T15:09:14.172+00:00',
                    tag_table_data_steward='dummy',
                    tag_table_frequency_ingestion='dummy',
                    tag_table_source_system='dummy',
                    tag_table_type_ingestion='dummy',
                    table_id='dummy.dummy.dummy',
                )

                new_data_df = self.spark.createDataFrame([new_data])
                df_updates = df_updates.unionByName(new_data_df)
                df_updates = df_updates \
                    .withColumn("table_created_at", col("table_created_at").cast("timestamp")) \
                    .withColumn("table_last_updated_at", col("table_last_updated_at").cast("timestamp"))
                df_updates.filter(col("table_id") == "dummy.dummy.dummy").display()
            
            test new col:
                from pyspark.sql.functions import lit
                df_updates = df_updates.withColumn('new_tag', lit('42'))
                df_updates.display()
        """
        df_updates = self.add_id_cols(df_origin, table_name)
        delta_table_path = self.get_delta_path(table_name)
        
        try:
            # check if data already exists
            df_historical = self._load_delta_table(table_name)
            self.logger.info(f'Total rows in bronze (historical) = {df_historical.count()}')
            df_historical_updated = self._append_changes_and_new_rows(df_updates, df_historical, table_name)

        except AnalysisException as e:
            # wipe folder to avoid read the old schema
            self.dbutils.fs.rm(delta_table_path, recurse=True)
            
            self.logger.warning(f'Delta Table: {table_name} on Azure '
                                f'{self.layer_name}/{self.folder_name} not found.\n{e}')
            self.logger.info(f'The created_at and last_updated cols are get from '
                             f'information_schema in Unity Catalog')
            return df_updates

        else:
            try:
                # add new cols
                new_cols = list(set(df_updates.columns) - set(df_historical_updated.columns))
                singular_table_name = table_name[:-1]

                if len(new_cols) > 0:
                    self.logger.info(f'Identified {new_cols} as a new tag(s)')
                    # wipe folder to avoid read the old schema
                    self.dbutils.fs.rm(delta_table_path, recurse=True)
                    return df_updates
                else:
                    return df_historical_updated

            except Exception as e:
                return df_updates

    def execute(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Args:
            df (DataFrame): The DataFrame to execute transformations on.
            table_name (str): The name of the table.

        Returns:
            DataFrame: The resulting DataFrame after transformations.
        """
        if table_name in ['sources', 'tables', 'fields']:
            return self.__process_df(df, table_name)

        else:
            raise ValueError(f"Unsupported table_name: {table_name}")
