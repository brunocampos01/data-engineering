import time
from typing import Any

from pyspark.errors import (
    AnalysisException,
    ParseException,
)
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import when

from data_catalog.src.base_data_catalog import BaseDataCatalog

PROGRESS_LOG_INTERVAL = 100


class MetadataUCWriter(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def __get_content(self, df: DataFrame, col_name: str) -> Any:
        """
        Args:
            df (DataFrame): The input DataFrame.
            col_name (str):
                example: 'tags_aggregated' or 'obj_type' or table_description
        """
        try:
            result = df.select(when(df[col_name].isNotNull(), df[col_name]).otherwise(None))
            return result.first()[0]
        except TypeError:
            self.logger.error(f'{col_name} not found in df.')
            return None

    def __set_catalog_metadata(self, catalog_description: str) -> None:
        full_layer_name = f'{self.env}_{self.layer_name}'
        if catalog_description is not None:
            self.spark.sql(f"""
                COMMENT ON CATALOG {full_layer_name}
                IS "{catalog_description}"
            """)

    def execute_set_catalog_metadata(self, df: DataFrame) -> None:
        catalog_description = self.__get_content(df, 'layer_description')
        try:
            self.__set_catalog_metadata(catalog_description)
        except Exception as e:
            self.logger.error(f'Error catalog: {e}')

    def __set_db_metadata(
        self,
        path_db_uc: str,
        db_tags_content: str,
        db_description: str,
        current_time: str
    ) -> None:
        """
        Set metadata for a database in Unity Catalog.

        Args:
            path_db_uc (str): The path to the database.
                e.g.: dev_silver.compas_shipsure_crew
            db_tags_content (str): The content of tags to be set for the database.
                e.g.:
                    '''
                    'data_steward' = '_',
                    'csl_internal_system' = '_',
                    'active_system' = '_'
                    '''
            db_description (str): The description to set for the database.
                e.g.: 'IMO number'
            current_time (str): The current time to set as a DB property.
        """
        executed = False

        if db_tags_content is not None:
            self.spark.sql(f"""
                ALTER DATABASE {path_db_uc}
                SET TAGS ({db_tags_content})
            """)
            executed = True

        if db_description is not None:
            self.spark.sql(f"""
                COMMENT ON DATABASE {path_db_uc}
                IS "{db_description}"
            """)
            executed = True

        if executed:
            self.spark.sql(f"""
                ALTER DATABASE {path_db_uc}
                SET DBPROPERTIES ('db_catalog_updated_at' = '{current_time}')
            """)

    def execute_set_db_metadata(
        self,
        df: DataFrame,
        db_name: str,
        path_db_uc: str,
        list_db_tags_names: list,
        current_time: str,
    ) -> None:
        db_description = self.__get_content(df, 'source_description')
        db_tags = self.__get_content(df, 'tags_aggregated')

        try:
            self.__set_db_metadata(
                path_db_uc,
                db_tags,
                db_description,
                current_time,
            )
        except Exception as e:
            self.logger.error(f'Error database: {e}')

    def execute_statements(self, list_statements: list[str], col_name: str) -> None:
        total_executed = 0
        list_not_executed = []
        for stat in list_statements:
            self.logger.info(f'Executing {col_name}:\n{stat}')
            time.sleep(1)  # when executed normaly some statements was not executed

            try:
                self.spark.sql(stat)
                total_executed += 1

                if total_executed % PROGRESS_LOG_INTERVAL == 0:
                    print(f'{total_executed} statements executed.')

            except (ParseException, AnalysisException) as e:
                self.logger.error(f'{e}\n')
                list_not_executed.append(stat)

        self.logger.warning(f'List of statements in {col_name} not executed = {list_not_executed}\n')
        self.logger.info(f'Total {col_name} statements executed = {total_executed}')
