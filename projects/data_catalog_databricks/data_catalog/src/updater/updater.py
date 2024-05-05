import datetime
from typing import (
    Dict,
    List,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import col

from data_catalog.src.base_data_catalog import BaseDataCatalog
from data_catalog.src.updater.metadata_uc_writer import MetadataUCWriter
from data_catalog.src.utils import get_list_statements


class Updater(BaseDataCatalog):
    """
    This class check if object exists in UC
    and execute iterations on all objects in Unity Catalog.
    """
    def __init__(
        self, spark: SparkSession, layer_name: str, dict_map_sources_tables: Dict
    ):
        super().__init__(spark, layer_name)
        self.full_layer_name = f'{self.env}_{self.layer_name}'
        self.dict_map_sources_tables = dict_map_sources_tables
        self.current_time = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.writer_uc = MetadataUCWriter(self.spark, self.layer_name)

    def get_dict_path_dbs(self):
        """
        Returns a dictionary containing the paths of various database tables.

        Returns:
            (dict): A dict where the keys are the names of the databases
            and the values are lists of corresponding table paths.

            Example:
                {
                    'bloomberg': ['dev_silver.bloomberg'],
                    'compas': ['dev_silver.compas', 'dev_silver.compas_shipsure_crew'],
                    'concur': ['dev_silver.concur'],
                    # ...
                }
        """
        dict_dbs_path = {}
        for db_name, dict_db_tables in self.dict_map_sources_tables.items():
            db_raw_name = list(dict_db_tables.keys())
            dict_dbs_path[db_name] = [f'{self.full_layer_name}.{table}'
                                      for table in db_raw_name]
        return dict_dbs_path

    def get_dict_path_tables(self) -> Dict:
        """
        Return:
            e.g.: { 'compas': ['dev_silver.crew_contact',
                               ...
                              'dev_silver.projects'],
                    'compas_shipsure_crew': ['dev_silver.integration_seafarer',
                                            'dev_silver.service_record'],
                   ... }
        """
        dict_tables_path = {}
        for dict_db_tables in self.dict_map_sources_tables.values():
            db_raw_name = list(dict_db_tables.keys())

            for db_name in db_raw_name:
                dict_tables_path[db_name] = [f'{self.full_layer_name}.{db_name}.{table}'
                                             for table in dict_db_tables.get(db_name)]

        return dict_tables_path

    def __update_catalog(self, df_origin_layers: DataFrame) -> None:
        """
        Update the catalog metadata.

        Args:
            df_origin_layers (DataFrame): The DataFrame to update the catalog with.
                Example: df_origin_layers
        """
        self.logger.info(f'Layer: {self.full_layer_name}')
        df = df_origin_layers.filter(col("layer") == self.layer_name)
        self.writer_uc.execute_set_catalog_metadata(df)

    def __update_db(self, df_origin_sources: DataFrame, list_tags: List[str]) -> None:
        """
        Update the database based on the provided DataFrame and list of tags.

        Args:
            df_origin_sources: The DataFrame containing the original sources.
                e.g.: df_origin_sources
            list_tags (List[str]): List of tags to be used.
                e.g.: list_tags_names_sources
                      or ['csl_internal_system', 'active_system', 'data_certified']
        """
        dict_path_dbs = self.get_dict_path_dbs()

        for db_name, list_path_dbs in dict_path_dbs.items():
            for path in list_path_dbs:

                if self.spark.catalog.databaseExists(path):
                    self.logger.info(f"db: {path}")
                    df = df_origin_sources.filter(col("source") == db_name)

                    self.writer_uc.execute_set_db_metadata(
                        df=df,
                        db_name=db_name,
                        path_db_uc=path,
                        list_db_tags_names=list_tags,
                        current_time=self.current_time,
                    )
                else:
                    self.logger.info(f'The {path} does not exists in UC!')

    def __update_table(self, df_tables: DataFrame, list_tags: List[str]) -> None:
        """
        Updates the table using the provided DataFrame and list of tags.

        Args:
            df_tables (DataFrame): DataFrame to update the table.
                e.g.: df_tables
            list_tags (List[str]): List of tags to be associated with the table.
                e.g.: list_origin_tables_tags_names
                      or ['frequency_ingestion', 'type_ingestion', 'data_steward']
        """
        list_tag_statements = get_list_statements(df_tables, 'tag_statement')
        self.logger.info(f'Total tag statements to add = {len(list_tag_statements)}')
        self.writer_uc.execute_statements(list_tag_statements, 'tag_statement')

        list_description_statement = get_list_statements(df_tables, 'description_statement')
        self.logger.info(f'Total description statements to add = {len(list_description_statement)}')
        self.writer_uc.execute_statements(list_description_statement, 'description_statement')

    def __update_field(self, df_field: DataFrame, type_metadata: str) -> None:
        """
        Update the field in the DataFrame based on the list of tags.

        Args:
            df_field (DataFrame): The DataFrame to update.
                e.g.: df_fields
            type_metadata: (str): define which metadata will update. 
                Example: 'tags' or 'descriptions'

        Notes:
            To avoid these message during execution:
            *** WARNING: max output size exceeded, skipping output. ***
            The updater for fields it's got to use the param: type_metadata
        """
        if type_metadata == 'tags':
            list_tag_statements = get_list_statements(df_field, 'tag_statement')
            self.logger.info(f'Total tag statements to add = {len(list_tag_statements)}')
            self.writer_uc.execute_statements(list_tag_statements, 'tag_statement')

        elif type_metadata == 'descriptions':
            # It is not allow add comments in views, because of this it is necessary to filter
            df_field = df_field.filter(col('obj_type') == 'TABLE')

            list_description_statements = get_list_statements(df_field, 'description_statement')
            self.logger.info(f'Total descriptions statements to add = {len(list_description_statements)}')
            self.writer_uc.execute_statements(list_description_statements, 'description_statement')

    def execute(
        self,
        delta_table_name: str,
        df: DataFrame,
        list_tags: List = None,
        type_metadata: str = None,
    ) -> None:
        """
        Executes different update operations based on the given delta table name.

        Args:
            delta_table_name (str): Name of the delta table.
            df (DataFrame): DataFrame to be used for updating.
            update_only_fields (bool, optional): If True, update only fields.
            list_tags (List[str], optional): List of tags.
        """
        df.cache()  # useful to apply filters

        if delta_table_name == 'catalogs':
            self.__update_catalog(df)

        elif delta_table_name == 'sources':
            self.__update_db(df, list_tags)

        elif delta_table_name == 'tables':
            self.__update_table(df, list_tags)

        elif delta_table_name == 'fields':
            self.__update_field(df, type_metadata)

        else:
            raise ValueError(f"Unsupported delta_table_name: {delta_table_name}")
