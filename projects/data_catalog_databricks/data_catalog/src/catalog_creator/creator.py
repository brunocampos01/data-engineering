import os
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
)

from pyspark.sql import SparkSession

from data_catalog.src.catalog_creator.base_creator import BaseCreator
from data_catalog.src.catalog_creator.catalog_helper import CatalogHelper
from data_catalog.src.catalog_creator.db_helper import DBHelper
from data_catalog.src.catalog_creator.table_helper import TableHelper


class Creator(BaseCreator):
    def __init__(self, spark: SparkSession, layer_name: str, storage_account: str, owner: str):
        super().__init__(spark, layer_name, owner)
        self.catalog_name = f"{self.env}_data_catalog"
        self.folder_name = 'data_catalog'
        self.storage_account = storage_account
        self.sql_base_path = "".join(os.getcwd() + "/../" + "src/catalog_creator/").replace('/Workspace', '')
        self.sql_path_create_table = "".join(self.sql_base_path + "create_table")
        self.parent_folder = os.path.dirname(os.getcwd())
        self.max_execution_time = 60

    @staticmethod
    def find_key_by_value(dictionary: Dict, target_value: Any) -> Optional[Any]:
        """
        Find and return the key in the dict that corresponds to the given target value.

        Args:
            dictionary (Dict[Any, Any]): The dict in which to search for the value.
            target_value (Any): The value to search for in the dict.

        Returns:
            Any: The key in the dict
        """
        for key, value in dictionary.items():
            if value == target_value:
                return key
        return None

    def get_sk_names(self, list_delta_tables: List[str]) -> Tuple[Dict, Dict]:
        """
        Get surrogate key names from a list of delta tables.

        Parameters:
            list_delta_tables (List[str]): A list of delta table names.
                e.g.: ['data_stewards', 'fields', 'layers', 'sources', 'tables']

        Returns:
            A tuple containing two dictionaries,
                the first for gold fact SKs
                e.g.:
                {
                    'fact_fields': ['sk_layer', ...],
                    'fact_sources': ['sk_source', ...],
                    ...
                 }
                and the second for gold dim SKs.
                e.g.:
                {
                    'dim_data_stewards': 'sk_data_steward',
                    'dim_layers': 'sk_layer',
                    ...
                }
        """
        dict_fact_sk = {}
        dict_dim_sk = {}

        for table_name in list_delta_tables:
            df = self._load_delta_table(table_name)
            list_sk = [col for col in df.columns if col.startswith('sk_')]

            if table_name.startswith('dim'):
                sk_col = list_sk[0]
                dict_dim_sk[table_name] = sk_col
            else:
                dict_fact_sk[table_name] = list_sk

        return dict_fact_sk, dict_dim_sk

    def add_comments(self, list_delta_tables: List[str]) -> None:
        """
        Adds a comment to the specified col

        Args:
            A dict where the keys are column names and the values are comments.
                e.g.: dict_fields_comment_cols
        """
        dict_comment = self.get_dict_comment_cols()

        for table_name in list_delta_tables:
            for column_name, comment_msg in dict_comment.items():
                try:
                    self.spark.sql(f"""
                        ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table_name}`
                        ALTER COLUMN {column_name} COMMENT '{comment_msg}'
                    """)
                except Exception as e:
                    continue

    def add_pk(self, dict_pk: Dict) -> None:
        """
        Adds a primary key constraint to the specified col

        Args:
            A dict where the keys are table names and the values are col names.
                e.g.: dict_gold_fact_pk
        """
        for table, column_name in dict_pk.items():
            self.logger.info(f"Creating PK for {table}")
            if isinstance(column_name, str):
                constraint_name = f'{self.layer_name}_{table}_{column_name}'
                try:
                    self.spark.sql(f"""
                        ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                        ALTER COLUMN {column_name} SET NOT NULL
                    """)
                    self.spark.sql(f"""
                        ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                        ADD CONSTRAINT {constraint_name} PRIMARY KEY(`{column_name}`);
                    """)

                except Exception as e:
                    raise Exception(f"Error adding PK constraint for {table}:\n{e}")

            else:
                # composite key
                list_cols = column_name
                for c in list_cols:
                    try:
                        self.spark.sql(f"""
                            ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                            ALTER COLUMN {c} SET NOT NULL
                        """)
                    except Exception:
                        # the NOT NULL can already exists
                        continue

                key_composite = str(list_cols).replace('[', '').replace(']', '').replace('\'', '`')
                constraint_name = f'{self.layer_name}_{table}_{list_cols[0]}_{list_cols[1]}_pk_composite'
                self.spark.sql(f"""
                    ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                    ADD CONSTRAINT {constraint_name} PRIMARY KEY({key_composite});
                """)

    def add_fk(
        self, dict_gold_fact_sk: Dict[str, list], dict_gold_dim_sk: Dict[str, str]
    ) -> None:
        """
        Adds foreign key constraints to the specified columns

        Args:
            Dict mapping fact_table_name: fact_foreign_key.
            e.g.:
                {
                    'fact_fields': ['sk_layer', ...],
                    'fact_sources': ['sk_source', ...],
                    ...
                 }
            Dict mapping dimension_table_name: dimension_primary_key
            e.g.:
                {
                    'dim_data_stewards': 'sk_data_steward',
                    'dim_layers': 'sk_layer',
                    ...
                }
        """
        for table, list_cols in dict_gold_fact_sk.items():

            for column_name in list_cols:
                constraint_name = f'{self.layer_name}_{table}_{column_name}_fk'
                self.logger.info(f"Creating FK for {table}: {constraint_name}")

                try:
                    self.spark.sql(f"""
                        ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                        ALTER COLUMN {column_name} SET NOT NULL
                    """)
                    dim_table_name = self.find_key_by_value(dict_gold_dim_sk, column_name)
                    self.spark.sql(f"""
                        ALTER TABLE `{self.env}_data_catalog`.`{self.layer_name}`.`{table}`
                        ADD CONSTRAINT {constraint_name} FOREIGN KEY(`{column_name}`)
                            REFERENCES `{self.env}_data_catalog`.`{self.layer_name}`.`{dim_table_name}`;
                    """)

                except Exception as e:
                    raise Exception(f"Error adding FK for "
                                    f"{self.env}_data_catalog.{self.layer_name}.{table} in {column_name}:\n{e}")

    def create_tables(self, list_delta_tables: List[str]) -> None:
        table_helper = TableHelper(
            spark=self.spark, 
            layer_name=self.layer_name, 
            owner=self.owner,
            storage_account=self.storage_account,
        )
        table_helper.use_catalog()

        for table_name in list_delta_tables:
            self.logger.info(f"Creating table: {self.layer_name}.{table_name}")
            table_helper.drop(self.layer_name, table_name)
            table_helper.create(self.layer_name, table_name)
            table_helper.alter_owner(self.layer_name, table_name)
            table_helper.alter_tags(self.layer_name, table_name)
            table_helper.comment_on_table(self.layer_name, table_name)

    def __process_bronze(self, list_delta_tables: List[str]) -> None:
        self.create_tables(list_delta_tables)
        dict_bronze_table_pk = self.get_dict_bronze_table_pk()
        self.add_pk(dict_bronze_table_pk)
        self.add_comments(list_delta_tables)

    def __process_silver(self, list_delta_tables: List[str]) -> None:
        self.create_tables(list_delta_tables)
        dict_silver_table_pk = self.get_dict_silver_table_pk()
        self.add_pk(dict_silver_table_pk)
        self.add_comments(list_delta_tables)

    def __process_gold(self, list_delta_tables: List[str]) -> None:
        self.create_tables(list_delta_tables)

        # constraints
        dict_gold_fact_pk = self.get_dict_gold_fact_pk()
        dict_gold_fact_sk, dict_gold_dim_sk = self.get_sk_names(list_delta_tables)
        dict_gold_dim_pk = dict_gold_dim_sk # to simplify, create a dict with PK dim
        self.add_pk(dict_gold_fact_pk)
        self.add_pk(dict_gold_dim_pk)
        self.add_fk(dict_gold_fact_sk, dict_gold_dim_sk)
        self.add_comments(list_delta_tables)

    def catalog_execute(self) -> None:
        catalog = CatalogHelper(
            spark=self.spark, layer_name=self.layer_name, owner=self.owner
        )

        catalog_name = f'{self.env}_data_catalog'
        if self.env == 'prod':
            catalog_name = 'data_catalog'

        catalog.create(catalog_name)
        catalog.alter_owner(catalog_name)
        catalog.alter_tags(catalog_name)
        catalog.grant_privileges()
        catalog.drop_schema(catalog_name)
        catalog.comment_on_catalog(catalog_name)
        self.logger.info(f'Created data_catalog catalog.')

    def db_execute(self, owner: str) -> None:
        db_helper = DBHelper(
            spark=self.spark, layer_name=self.layer_name, owner=self.owner
        )
        db_helper.use_catalog()

        if not self.spark.catalog.databaseExists(self.layer_name):
            db_helper.create(self.layer_name)
            db_helper.alter_owner(self.layer_name)
            db_helper.grant_privileges(self.layer_name)
            db_helper.alter_tags(self.layer_name, 'data_governance')
            db_helper.comment_on_database(self.layer_name)
            self.logger.info(f'Created {self.layer_name} database.')

    def table_execute(self, owner: str) -> None:
        """
        Execute table processing based on the provided layer name.

        Args:
            owner (str): The owner of the table in Unity Catalog
        """
        list_delta_tables = self.get_folder_names_adsl(
            container_name=self.layer_name, folder=self.folder_name
        )
 
        if self.layer_name == 'bronze':
            return self.__process_bronze(list_delta_tables)

        elif self.layer_name == 'silver':
            return self.__process_silver(list_delta_tables)

        elif self.layer_name == 'gold':
            self.__process_gold(list_delta_tables)

        else:
            raise ValueError(f'layer_name = {self.layer_name} not found!')
