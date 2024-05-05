import os
from abc import ABC
from typing import List

import IPython
from pyspark.sql import SparkSession

from library.database.azure_adls import AzureADSL
from library.datalake_storage import DataLakeStorage
from library.logger_provider import LoggerProvider


class BaseDataCatalog(ABC):
    def __init__(self, spark: SparkSession, layer_name: str):
        self.spark = spark
        self.logger = LoggerProvider.get_logger()
        self.env = os.getenv("Environment").lower()
        self.layer_name = layer_name
        self.folder_name = 'data_catalog'
        ipython = IPython.get_ipython()
        self.dbutils = ipython.user_ns["dbutils"] if ipython else None

    @staticmethod
    def get_azure_conn():
        return AzureADSL()._create_conn()

    def get_container_client(self, container_name: str):
        return self.get_azure_conn().get_container_client(container=container_name)

    @staticmethod
    def get_storage_url(container_name: str, folder: str) -> str:
        """
        container_name (str): The name of the Azure Blob Storage container.
            e.g.: silver
        folder (str): The folder within the container where the file is located.
            e.g.: data_catalog
        """
        return DataLakeStorage.get_storage_url(container_name, folder)

    def get_folder_names_adsl(self, container_name: str, folder: str) -> List[str]:
        """
        Fetches the names of the folders inside a specified folder in
        an Azure Blob Storage container.

        Args:
            container_name (str): The name of the Azure Blob Storage container.
                e.g.: bronze
            folder (str): The name of the folder inside the container.
                e.g.: data_catalog/

        Returns:
            List[str]: A list of folder names.
                e.g.: ['data_stewards', 'fields', 'layers', 'sources', 'tables']
        """
        container_client = self.get_container_client(container_name)
        blob_list = container_client.list_blobs(folder)

        list_delta_tables = []
        for blob in blob_list:
                folder = blob.name.split('/')
                if len(folder) == 2:
                    list_delta_tables.append(folder[1])

        return list_delta_tables

    @staticmethod
    def get_list_cols_default():
        """
        Used this list to get only the tags
        """
        return ['layer_raw', 'layer', 'table', 'field',
                'source_raw', 'source',
                'data_type', 'path_table_uc', 'path_col_uc'
                'type_obj', 'obj_type',
                'created_at', 'last_updated_at',
                'source_created_at', 'source_last_updated_at',
                'table_created_at', 'table_last_updated_at',
                'field_created_at', 'field_last_updated_at',
                'layer_description', 'source_description',
                'table_description', 'field_description',
                'type_extraction', 'sa_checked']
