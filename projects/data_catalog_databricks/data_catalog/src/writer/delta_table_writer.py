import os
import tempfile
from typing import List

import pandas as pd
from azure.storage.blob import BlobServiceClient
from pyspark.sql import DataFrame

from data_catalog.src.base_data_catalog import BaseDataCatalog


class DeltaTableWriter(BaseDataCatalog):
    def __init__(self, spark, layer_name: str, container_name: str, folder: str):
        super().__init__(spark, layer_name)
        self.container_name = container_name
        self.folder = folder
        self.delta_path = self.get_storage_url(self.container_name, self.folder)

    def __save_delta_table(self, df: DataFrame, delta_table_name: str) -> None:
        """
        Saves the DataFrame as a Delta table with the specified table name.

        Args:
            df (DataFrame): The DataFrame to be saved as a Delta table.
            delta_table_name (str): The name of the Delta table.
                e.g.: 'fields'
        """
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(f'{self.delta_path}/{delta_table_name}')

    def _save_excel_to_azure_blob(
        self, adls_conn: BlobServiceClient, container_name: str, file_name: str,
    ) -> None:
        """
        Uploads an Excel file to an Azure Blob Storage container.

        Args:
            adls_conn (BlobServiceClient): The Azure Data Lake Storage client.
            file_name (str): The name of the Excel file to load.
                e.g.: DataCatalog.xlsx
            container_name: e.g.: bronze, silver, gold
        """
        adls_file_path = f'data_catalog_processed/{file_name}'.replace('/tmp/', '')
        blob_client = adls_conn.get_blob_client(container_name, adls_file_path)

        try:
            with open(file_name, 'rb') as data:
                blob_client.upload_blob(data, overwrite=True)
        except Exception as e:
            raise Exception(
                f"Failed to upload blob. Check the parameters:\n"
                f"container_name: {container_name}\n"
                f"adsl_file_path: {adls_file_path}\n"
                f"{e}")
        else:
            self.logger.info(f"Saved at: {container_name}/{adls_file_path}")

    def __save_table_excel(
        self,
        list_df: List,
        adls_conn: BlobServiceClient,
        file_name: str,
        list_sheet_tab: List,
    ) -> None:
        """
        Save a list of df to an Excel file and upload it to Azure Blob Storage.

        Args:
            list_df (list): A list of DataFrames to be saved in the Excel file.
                e.g.: [df_uc_sources, df_uc_tables, df_uc_cols]
            container_name (str): Name of the Azure Blob Storage container.
                e.g.: silver
            file_name (str): Name of the Excel file to be saved.
                e.g.: DataCatalog.xlsx
            folder (str): Folder in the Azure Blob Storage container.
                e.g.: data_catalog
            list_sheet_tab (list): List of sheet names for the DataFrames.
                e.g.: ['Sources', 'Tables', 'Fields']
        """
        temp_folder = tempfile.gettempdir()
        excel_file_path = os.path.join(temp_folder, file_name)

        with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='w') as writer:
            for df, sheet_tab in zip(list_df, list_sheet_tab):
                pdf = df.toPandas()
                pdf.to_excel(
                    writer, 
                    index=False, 
                    header=True, 
                    sheet_name=sheet_tab, 
                    freeze_panes=(1, 1),
                )

        self._save_excel_to_azure_blob(
            adls_conn=adls_conn,
            container_name=self.container_name,
            file_name=excel_file_path,
        )

    def execute_cleaning(self, folder_name: str, file_name: str) -> None:
        adls_conn = self.get_azure_conn()
        adls_file_path = f'{folder_name}/{file_name}'

        try:
            blob_client = adls_conn.get_blob_client(self.container_name, adls_file_path)
            blob_client.delete_blob()
        except Exception as e:
            self.logger.error(f'It was not possible to clean the folder: {folder_name}. \{e}')
        else:
            self.logger.info(f"The file {file_name} in {self.container_name} have been deleted.")

    def execute(
        self,
        data_format: str,
        table_name: str,
        df: DataFrame = None,
        list_df: List[DataFrame] = None,
    ) -> None:
        if data_format == 'delta':
            self.__save_delta_table(df, table_name)

        elif data_format == 'excel':
            self.__save_table_excel(
                list_df=list_df,
                adls_conn=self.get_azure_conn(),
                file_name='DataCatalog.xlsx',
                list_sheet_tab=['Data Stewards', 'Layers', 'Sources', 'Tables', 'Fields'],
            )

        else:
            self.logger.error(f'data_format = {data_format} not found!')
