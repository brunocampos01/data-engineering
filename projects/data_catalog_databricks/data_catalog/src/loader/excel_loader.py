import os
import tempfile

import pandas as pd
from azure.storage.blob import BlobServiceClient
from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog


class ExcelLoader(BaseDataCatalog):
    def __init__(
        self,
        spark: SparkSession,
        layer_name: str,
        container_name: str,
        folder: str,
    ):
        """
        Args:
            adls_conn (BlobServiceClient): The Azure Blob Service client.
            container_name (str): The name of the Azure Blob Storage container.
                e.g.: silver
            folder (str): The folder within the container where the file is located.
                e.g.: data_catalog
        """
        super().__init__(spark, layer_name)
        self.container_name = container_name
        self.folder = folder

    def load_excel_from_azure_blob(
        self,
        adls_conn: BlobServiceClient,
        file_name: str,
        sheet_tab: str,
    ) -> pd.DataFrame:
        """
        Load an Excel file from an Azure Blob Storage container and return it as a pd df

        Args:
            adls_conn (BlobServiceClient): The Azure Blob Service client.
            file_name (str): The name of the Excel file to load.
                e.g.: DataCatalog.xlsx
            sheet_tab (str): The name of the sheet/tab within the Excel file to read.
                e.g.: Sources

        Returns:
            pandas DataFrame
        """
        adls_file_path = f'{self.folder}/{file_name}'
        blob_client = adls_conn.get_blob_client(self.container_name, adls_file_path)

        with tempfile.TemporaryDirectory() as temp_folder:
            excel_file_path = os.path.join(temp_folder, file_name)
            try:
                with open(excel_file_path, "wb") as blob:
                    data = blob_client.download_blob()
                    data.readinto(blob)
            except FileExistsError as e:
                self.logger.warn(
                    f"Failed to download blob. Check the parameters:\n"
                    f"container_name: {self.container_name}\n"
                    f"adls_file_path: {adls_file_path}\n"
                    f"{e}")
            except Exception as e:
                self.logger.warn(f'The blob does not exist in {self.container_name}. Using {file_name} file.')
                excel_file_path = file_name

            pdf = pd.read_excel(excel_file_path, sheet_tab, header=0, engine='openpyxl')
            pdf = pdf.astype(str).where(pd.notna(pdf), None)
        
            return pdf

    def execute(
        self,
        file_name: str,
        sheet_tab: str,
    ) -> DataFrame:
        """
        Reads data from an Excel file, converts it to a PySpark df,
        and performs column standardization.

        Args:
            folder (str): The path to the Excel file.
                e.g.: data_catalog/
            sheet_tab (str): The name of the Excel sheet to read.
                e.g.: Sources

        Returns:
            pyspark DataFrame
            +---------+ ...+----+-------------+-----------+------------+
            |   source| ...|csl_|active_system|description|data_steward|
            +---------+ ...+----+-------------+-----------+------------+
            |     imos| ...|    |            _|          _|           _|
            |   compas| ...|    |            _|          _|           _|
        """
        pdf = self.load_excel_from_azure_blob(
            adls_conn=self.get_azure_conn(),
            file_name=file_name,
            sheet_tab=sheet_tab,
        )
        return self.spark.createDataFrame(pdf)
