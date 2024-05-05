import os
from datetime import datetime
from IPython import get_ipython

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

from library.logger_provider import LoggerProvider

logger = LoggerProvider.get_logger()


class AzureADSL:
    """
    Initialize the AzureADSL class to manage Azure ADLS connections.
    Retrieves necessary configuration details from environment variables and secrets.
    """
    def __init__(self):
        dbutils = get_ipython().user_ns["dbutils"]
        scope = os.environ["KVScope"]

        storage_account = dbutils.secrets.get(scope, "ADLS-storage-account")
        self.account_url = f"https://{storage_account}.blob.core.windows.net"
        self.tenantid = dbutils.secrets.get(scope, "tenantid")
        self.client_id = dbutils.secrets.get(scope, "ADLS-adls-app-id")
        self.client_secret = dbutils.secrets.get(scope, "ADLS-adls-app")

    def _get_access_token(self) -> ClientSecretCredential:
        """Returns the access token to connect to Azure ADLS"""
        return ClientSecretCredential(
            tenant_id=self.tenantid,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def _create_conn(self):
        """Return an obj that allows to manipulate Azure service resources and blob containers"""
        try:
            return BlobServiceClient(self.account_url, self._get_access_token())
        except Exception as e:
            raise Exception(f"Failed to create BlobServiceClient: {e}")

    @staticmethod
    def generate_adsl_path(
            step_layers: str, schema_target_name: str, table_target_name: str
    ) -> str:
        """
        Generate an ADLS path with filename for storing the report.

        Args:
            step_layers: The step and layers information.
                e.g.: staging_to_onpremises
            schema_target_name: The target schema name.
                e.g.:dw
            table_target_name: The target table name.
                e.g.: dim_account

        Returns:
            str: The generated ADLS path.
                e.g.: staging_to_onpremises/dw/dim_account/dim_account_2023....html
        """
        curr_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"{step_layers}" \
               f"/{schema_target_name}" \
               f"/{table_target_name}" \
               f"/{table_target_name}_{curr_datetime}.html"

    def save_report_tests(
            self,
            html_result_gx: str,
            adsl_path: str,
            container_name: str,
    ) -> None:
        """Save the report tests to Azure ADLS.

        Args:
            html_result_gx: The HTML content of the report.
            adsl_path: The ADLS path to save the report.
                e.g.: staging_to_onpremises/dw/dim_account/dim_account_202....html
            container_name: The name of the ADLS container.
                e.g: quality-assurance
        """
        adsl_conn = self._create_conn()
        adsl_conn.get_container_client(container_name)
        blob_client = adsl_conn.get_blob_client(container_name, adsl_path)

        try:
            blob_client.upload_blob(html_result_gx)
        except TypeError as e:
            raise Exception(
                f"Failed to upload blob. Check the parameters:\n"
                f"container_name: {container_name}\n"
                f"adsl_path: {adsl_path}\n"
                f"{e} the html_result_gx needs to be string.")
        else:
            logger.info(f"Saved at: {self.account_url}/{container_name}/{adsl_path}")
