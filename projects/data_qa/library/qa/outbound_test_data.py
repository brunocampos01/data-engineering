from library.database.azure_sql import AzureSQL
from library.qa.template_data_tests import TemplateDataTest
from library.qa.schema_helper import get_data_type_mapping


class OutboundTestData(TemplateDataTest):
    """Class for testing the data between outbound and azure storage account. This class following the skeleton define at library.qa.template_data_tests"""

    def __init__(
        self,
        spark,
        schema_name: str,
        table_name: str,
        catalog_name: str,
        step_layers: str,
    ):
        super().__init__()
        self.spark = spark
        self.table_name = table_name
        self.schema_name = schema_name
        self.catalog_name = catalog_name
        self.step_layers = step_layers
        self._azure_loader = AzureSQL()

    def log_execution_parameters(self) -> None:
        self.logger.info("***************** Execution Parameters *****************")
        self.logger.info(f"environment:  {self.env}")
        self.logger.info(f"catalog_name: {self.catalog_name}")
        self.logger.info(f"schema_name:  {self.schema_name}")
        self.logger.info(f"table_name:   {self.table_name}")
        self.logger.info(f"step_layers:  {self.step_layers}")
