"""
### Module - TemplateDataTests for QA Library

#### Class:
    * TemplateDataTest: Template Method Design Partner
"""
import os
import warnings
from abc import abstractmethod
from typing import Dict

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.render import DefaultJinjaPageView
from great_expectations.render.renderer import ValidationResultsPageRenderer

from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame

from library.database.azure_adls import AzureADSL
from library.logger_provider import LoggerProvider
from library.qa.great_expectations_helper import GreatExpectationsHelper


warnings.filterwarnings("ignore", category=DeprecationWarning)


class TemplateDataTest:
    """
    ### Template method as design pattern
    ### Base class representing a skeleton of algorithm for data tests.
    ### Subclasses should implement the abstract methods to define their specific data testing.
    """
    def __init__(self):
        self.logger = LoggerProvider.get_logger()
        self.env = os.getenv("Environment").lower()
        self.scope = os.getenv("KVScope")
        self.container_adls_name = "quality-assurance"

    @abstractmethod
    def run_template_method(self) -> None:
        """
        ### The template method defines the skeleton of an algorithm for data testing.
        ### The method calls several methods in a specific order to carry out the data testing process.

        ### This method code must implement in notebooks to allow that QA team. 
        see what is happening during the tests and if necessary debug inside it.

        #### Example:
            * data_tester = BronzeToSilverTestData(params, ...)
            * data_tester.log_execution_parameters()
            * data_tester.validate_parameters()

            * data_tester.get_and_prepare_data()
            * data_tester.execute_custom_data_tests()
            * data_tester.execute_great_expectations()
            * data_tester.execute_data_tests()

            * data_tester.save_report_tests_azure_storage()
            * data_tester.display_results()
        """
        pass

    @abstractmethod
    def log_execution_parameters(self) -> None:
        # Implement logging of execution parameters specific to your data test
        pass

    @abstractmethod
    def validate_parameters(self) -> None:
        # Implement parameter validation specific for layer
        pass

    @abstractmethod
    def get_and_prepare_data(self, kwargs: Dict = None) -> DataFrame:
        pass

    @abstractmethod
    def execute_custom_data_tests(
        self,
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        kwargs: Dict = None,
    ) -> Dict:
        # Implement the custom data test in respective layer
        pass

    @abstractmethod
    def execute_great_expectations(
        self,
        df_expected: DataFrame,
        df_observed: DataFrame,
        dict_result_custom_tests: Dict,
        kwargs: Dict = None,
    ) -> None:
        # Implement data tests
        pass

    @abstractmethod
    def execute_data_tests(
        self, 
        df_expected: DataFrame,
        df_expected_name: str,
        df_observed: DataFrame,
        df_observed_name: str,
        kwargs: Dict = None,
    ) -> Dict:
        # Implement data tests
        pass

    @staticmethod
    def generate_results_html(validation_results: ExpectationSuiteValidationResult) -> str:
        """
        ### Generate an HTML report from the validation result.
        
        #### Args:
            * validation_results (dict): The result of the data validation. 
            A JSON-formatted dict containing a list of the validation results.
        
        #### Returns:
            * str: HTML as str
        """
        validation_results = GreatExpectationsHelper().set_correct_status_gx_results(validation_results)
        render_content = ValidationResultsPageRenderer().render(validation_results)
        return DefaultJinjaPageView().render(render_content)

    @staticmethod
    def save_report_tests_azure_storage(
        html_result_gx: str,
        container_name: str,
        step_layer: str,
        schema_target_name: str,
        table_target_name: str,
    ) -> None:
        """
        ### Save the HTML report to Azure Storage.

        #### Args:
            * step_layer (str): The layers name. e.g.: `silver`
            * schema_target_name (str): The target schema name. e.g.: `csldw`
            * table_target_name (str): The target table name. e.g.: `dim_account`
            * html_result_gx (str): The HTML report as a string.
            * container_name (str): e.g.: `quality-assurance`
        """
        azure_conn = AzureADSL()
        adsl_path = azure_conn.generate_adsl_path(
            step_layers=step_layer, 
            schema_target_name=schema_target_name, 
            table_target_name=table_target_name,
        )
        azure_conn.save_report_tests(
            adsl_path=adsl_path,
            container_name=container_name,
            html_result_gx=html_result_gx,
        )

    @staticmethod
    def display_results(validation_results: Dict) -> str:
        """
        ### Generate a report

        #### Args:
            * validation_results (Dict): The result of the data validation.

        #### Returns:
            * str: The generated report as a string.
            e.g.:
                ################### GREAT EXPECTATIONS FAILED! ##################
                expect_column_values_to_not_be_null                          True
                expect_column_unique_value_count_to_be_between               False
                expect_column_same_precision                                 True
                expect_same_content_rows                                     False
                expect_column_same_datetime_precision                        True
                #################################################################
        """
        gx_helper = GreatExpectationsHelper()
        list_warnings = gx_helper.get_warning_msg_gx_results(validation_results)
        dict_filtered_results = gx_helper.get_dict_gx_result(validation_results)

        # e.g. expect_column_have_same_count_distinct   False
        return gx_helper.generate_orchestrator_report(validation_results, dict_filtered_results, list_warnings)
