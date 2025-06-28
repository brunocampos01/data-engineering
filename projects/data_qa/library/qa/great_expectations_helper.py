from typing import Dict, List


class GreatExpectationsHelper:
    """
    A helper class for managing Great Expectations tests
    """
    @staticmethod
    def get_dict_gx_result(validation_results: Dict) -> Dict:
        """
        ### Get the validation_results and return a filtered dict

        #### Args:
            * validation_results (dict): The result of the data validation.

        #### Returns:
            * dict: splited results, example: "'expect_column_values_to_not_be_null': 'True'"
        """
        dict_filtered_results = {}
        for dict_result in validation_results["results"]:
            key = dict_result['expectation_config']['expectation_type']
            value = dict_result['success']
            if key in dict_filtered_results:
                if value == False:
                    dict_filtered_results[key] = value
            else:
                dict_filtered_results[key] = value
        return dict_filtered_results

    @staticmethod
    def get_results_from_expectation_type(validation_results: Dict, expectation_type: str) -> Dict:
        """
        ### Get the validation_results and return a filtered dict

        #### Args:
            * expectation_type (str): e.g.: expect_same_content_rows
            * validation_results (Dict): The result of the data validation.

        #### Returns:
            * List: splited results, example: [('date_of_death', True), ('pillar_name', True), ...]
        """
        list_success_and_column = []
        for i in validation_results["results"]:
            if expectation_type == i["expectation_config"]["expectation_type"]:
                success = i["success"]
                column = i["expectation_config"]["kwargs"].get("column")
                list_success_and_column.append((expectation_type, column, success))

        return list_success_and_column

    @staticmethod
    def set_correct_status_gx_results(validation_results: Dict) -> Dict:
        """
        ### Analyze if the some data test not passed and then update the ge result
        
        #### Args:
            * validation_results (Dict): The result of the data validation.
        
        #### Returns:
            * Dict: The validation_results updated
        """
        dict_filtered_results = GreatExpectationsHelper().get_dict_gx_result(validation_results)
        if False in list(dict_filtered_results.values()):
            validation_results["success"] = False
        
        return validation_results

    @staticmethod
    def get_warning_msg_gx_results(validation_results: Dict) -> List[str]:
        """Extracts warning messages from validation results.

        Args:
            validation_results (Dict): A dictionary containing validation results.

        Returns:
            List[str]: A list containing warning messages extracted from the validation results.
            e.g.: ['WARNING: Not found the fk in csldw.dw.dim_vendor_sites...', ...]
        """
        list_warn = []
        for data_test in validation_results["results"]:
            if "msg" in data_test["expectation_config"]["kwargs"]:
                msg = data_test["expectation_config"]["kwargs"]["msg"]
                if msg.startswith('WARNING'):
                    name_data_test = data_test["expectation_config"]['expectation_type']
                    list_warn.append(msg)
        return list_warn

    @staticmethod
    def generate_orchestrator_report(validation_results: Dict, dict_filtered_results: Dict, list_warnings: List[str] = None) -> str:
        """
        Generate an orchestrator report based on validation and filtered results.
        e.g.:       
        ################### GREAT EXPECTATIONS FAILED! ##################
        expect_column_have_fk_constraint                            True
        expect_column_values_to_not_be_null                         True
        expect_have_same_total_nulls                                False
        #################################################################

        ############################ WARNINGS ###########################
        WARNING: Not found the fk in csldw.dw.dim_vendor_sites. ...
        WARNING: Not found the fk in csldw.dw.dim_vendor_sites. ...
        #################################################################

        Args:
            validation_results (Dict): Dictionary containing validation results.
            dict_filtered_results (Dict): Dictionary containing filtered results.
            list_warnings (List[str], optional): List of warning messages. Defaults to None.

        Returns:
            str: Orchestrator report string.
        """
        padding = 60
        gx_results = sorted([
            (k.ljust(padding) + str(v)) 
            for k, v in dict_filtered_results.items()
        ])
        # Determine the report header based on successful or failed expectations
        if validation_results["statistics"].get("unsuccessful_expectations", 0) == 0:
            report_header = " GREAT EXPECTATIONS SUCCEEDED! "
        else:
            report_header = " GREAT EXPECTATIONS FAILED! "

        if len(list_warnings) > 0:
            return (
                report_header.center(padding + 5, "#") 
                + "\n"
                + "\n".join([x for x in gx_results])
                + "\n" + "#" * (padding + 5)
                + "\n\n" + ' WARNINGS '.center(padding + 5, "#") 
                + "\n"
                + "\n".join([x for x in list_warnings])
                + "\n" + "#" * (padding + 5)
            )
        else:
            return (
                report_header.center(padding + 5, "#") 
                + "\n"
                + "\n".join([x for x in gx_results])
                + "\n" + "#" * (padding + 5)
            )
