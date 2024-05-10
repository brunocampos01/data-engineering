from typing import Union

from great_expectations.core import ExpectationSuiteValidationResult, ExpectationValidationResult


def add_custom_result_to_validation(
    result: ExpectationValidationResult,
    validation: Union[ExpectationValidationResult, ExpectationSuiteValidationResult],
) -> Union[ExpectationValidationResult, ExpectationSuiteValidationResult]:
    """Function to add custom result in the actual validation

    Args:
        result (ExpectationValidationResult): expectation Result to be added
        validation (Union[ExpectationValidationResult, ExpectationSuiteValidationResult]): Expectation and Results to be considered

    Returns:
        Union[ExpectationValidationResult, ExpectationSuiteValidationResult]: Expectation and Results with the adition of the custom result
    """

    validation.results.append(result)

    current_evaluated_expec = validation["statistics"]["evaluated_expectations"]
    current_successful_exp = validation["statistics"]["successful_expectations"]
    current_unsuccessful_exp = validation["statistics"]["unsuccessful_expectations"]

    validation["statistics"]["evaluated_expectations"] = current_evaluated_expec + 1
    if result.success:
        validation["statistics"]["successful_expectations"] = current_successful_exp + 1
    else:
        validation["statistics"]["unsuccessful_expectations"] = current_unsuccessful_exp + 1

    success_percent = (
        float(validation["statistics"]["successful_expectations"])
        / float(validation["statistics"]["evaluated_expectations"])
        * 100.0
    )
    validation["statistics"]["success_percent"] = success_percent
    return validation
