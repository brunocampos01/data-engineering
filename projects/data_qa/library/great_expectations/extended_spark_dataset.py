from abc import ABC

from great_expectations.core import ExpectationValidationResult
from great_expectations.data_asset.util import DocInherit
from great_expectations.dataset import SparkDFDataset


class ExtendedSparkDataset(SparkDFDataset, ABC):
    @DocInherit
    def expect_all_rows_match_dataframe(
        self,
        column_list,
        df,
        result_format="COMPLETE",
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):
        """Custom Expectation to evalute with all rows from one dataset is included in the other one.

        Args:
            column_list (): List of the columns to be evaluated
            df (): Dataframe to be evaluated
            result_format (str, optional): Format of how to diplay the result. Defaults to "COMPLETE".
            include_config (bool, optional): Option to include config. Defaults to True.
            catch_exceptions (bool, optional): Flag to capture exceptions. Defaults to None.
            meta (dict, optional): Dict with meta content for the expectation. Defaults to None.
        """

        this_df = self.spark_df.select(*column_list)
        truth_df = df.select(*column_list)
        missing_count = truth_df.subtract(this_df).count()
        unexpected_count = this_df.subtract(truth_df).count()
        success = missing_count == 0 and unexpected_count == 0

        result = ExpectationValidationResult(
            success=success,
            result={
                "observed_value": {
                    "row_count": this_df.count(),
                    "expected_count": df.count(),
                    "missing_count": missing_count,
                    "unexpected_count": unexpected_count,
                },
                "meta": meta,
            },
        )
