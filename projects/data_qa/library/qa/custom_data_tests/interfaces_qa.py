from typing import (
    List, 
    Tuple,
)

from pyspark.sql import DataFrame

from library.qa.custom_data_tests.common_tests import CommonQA as custom_qa
from library.qa.utils import LogTag


class InterfacesQA:
    @staticmethod
    def check_same_content_rows(
        df_expected: DataFrame, 
        df_observed: DataFrame, 
        list_id: List[str],
        c: str,
    ) -> Tuple[bool, str]:
        return custom_qa.check_if_two_df_contain_the_same_rows(
                df_expected=df_expected.select(*list_id, c),
                df_expected_name='interfaces_testing',
                df_observed=df_observed.select(*list_id, c),
                df_observed_name='interfaces',
                tag=LogTag.ROW_CONTENT,
            )
