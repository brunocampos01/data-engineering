import json
import time
import datetime
from typing import (
    List, 
    Dict, 
    Tuple,
)

from great_expectations.core.expectation_suite import (
    ExpectationConfiguration,
    ExpectationSuite,
)
from great_expectations.dataset import SparkDFDataset
from great_expectations.expectations.expectation import ExpectationValidationResult
from pyspark.sql import (
    DataFrame, 
    SparkSession,
)
from pyspark.sql.functions import (
    col, 
    regexp_replace,
)

from library.great_expectations.util import add_custom_result_to_validation
from library.qa.utils import LogTag
from library.qa.utils import find_df_diff


class LiftShiftQA:
    def __generate_checksum_col(
        spark: SparkSession,
        table_name_dim: str,
        schema_name: str,
        catalog_name: str,
        env_name: str,
        list_deny_cols: List[str] = [],
    ) -> DataFrame:
        """
        ### Generate checksum column for a dimension table.

        #### Args:
            * spark (SparkSession): The Spark session.
            * table_name_dim (str): The name of the dimension table.
            * schema_name (str): The schema name of the table.
            * catalog_name (str): The catalog name of the table.
            * env_name (str): Environment. `[dev, test, prd]`
            * list_deny_cols (List): List of Deny columns.

        #### Returns:
            * DataFrame: DataFrame with checksum generated column
        """
        conn = AzureSQL() if env_name == 'azure' else SQLServerDWSql()
        
        if '.' in table_name_dim:
            schema_name, table_name = table_name_dim.split('.', 1)
        else:
            schema_name = schema_name
            table_name = table_name_dim

        df_dim = spark.read.format('jdbc') \
            .options(**conn.options(catalog_name)) \
            .option("query", f'SELECT * FROM {schema_name}.{table_name}') \
            .load()
        # Get the list of columns to keep
        cols_to_keep = [c for c in df_dim.columns 
                        if not c.startswith('ETL_') 
                            and not c.endswith('_SK')
                            and not c.endswith("DATE")
                            and not c.endswith("DT")
                            and c not in list_deny_cols]
        # remove unnecessary cols
        if len(cols_to_keep) > 0:
            df_dim = df_dim.select(*cols_to_keep).orderBy(*cols_to_keep)
        else:
            df_dim = df_dim.select(*cols_to_keep)
        
        # Concatenate the selected cols into a single string
        concatenated_col = concat_ws("", *[col(c) for c in cols_to_keep])
        # Calculate the checksum using SHA-256
        return df_dim.withColumn("CHECK_SUM", sha2(concatenated_col, 256))

    def check_if_checksum_are_equal(
        spark: SparkSession,
        table_name_dim: str,
        schema_name: str,
        catalog_name: str,
        list_dim_tables_name: List[str],
    ) -> Tuple[bool, str]:
        """
        ### Check if checksums are equal for a list of dimension tables.

        #### Args:
            * spark (SparkSession): The Spark session.
            * table_name_dim (str): The name of the dimension table.
            * schema_name (str): The schema name of the table.
            * catalog_name (str): The catalog name of the table.
            * list_dim_tables_name (list): List of dimension table names.

        #### Returns:
            * bool: boolean indicating whether the checksums are equal
            * str: message describing the result.
        """
        for table_name in list_dim_tables_name:
            df_dim_source = LiftShiftQA.__generate_checksum_col(
                spark=spark,
                schema_name=schema_name,
                table_name_dim=table_name_dim,
                catalog_name=catalog_name,
                env_name='azure',
            )
            df_dim_target = LiftShiftQA.__generate_checksum_col(
                spark=spark,
                schema_name=schema_name,
                table_name_dim=table_name_dim,
                catalog_name=catalog_name,
                env_name='onpremises',
            )
        df_diff_source_target, df_diff_target_source = find_df_diff(df_dim_source, df_dim_target)
        total_df_diff_src_trg = df_diff_source_target.count()
        total_df_diff_trg_src = df_diff_target_source.count()

        if total_df_diff_src_trg > 0 or total_df_diff_trg_src > 0:
            return False, f'The measures was tested with corresponding dims business key column(s) for each involved dimensions. ' \
                        f'Make sure that dimensions are tested first and pass the data tests. ' \
                        f'Checked for these dimesions table: {list_dim_tables_name}'
        else:
            return True, f'The measures was tested with corresponding dims business key column(s) for each involved dimensions. ' \
                        f'Checked for these dimesions table: {list_dim_tables_name}'
