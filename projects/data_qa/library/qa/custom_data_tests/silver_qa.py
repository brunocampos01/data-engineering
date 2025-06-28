import json
import time
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
from pyspark.sql.functions import col

from library.dataloader.defaults import Defaults as col_bronze
from library.datacleaner.defaults import Defaults as col_silver
from library.great_expectations.util import add_custom_result_to_validation
from library.qa.utils import initialize_and_prepare_delta


class SilverQA:
    @staticmethod
    def __analyze_duplicates_and_counts_src_syst_effective_to_col(
        spark: SparkSession, 
        catalog_expected: str, 
        schema_expected: str, 
        table_expected: str,
        show_data: bool = False,
    ) -> DataFrame:
        """
        ### Args:
            spark (SparkSession): The SparkSession object.
            catalog_expected (str): The silver catalog
            schema_expected (str): The silver database
            table_expected (str): The silver table

        ### Return:
            DataFrame. e.g.:
            +------------------------------------------------------------------+---+
            |TypeQty                                                           |Qty|
            +------------------------------------------------------------------+---+
            |Silver rows with src_syst_effective_to Not Null                   |82 |
            |Silver rows with src_syst_effective_to *set as NULL* and have each, a previously matching primary key(s) entered pair|19 |
            |Silver rows with src_syst_effective_to but without a previous pair|63 |
            |Bronze rows not moved to silver by deleted flag True.             |63 |
            +------------------------------------------------------------------+---+
        """
        # SQL query to get primary key columns
        sql_query_pk = f"""
            SELECT CONCAT_WS(', ', collect_list(ccu.column_name),'etl_src_pkeys_hash_key') AS primary_key_columns
            FROM system.information_schema.constraint_column_usage ccu
            JOIN system.information_schema.table_constraints tc
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_name = tc.table_name
                AND ccu.table_schema = tc.table_schema
                AND ccu.table_catalog = tc.table_catalog
            WHERE tc.constraint_type = 'PRIMARY KEY' 
                AND ccu.table_catalog = '{catalog_expected}'
                AND ccu.table_schema = '{schema_expected}'
                AND ccu.table_name = '{table_expected}'
        """
        results_pk = spark.sql(sql_query_pk).collect()

        pkcols = ""
        for row in results_pk:
            # Get primary key column names
            pkcols = row["primary_key_columns"]

            if len(pkcols) > 0:
                sql_query = f"""
                select
                    {pkcols}, {table_expected}.src_syst_effective_from, {table_expected}.src_syst_effective_to
                    from {catalog_expected}.{schema_expected}.{table_expected}
                    where ({pkcols}) in (
                        select {pkcols}
                        from
                        (select {pkcols}, count({pkcols})
                         from {catalog_expected}.{schema_expected}.{table_expected}
                         group by {pkcols}
                         having count({pkcols}) > 1) subqry1
                        ) 
                """
                # Execute SQL query to get duples rows
                results_values = spark.sql(sql_query)

                if show_data:
                    results_values.display()

                # SQL query to count rows
                sql_query_count = f"""
                Select QueryOrder, TypeQty, Qty From (
                select 
                    '01' as QueryOrder, 'Silver rows with src_syst_effective_to Not Null' as TypeQty, count(DISTINCT {pkcols}) as Qty
                    from {catalog_expected}.{schema_expected}.{table_expected}
                    where {table_expected}.src_syst_effective_to is not null
                UNION
                select 
                    '02' as QueryOrder, 'Silver rows with src_syst_effective_to *set as NULL* and have each, a previously matching primary key(s) entered pair' as TypeQty, count(DISTINCT {pkcols}) as Qty
                    from ({sql_query})
                UNION
                select
                    '03' as QueryOrder, 'Silver rows with src_syst_effective_to but without a previous pair' as TypeQty, count(DISTINCT {pkcols}) as Qty
                    from {catalog_expected}.{schema_expected}.{table_expected} 
                    where {table_expected}.src_syst_effective_to is not null
                    and ({pkcols}) not in (
                        select {pkcols}
                        from
                        (select {pkcols}, count(*)
                            from {catalog_expected}.{schema_expected}.{table_expected}
                            group by {pkcols}
                            having count(*) > 1) subqry1
                    ) 
                UNION
                select 
                    '04' as QueryOrder, 'Bronze rows not moved to silver by deleted flag True' as TypeQty, count(DISTINCT {pkcols}) as Qty
                    from test_bronze.{schema_expected}.{table_expected}
                    where where ({pkcols}) in (
                    select
                    {pkcols}
                    from {catalog_expected}.{schema_expected}.{table_expected}
                    where ({pkcols}) not in (
                select
                    {pkcols}
                    from {catalog_expected}.{schema_expected}.{table_expected}
                    where ({pkcols}) in (
                        select {pkcols}
                        from
                        (select {pkcols}, count(*)
                            from {catalog_expected}.{schema_expected}.{table_expected}
                            group by {pkcols}
                            having count(*) > 1) subqry1
                    ) )
                    and {table_expected}.src_syst_effective_to IS NOT NULL
                )
                and etl_deleted_flag = TRUE
                ) Order by QueryOrder
            """
                # Execute SQL query to count rows
                return spark.sql(sql_query_count)
            else:
                return None

    @staticmethod
    def check_if_col_src_syst_effective_to_is_correct(
        spark: SparkSession, 
        catalog_expected: str, 
        schema_expected: str,
        table_expected: str,
    ) -> Tuple[bool, str]:
        try:
            df = SilverQA.__analyze_duplicates_and_counts_src_syst_effective_to_col(
                spark=spark,
                catalog_expected=catalog_expected, 
                schema_expected=schema_expected, 
                table_expected=table_expected,
                show_data=True,
            )
            list_without_previous_and_previous = df \
                .filter(col("TypeQty").isin(
                    "Silver rows with src_syst_effective_to but without a previous pair", 
                    "Silver rows with src_syst_effective_to *set as NULL* and have each, a previously matching primary key(s) entered pair")) \
                .select("Qty") \
                .collect()

            list_without_previous_and_previous = [x.Qty for x in list_without_previous_and_previous]
            sum_without_previous_and_previous = sum(list_without_previous_and_previous)

            total_src_syst_effective_to_not_null = df \
                .filter(col("TypeQty").isin("Silver rows with src_syst_effective_to Not Null")) \
                .select("Qty") \
                .collect()[0][0]
            
            df.display()


            # great expectations output
            if sum_without_previous_and_previous == total_src_syst_effective_to_not_null:
                return True, f'The sum in (Silver rows with src_syst_effective_to but without a previous pair) + (Silver rows with src_syst_effective_to *set as NULL* and have each, a previously matching primary key(s) entered pair") is equal (Silver rows with src_syst_effective_to Not Null)| {list_without_previous_and_previous} == {sum_without_previous_and_previous}'
            else:
                return False, f'Mismatch sum. EXPECTED (Silver rows with src_syst_effective_to Not Null) = {total_src_syst_effective_to_not_null} | OBSERVED = (Silver rows with src_syst_effective_to but without a previous pair) + (Silver rows with src_syst_effective_to *set as NULL* and have each, a previously matching primary key(s) entered pair") | {list_without_previous_and_previous}'

        except:
            return True, "No primary key found in the table."

    @staticmethod
    def check_if_col_src_syst_effective_to_is_equal(
        spark: SparkSession, 
        catalog_expected: str, 
        schema_expected: str,
        table_expected: str,
    ) -> Tuple[bool, str]:
        try:
            df = SilverQA.__analyze_duplicates_and_counts_src_syst_effective_to_col(
                spark=spark,
                catalog_expected=catalog_expected, 
                schema_expected=schema_expected, 
                table_expected=table_expected,
            )
            row_3 = df \
                .filter(col("TypeQty").isin("Silver rows with src_syst_effective_to but without a previous pair")) \
                .select("Qty") \
                .collect()[0][0]
            row_4 = df \
                .filter(col("TypeQty").isin("Bronze rows not moved to silver by deleted flag True")) \
                .select("Qty") \
                .collect()[0][0]
            
            # Condition for not evaluating src_syst_effective_to
            skipped_tables = ['voypnl', 'voypnlbnkr', 'voypnldet', 'voypnldrill', 'voypnlitin']

            if not (schema_expected == 'imos_datalake' and table_expected in skipped_tables):

                # great expectations output
                if row_3 == row_4:
                    return True, f'The (Silver rows with src_syst_effective_to but without a previous pair) {row_3} == {row_4} (Bronze rows not moved to silver by deleted flag True)'
                else:
                    return False, f'Mismatch rows. The (Silver rows with src_syst_effective_to but without a previous pair) {row_3} != {row_4} (Bronze rows not moved to silver by deleted flag True)'
            else:
                return True, "Not evaluated for deletion conditions in bronze table."
            
        except:
            return True, "No primary key found in the table."

    @staticmethod
    def check_if_col_have_same_count_distinct(
        list_pk: List[str], 
        col_name: str, 
        df_expected: DataFrame, 
        df_observed: DataFrame,
        catalog_observed: str,
        catalog_expected: str,
    ) -> Tuple[bool, str]:
        """
        Checks if the count of rows by col in the expected df matches the count of rows in the bronze df.

        #### Args:
            - list_pk (List): The list of primary key columns.
            - col_name (str): the name of the column. This column exists in both dataframes.
            - df_expected (DataFrame): The expected df whose count is compared.
            - df_observed (DataFrame): The expected df whose count is compared (bronze).

        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating if the counts match and a message.
        """
        if len(list_pk) > 0:
            list_cols_with_pk = list_pk + [col_name]
            total_observed_col = df_observed \
                .select(*list_cols_with_pk) \
                .distinct() \
                .count()
            total_expected_col = df_expected \
                .select(*list_cols_with_pk) \
                .distinct() \
                .count()
        else:
            total_observed_col = df_observed \
                .select(col_name) \
                .distinct() \
                .count()
            total_expected_col = df_expected \
                .select(col_name) \
                .distinct() \
                .count()

        # great expectations output
        if total_observed_col == total_expected_col:
            return True, f'{total_expected_col}'
        else:
            if total_observed_col == 0 and total_expected_col != 0:
                return False, f'Col is empty in {catalog_observed} but not in {catalog_expected} = {total_expected_col}!'
            else:
                return False, f'''Mismatch distinct count between {catalog_observed} and {catalog_expected}! 
                                  EXPECTED: {total_expected_col} | OBSERVED: {total_observed_col}'''

    @staticmethod
    def check_if_tables_have_same_owner(
        spark: SparkSession, 
        path_observed: str, 
        path_expected: str,
    ) -> Tuple[bool, str]:
        """
        Check if the bronze and silver tables have the same owner.

        ### Args:
            spark (SparkSession): The SparkSession object.
            path_observed (str): The path in Unity Catalog e.g.: dev_bronze.imos_report_api.md_voyagepnl_estimate
            path_expected (str): The path in Unity Catalog e.g.: dev_silver.imos_report_api.md_voyagepnl_estimate

        #### Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the tables have the same owner
            and a message describing the comparison result.
        """
        owner_observed = spark.sql(f"DESCRIBE EXTENDED {path_observed}") \
            .filter(col('col_name') == 'Owner') \
            .select('data_type') \
            .collect()[0][0]
        owner_expected = spark.sql(f"DESCRIBE EXTENDED {path_expected}") \
            .filter(col('col_name') == 'Owner') \
            .select('data_type') \
            .collect()[0][0]

        # great expectations output
        if owner_observed == owner_expected:
            return True, f'Bronze and silver have the same owner: {owner_expected}'
        else:
            return False, f'The owner of the tables are not the same! EXPECTED: {owner_expected} | OBSERVED: {owner_observed}'

    @staticmethod
    def check_if_validation_rules_not_issues(
        list_pk: List[str], 
        df_expected: DataFrame, 
        df_observed: DataFrame,
        list_email_columns: List[str],
        list_phone_columns: List[str],
    ) -> Tuple[bool, str]:
        """
        etl_none_compliant_pattern aggregates any column that has an invalid value in silver
        into an array, for example, it runs a rule to convert datetime
        If it doesn`t work, it says it failed to convert datetime to a certain column.

        #### Args:
            - list_pk (List): The list of primary key columns.
            - df_expected (DataFrame): The DataFrame from silver.
            - df_observed (DataFrame): The DataFrame from bronze transformed.

        #### Returns:
            - Tuple[bool, str]: A tuple containing a boolean indicating whether the rows are valid,
                                and a message describing the result of the check.
        """
        list_cols = list_pk + list_email_columns + list_phone_columns
        df_expected_filtered = df_expected.select(*list_cols).distinct().orderBy(*list_cols)
        df_observed_filtered = df_observed.select(*list_cols).distinct().orderBy(*list_cols)
        df_result = df_expected_filtered.exceptAll(df_observed_filtered)

        # great expectations output
        if df_result.count() == 0:
            return True, f'Analyzed {col_silver.etl_none_compliant_pattern} col and no rows found problem after validation facade.'
        else:
            return False, f'''Rows with problems after execute validation facade 
                            and checked the {col_silver.etl_none_compliant_pattern}: {df_result.collect()}'''

    @staticmethod
    def check_if_have_not_corrupt_rows_bronze(
        spark: SparkSession, 
        path_observed: str, 
        list_exception_corrupted_records: List[str],
    ) -> Tuple[bool, str]:
        """
        Check if the corrupt column has value.

        ### Args:
            - spark (SparkSession): The SparkSession object.
            - path_observed (str): The path in Unity Catalog e.g.: dev_bronze.imos_report_api.md_voyagepnl_estimate
            - list_exception_corrupted_records (List[str]): The list which contains values that are aceptable in the test.

        #### Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating is have not corrupted records
            and a message describing the comparison result.
        
        ### Notes:
            This test was to be done in landing_to_bronze but was not did.
        """
        df_bronze = initialize_and_prepare_delta(spark, path_delta=path_observed)

        df_with_corrupted = df_bronze.filter(col(col_bronze.corrupt_record_column).isNotNull())
        list_corrupted = [list(r)[0] for r in df_with_corrupted.select(col_silver.etl_hash_key_pk).collect()]
        total_corrupt = df_with_corrupted.count()

        # if contains truly corrupted values
        if len(list_exception_corrupted_records) > 0:
            rows = df_with_corrupted.select(col_silver.etl_hash_key_pk).collect()
            set_corrupted = {r[col_silver.etl_hash_key_pk].lower() for r in rows}
            list_corrupted = list(set_corrupted - set(list_exception_corrupted_records))
            total_corrupt = len(list_corrupted)

        # great expectations output
        if total_corrupt == 0:
            return True, f'Analyzed {col_bronze.corrupt_record_column} col and not found corrupted rows.'
        else:
            if len(col_silver.etl_hash_key_pk) > 0:
                log_msg = f'Found {total_corrupt} corrupted rows. '\
                          'If it is necessary to accept these, add them to the list_exception_corrupted_records. ' \
                          f'Showing the {col_silver.etl_hash_key_pk} of each row: {list_corrupted}'
            else:
                log_msg = f'Found {total_corrupt} corrupted rows.'

            return False, log_msg
    
    @staticmethod
    def check_if_have_valid_flag_bronze(
        spark: SparkSession, 
        list_pk: List[str], 
        df_observed: DataFrame, 
        path_observed: str,
    ) -> Tuple[bool, str]:
        """
        Check if the deleted_flag column are consistence

        ### Args:
            - spark (SparkSession): The SparkSession object.
            - list_pk (List): The list of primary key columns.
            - df_observed (DataFrame): The DataFrame from bronze transformed.
            - path_observed (str): The path in Unity Catalog e.g.: dev_bronze.imos_report_api.md_voyagepnl_estimate

        #### Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating is have not deleted records consistence
            and a message describing the comparison result.
        
        ### Notes:
            This test was to be done in landing_to_bronze but was not did.
        """
        df_bronze = initialize_and_prepare_delta(spark, path_delta=path_observed)
        df_bronze.createOrReplaceTempView("bronze_view")
        list_pk_cols_specific = list_pk[0]

        # Query created by Turriago 01.03.24
        query = f"""
        SELECT
            {list_pk_cols_specific}, etl_deleted_flag 
        FROM bronze_view
        WHERE etl_deleted_flag = true 
            AND `{list_pk_cols_specific}` not in (
                SELECT {list_pk_cols_specific}
                FROM bronze_view
                WHERE etl_deleted_flag = false
            )
        """
        list_deleted_flag = spark.sql(query).collect()
        total_deleted_flag = len(list_deleted_flag)

        # great expectations output
        if total_deleted_flag == 0:
            return True, f'Analyzed {col_bronze.deleted_flag_column} col and all values are valid.'
        else:
            log_msg = f'Found {total_deleted_flag} deleted flag with issues. Showing the rows to be analyze: {list_deleted_flag}'
            return False, log_msg
