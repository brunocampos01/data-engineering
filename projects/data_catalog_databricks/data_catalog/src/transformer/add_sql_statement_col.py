from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    concat,
    concat_ws,
    lit,
    when,
    col,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog


class AddSQLStatementCol(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def _add_col_tag_statement_for_fields(df: DataFrame) -> DataFrame:
        """
        Return:
            +------------------ ... -----------------------------------------+--------------... +
            |tag_statement_raw  ...                                          |tag_statement ... |
            +------------------ ... -----------------------------------------+--------------... +
            |ALTER TABLE dev_si ... CreationTime SET TAGS ("")               |              ... |
            |ALTER TABLE dev_si ... Id SET TAGS ("")                         |              ... |
            |ALTER TABLE dev_si ... LastModifierUserId SET TAGS ed by User"")|ALTER TABLE de... |
        """
        # # e.g.: ALTER TABLE dev_silver.thor.abpsettings ALTER COLUMN LastModifierUserId 
        # SET TAGS (""is_derived" = "No", "data_element" = "Last Updated by User"")
        concat_expr = concat(
            lit('ALTER TABLE '), col('table_id_backtick'), 
            lit(' ALTER COLUMN '), col('field'), 
            lit(' SET TAGS ('), col('tags_aggregated'), lit(')')
        )
        return df \
            .withColumn("tag_statement_raw", concat_expr) \
            .withColumn("tag_statement", 
                        when(col('tags_aggregated') != '', col('tag_statement_raw')).otherwise(lit(None))
            )

    @staticmethod
    def _add_col_tag_statement_for_tables(df: DataFrame) -> DataFrame:
        """
        Return:
            +--------- ...+------------------ ... -----------------------------------------+--------------... +
            |table_id  ...|tag_statement_raw  ...                                          |tag_statement ... |
            +--------- ...+------------------ ... -----------------------------------------+--------------... +
            |dev_silve ...|ALTER TABLE dev_si ... CreationTime SET TAGS ("")               |              ... |
            |dev_silve ...|ALTER TABLE dev_si ... Id SET TAGS ("")                         |              ... |
            |dev_silve ...|ALTER TABLE dev_si ... LastModifierUserId SET TAGS ed by User"")|ALTER TABLE de... |
        """
        # e.g.: ALTER TABLE dev_silver.shipsure.ppmparticulartype SET TAGS
        #      ("frequency_ingestion" = "Daily", "type_ingestion" = "Full")
        concat_expr = concat(
            lit('ALTER TABLE '), col('table_id_backtick'), 
            lit(' SET TAGS ('), col('tags_aggregated'), lit(')')
        )
        return df \
            .withColumn("tag_statement_raw", concat_expr) \
            .withColumn("tag_statement", 
                        when(col('tags_aggregated') != '', col('tag_statement_raw')).otherwise(lit(None))
            )

    @staticmethod
    def _add_col_tags_aggregated(df: DataFrame, list_origin_tags_names) -> DataFrame:
        """
        Generates a formatted content str based on the selected row and a list of tags.

        Args:
            df (DataFrame): DataFrame from origin (Excel)
                example: df_origin
            list_origin_tags_names (List[str]): List of field tag names.
                    example: ['sensitive_info', 'business_relevant', ...]

        Return:
            DataFrame: DataFrame with the added content column.
            Example:
            +----------+--------+------------+------------------------------------------------+
            |is_derived|category|data_element|tags_aggregated                                 |
            +----------+--------+------------+------------------------------------------------+
            |NULL      |NULL    |NULL        |NULL                                            |
            |NULL      |NULL    |NULL        |NULL                                            |
            |No        |NULL    |Last Updated|"is_derived" = No, "data_element" = Last Updated|
        """
        concat_expr = concat_ws(", ",
            *[
                when(col(c).isNotNull(), 
                    concat_ws(" = ", 
                               lit(f'"{c}"'), concat(lit('"'), col(c).cast("string"), lit('"'))
                    )
                ).otherwise(None)
                for c in list_origin_tags_names
            ]
        )
        return df \
            .withColumn('tags_aggregated', concat_expr) \
            .withColumn('tags_aggregated', 
                        when(col('tags_aggregated') == '', None).otherwise(col('tags_aggregated')))

    @staticmethod
    def _add_col_description_statement_for_fields(df: DataFrame) -> DataFrame:
        """
        Return:
            +------------------ ... -----------------------------------------+
            |description_statet ...                                          |
            +------------------ ... -----------------------------------------+
            |null                                                            |
            |ALTER TABLE dev_e  ... COLUMN fuelType COMMENT "Type of fue... "|
        """
        # # e.g.: ALTER TABLE dev_silver.thor.abpsettings ALTER COLUMN LastModifierUserId 
        # COMMENT "The ID of the user who updated the table in the system"
        concat_expr = concat(
            lit('ALTER TABLE '), col('table_id_backtick'), 
            lit(' ALTER COLUMN '), col('field'), 
            lit(' COMMENT "'), col('field_description'), lit('"')
        )
        return df.withColumn("description_statement", concat_expr) 

    @staticmethod
    def _add_col_description_statement_for_tables(df: DataFrame) -> DataFrame:
        """
        Return:
            +--------- ...+------------------ ... -----------------------------------------+
            |table_id  ...|description_statet ...                                          |
            +--------- ...+------------------ ... -----------------------------------------+
            |dev_silve ...|null                                                            |
            |dev_silve ...|ALTER TABLE dev_e  ... COLUMN fuelType COMMENT "Type of fue... "|
        """
        concat_expr = concat(
            lit('COMMENT ON TABLE '), col('table_id_backtick'), 
            lit(' IS "'), col('table_description'), lit('"')
        )
        return df.withColumn("description_statement", concat_expr) 

    def execute(
        self, 
        df: DataFrame,
        list_origin_tags_names: List[str],
        table_name: str,
    ) -> DataFrame:
        df = self._add_col_tags_aggregated(df, list_origin_tags_names)

        if table_name == 'sources':
            return df

        elif table_name == 'tables':
            df = self._add_col_tag_statement_for_tables(df)
            return self._add_col_description_statement_for_tables(df)

        elif table_name == 'fields':
            df = self._add_col_tag_statement_for_fields(df)
            return self._add_col_description_statement_for_fields(df)
