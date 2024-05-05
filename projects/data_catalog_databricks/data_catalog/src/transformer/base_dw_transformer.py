from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    when,
    col,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
)

from data_catalog.src.transformer.base_transformer import BaseTransformer


class DWTransformer(BaseTransformer):
    """
    This class contains methods that can be use in dimension and fact transformations
    
    Tech decision for tags_not_dim:
        Defined which tags will not are a dimension but just an attribute.
        sources will not generate new dims from your tags.
    """

    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def get_list_sources_tags_not_dim() -> List[str]:
        return ['tag_source_active_system', 'tag_source_csl_internal_system']

    @staticmethod
    def get_list_tables_tags_not_dim() -> List[str]:
        return ['tag_table_data_steward', 'tag_table_last_data_steward', 'tag_table_source_system']

    @staticmethod
    def get_list_fields_tags_not_dim() -> List[str]:
        return ['tag_field_imo_data_number', 'tag_field_source_of_truth', 'tag_field_data_element']

    def get_list_all_fields_tags_not_dim(self) -> List[str]:
        return self.get_list_fields_tags_not_dim()

    def get_list_all_tables_tags_not_dim(self) -> List[str]:
        list_all_values = []
        list_all_values.extend(self.get_list_fields_tags_not_dim())
        list_all_values.extend(self.get_list_tables_tags_not_dim())
        return list_all_values

    def get_list_all_sources_tags_not_dim(self) -> List[str]:
        list_all_values = []
        list_all_values.extend(self.get_list_fields_tags_not_dim())
        list_all_values.extend(self.get_list_tables_tags_not_dim())
        list_all_values.extend(self.get_list_sources_tags_not_dim())
        return list_all_values

    @staticmethod
    def get_list_cols_dim_layers():
        """
        Get a list of column names for the dimension sources.

        Returns:
            List[str]: List of column names for the dimension sources.
        """
        return ['layer', 'layer_description']

    @staticmethod
    def get_list_cols_dim_sources():
        """
        Get a list of column names for the dimension sources.

        Returns:
            List[str]: List of column names for the dimension sources.
        """
        return ['source', 'source_description',
                'source_created_in_uc_at', 'source_last_updated_at',
                'tag_source_active_system', 'tag_source_csl_internal_system']

    @staticmethod
    def get_list_cols_dim_sources():
        """
        Get a list of column names for the dimension sources.

        Returns:
            List[str]: List of column names for the dimension sources.
        """
        return ['source', 'source_description',
                'source_created_in_uc_at', 'source_last_updated_at',
                'tag_source_active_system', 'tag_source_csl_internal_system']

    @staticmethod
    def get_list_cols_dim_tables():
        """
        Get a list of column names for the dimension tables.

        Returns:
            List[str]: List of column names for the dimension tables.
        """
        return ['table_id', 'layer', 'source', 'table', 'obj_type', 'table_description',
                'table_created_in_uc_at', 'tag_table_last_data_steward',
                'table_last_data_updated_at', 'table_last_schema_updated_at',
                'tag_table_data_steward', 'tag_table_source_system']

    @staticmethod
    def get_list_cols_dim_fields():
        """
        Get a list of column names for the dimension fields.

        Returns:
            List[str]: List of column names for the dimension fields.
        
        Notes:
            Necessary to add name of table in dim_fields to facilitate the front end (PowerBI).
        """
        return ['field_id', 'layer', 'source', 'table', 'field', 'field_description', 'data_type',
                'field_created_in_uc_at', 'field_last_updated_at', 'tag_field_data_element',
                'tag_field_imo_data_number', 'tag_field_source_of_truth']

    @staticmethod
    def select_only_fact_cols(df: DataFrame, fact_name: str, list_not_dim: List[str] = []) -> DataFrame:
        """
        Select cols from a PySpark DataFrame that start with 'pk', 'sk', or 'qty'.

        Args:
            df (DataFrame): Input PySpark DataFrame.

        Returns:
            DataFrame: DataFrame with selected columns.
        """
        selected_cols = [c for c in df.columns
                         if c.startswith('pk')
                         or c.startswith('sk')
                         or c.startswith('qty')
                         or c.startswith('total')]

        if fact_name == 'table':
            selected_cols = [c for c in selected_cols if c not in ['qty_field_documented']]

        elif fact_name == 'field':
            selected_cols = [c for c in selected_cols if c not in ['qty_table_documented']]

        return df.select(*selected_cols)

    @staticmethod
    def apply_default_values_in_fact_tables(df: DataFrame, fact_name: str) -> DataFrame:
        df = df.na.fill(-1)
        return df \
            .withColumn(f"pk_fact_{fact_name}", col(f"pk_fact_{fact_name}").cast(IntegerType()))

    @staticmethod
    def apply_default_values_in_dim(df: DataFrame, dim_name: str) -> DataFrame:
        default_values = {f"{dim_name}": "not_filled", f"sk_{dim_name}": -1}
        df = df \
            .withColumn(f"{dim_name}",
                        when(col(f"{dim_name}") == "", default_values[f"{dim_name}"])
                        .otherwise(col(f"{dim_name}"))
                        ) \
            .withColumn(f"sk_{dim_name}",
                        when(col(f"sk_{dim_name}") == "", default_values[f"sk_{dim_name}"])
                        .otherwise(col(f"sk_{dim_name}"))
                        ) \
            .withColumn(f"sk_{dim_name}", col(f"sk_{dim_name}").cast(IntegerType()))

        for c in df.columns:
            if isinstance(df.schema[c].dataType, StringType):
                df = df.withColumn(c, when(col(c) == '', 'not_filled').otherwise(col(c)))

        return df

    def execute(self):
        pass
