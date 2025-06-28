import os
from typing import Dict

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog
from data_catalog.src.loader.delta_table_loader import DeltaTableLoader


class BaseCreator(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str, owner: str):
        super().__init__(spark, layer_name)
        self.env = os.getenv("Environment").lower()
        self.owner = owner

    def _load_delta_table(self, table_name: str) -> DataFrame:
        return DeltaTableLoader(
            spark=self.spark,
            layer_name=self.layer_name,
            container_name=self.layer_name,
            folder_name=self.folder_name,
        ).execute(table_name)

    @staticmethod
    def get_dict_comment_cols() -> Dict:
        """
        Returns a dict containing {column_name: comment}.
        """
        return {
            "layer": "A layer refers to the organization of data in the Data Lake according to the structure of the Medallion Architecture used by CSL such as: Bronze, Silver and Gold Layer.",
            "layer_raw": "Original name in Unity Catalog.",
            "source": "A source refers to the origin or location from which data is obtained. It can be one or more database, a file, an application, or any other system that generates or stores data.",
            "source_raw": "Original name in Unity Catalog.",
            "table": "The table to which the field belongs",
            "field": "A field refers to a specific piece of information within a dataset. It represents a single category of data that is typically arranged in columns in a database or spreadsheet. Each field holds a specific type of data, such as a name, date, or numerical value.",
            "table_id": "Natural key. An unique representation of table in catalog.",
            "field_id": "Natural key. An unique representation of field in catalog.",
            "sources_table_id": "Natural key. An unique representation of field in catalog.",
            "field_description": "Provides a concise explanation of the data field, including its context, significance, and any relevant additional details.",
            "table_description": "Provides a concise explanation of the data table, including its context, significance, and any relevant additional details.",
            "source_description": "Provides a concise explanation of the data source, including its context, significance, and any relevant additional details.",
            "layer_description": "Provides a concise explanation of the data layer, including its context, significance, and any relevant additional details.",
            "tag_table_data_steward": "The steward of the table.",
            "tag_table_last_data_steward": "The previous steward of the table.",
            "data_type": "Classifies the nature of the data, such as numerical, textual, or categorical.",
            "obj_type": "Identify if the object in UC is a TABLE or VIEW.",
            "data_steward": "The steward of the data.",
            "department": "The area of the data steward.",
            "email": "The e-mail of the data steward.",
            "phone_number": "The phone of the data steward.",
            "source_created_in_uc_at": "The date when the object was created in Unity Catalog.",
            "table_created_in_uc_at": "The date when the object was created in Unity Catalog.",
            "field_created_in_uc_at": "The date when the object was created in Unity Catalog.",
            "source_last_updated_at": "The last update data or schema of the database.",
            "table_last_updated_at": "The last update data or schema of the table.",
            "table_last_data_updated_at": "The last update data of the table.",
            "table_last_schema_updated_at": "The last update schema of the table.",
            "field_last_updated_at": "The last update data or schema of the column.",
            "tag_table_type_ingestion": "It indicates the type of the ingestion of the data into the Data Catalog. It can be incremental and full.",
            "tag_table_frequency_ingestion": "It indicates the frequency to which the data is ingested into the Data Catalog.",
            "tag_field_data_element": "Field friendily name.",
            "tag_field_sensitive_level": "Indicates whether the data contains sensitive or confidential information",
            "tag_field_imo_data_number": "The corresponding field number in IMO Compendium.",
            "tag_field_source_of_truth": "Indicates if the data is shared with IMO/ Indicates if the data is the source of truth.",
            "tag_source_active_system": "It indicates if it is an active system.",
            "tag_source_csl_internal_system": "It indicates if it is an internal CSL system.",
            "year_month": "The period to aggregate.",
            "sk_layer": "The numeric artificial key is used to identify the row.",
            "sk_source": "The numeric artificial key is used to identify the row.",
            "sk_table": "The numeric artificial key is used to identify the row.",
            "sk_field": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_business_relevant": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_category": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_data_usage": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_field_type": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_is_certified": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_is_derived": "The numeric artificial key is used to identify the row.",
            "sk_tag_field_sensitive_level": "The numeric artificial key is used to identify the row.",
            "sk_tag_table_frequency_ingestion": "The numeric artificial key is used to identify the row.",
            "sk_tag_table_type_ingestion": "The numeric artificial key is used to identify the row.",
            "pk_fact_source": "Primay key.",
            "pk_fact_table": "Primay key.",
            "pk_fact_field": "Primay key.",
            "qty_field": "Calculate total fields by row.",
            "qty_field_document": "Number of blank spaces to be fill in. e.g.: tag_field_source_of_truth + tag_field_data_element + tag_field_imo_data_number = 3",
            "qty_field_documented": "Number of blank spaces filled in field.",
            "qty_table": "Calculate total tables by row.",
            "qty_table_document": "Number of blank spaces to be fill in table.",
            "qty_table_documented": "Number of blank spaces filled in table.",
            "qty_table_document_total": "Number of blank spaces to be fill in (fields + table).",
            "qty_table_documented_total": "Number of blank spaces filled in (fields + table).",
            "qty_source": "Calculate total sources by row.",
            "qty_source_document": "Number of blank spaces to be fill in source.",
            "qty_source_documented": "Number of blank spaces filled in source.",
            "qty_source_document_total": "Number of blank spaces to be fill in (fields + tables + source).",
            "qty_source_documented_total": "Number of blank spaces filled in (fields + tables + source).",
            "total_cols_by_source": "Count occurrences of sk_field within each partition defined by sk_source.",
            "total_cols_by_table": "Count occurrences of sk_field within each partition defined by sk_table.",
            "total_tables_by_source": "Count occurrences of sk_table within each partition defined by sk_source.",
            "total_cols_by_source_by_data_steward": "Count occurrences of sk_field within each partition defined by sk_source and tag_table_data_steward.",
            "total_tables_by_source_by_data_steward": "Count occurrences of sk_table within each partition defined by sk_source and tag_table_data_steward.",
            "new_cols_by_table": "`total_cols_by_table` - `lag_total_cols`",
            "new_cols_by_source": "`total_cols_by_source` - `lag_total_cols`",
            "new_tables_by_source": "`total_tables_by_source` - `lag_total_tables`",
            "new_document": "`qty_source_document_total` - `lag_document`",
            "new_documented": "`qty_source_documented_total` - `lag_documented`",
            "lag_total_cols": "`LAG(total_cols_by_table) OVER(PARTITION BY sk_layer, sk_source, sk_table ORDER BY year_month)`",
            "lag_total_tables": "`LAG(total_tables_by_source) OVER(PARTITION BY sk_source ORDER BY year_month)`",
            "lag_document": "`LAG(qty_source_document_total) OVER(PARTITION BY sk_source ORDER BY year_month)`",
            "lag_documented": "`LAG(qty_source_documented_total) OVER(PARTITION BY sk_source ORDER BY year_month)`",
        }

    @staticmethod
    def get_dict_bronze_table_pk() -> Dict:
        """
        Returns a dict containing {table_name: primary_key} in a bronze tables.
        """
        return {
            "sources": ["source", "source_last_updated_at"],
            "tables": ["table_id", "table_last_updated_at"],
            "fields": ["field_id", "field_last_updated_at"],
            "layers": "layer",
            "data_stewards": "data_steward",
        }

    @staticmethod
    def get_dict_silver_table_pk() -> Dict:
        """
        Returns a dict containing {table_name: primary_key} in a silver tables.
        """
        return {
            "sources": "source",
            "tables": "table_id",
            "fields": "field_id",
            "layers": "layer",
            "data_stewards": "data_steward",
            "sources_tables": "sources_table_id",
            "fields_data_usage": ["field_id", "field_data_usage"],
        }

    @staticmethod
    def get_dict_gold_fact_pk() -> Dict:
        """
        Returns a dict containing {table_name: primary_key} in a gold tables.
        """
        return {
            "fact_sources": "pk_fact_source",
            "fact_tables": "pk_fact_table",
            "fact_fields": "pk_fact_field",
            "fact_tables_agg": ["year_month", "sk_layer", "sk_source", "sk_table"],
            "fact_sources_agg": ["year_month", "sk_source", "data_steward"],
        }
