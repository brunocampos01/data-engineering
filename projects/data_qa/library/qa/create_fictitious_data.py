import os
from typing import Dict, List, Any
from datetime import datetime

import pyspark.pandas as pd

from pyspark.sql.types import StructType, ArrayType, StructField, DataType
from pyspark.sql import (
    DataFrame,
    SparkSession,
    Row,
)
from pyspark.sql.types import StructType, ArrayType, StructField, DataType, StringType
from pyspark.sql.functions import col, regexp_replace, split
from delta.tables import DeltaTable

from library.sensitive import encrypt_columns
from library.datacleaner.defaults import Defaults
from library.logger_provider import LoggerProvider
from library.qa.upsert_helper import UpsertHelper
from library.qa.utils import row_to_dict


class CreateFictitiousData:
    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        table_name: str,
    ):
        self.spark = spark
        self.logger = LoggerProvider.get_logger()
        self.env = os.getenv("Environment").lower()
        self.catalog_name = f'{self.env}_{catalog_name}'
        self.schema_name = schema_name
        self.table_name = table_name
        self.table_full_name = f'{self.catalog_name}.{self.schema_name}.{self.table_name}'.strip().lower()

    def __repr__(self) -> str:
        return self.table_full_name

    def __get_list_cols_should_be_null(self, set_encrypted_cols, query: str) -> List[str]:
        df = self.spark.sql(query)

        list_cols_must_be_null = []
        for c in set_encrypted_cols:
            for field in df.select(c).schema.fields:
                if isinstance(field.dataType, ArrayType):
                    if df.select(c).filter(col(c)[0].isNull()).count() == 1:
                        list_cols_must_be_null.append(c)
                else:
                    if df.select(c).filter(col(c).isNull()).count() == 1:
                        list_cols_must_be_null.append(c)
            
        return list_cols_must_be_null

    def validate_input_data(self, list_encrypted_cols: List[str], list_cols_to_encrypt: List[str], query: str) -> None:
        """
        Validates the input data for encryption.

        This method performs two main checks:
        1. Ensures all cols in `list_cols_to_encrypt` are present in `list_encrypted_cols`.
        2. Ensures no cols that should be null (based on the query) are included in `list_cols_to_encrypt`.

        Args:
            list_encrypted_cols (List[str]): A list of column names that are encrypted.
            list_cols_to_encrypt (List[str]): A list of column names that are to be encrypted.
                e.g: ['addresses.addressline1', emails.emailaddress', ... ]
            query (str): The SQL query to determine which columns should be null.
        """
        set_encrypted_cols = set(list_encrypted_cols)
        set_cols_to_encrypt = set(list_cols_to_encrypt)

        # check if all cols added in list_cols_to_encrypt exists in list_encrypted_cols
        list_diff_cols = list(set_cols_to_encrypt - set_encrypted_cols)
        if len(list_diff_cols) > 0:
            raise Exception(f'Not found these cols in list_encrypted_cols: {list_diff_cols}. Check if these cols exists in source table.')

        # check if someone col was forgot in json (people)
        list_cols_must_be_null = self.__get_list_cols_should_be_null(set(list_encrypted_cols), query)
        list_mismatch_cols = set_cols_to_encrypt.intersection(set(list_cols_must_be_null))

    def generate_empty_dict(self) -> Dict[str, Any]:
        """
        Generate an empty nested dictionary reflecting the schema of the given Spark table.
        """
        schema = self.spark.read.table(self.table_full_name).schema

        def generate_nested_dict(schema: DataType, prefix: str = "") -> Dict[str, Any]:
            result_dict = {}
            if isinstance(schema, StructType):
                for field in schema.fields:
                    result_dict[field.name] = generate_nested_dict(field.dataType, prefix=prefix + field.name + ".")
            elif isinstance(schema, ArrayType):
                result_dict["element"] = generate_nested_dict(schema.elementType, prefix=prefix)
            else:
                result_dict = []
            return result_dict
        
        return generate_nested_dict(schema)

    def get_str_values_in_dict(self, data, parent_key = '') -> List[str]:
        """
        Recursively extracts keys from a nested dict or list whose values are strings.

        Args:
            data (Union[dict, list]): The nested dictionary or list to extract string values from.
            e.g.:
                {
                    'personid': 911000024443113,
                    'addresses': [{'addressid': 911000024443120,
                                'addressline1': '1234 Str', ...
                }
            parent_key (str, optional): The base key to prepend to each key path. Defaults to ''.

        Returns:
            List[str]: A list of keys with string values. e.g.: ['addresses.addressline1', ... ]
            ... ]
        """
        strings_list = []

        if isinstance(data, dict):
            for key, value in data.items():
                full_key = f"{parent_key}.{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    strings_list.extend(self.get_str_values_in_dict(value, full_key))
                elif isinstance(value, str):
                    strings_list.append(full_key)
        
        elif isinstance(data, list):
            for index, item in enumerate(data):
                full_key = f"{parent_key}"
                strings_list.extend(self.get_str_values_in_dict(item, full_key))

        return strings_list

    @staticmethod
    def populate_dict_silver(df: DataFrame) -> Dict:
        """Populates a dict with the first value of specified columns from a DataFrame.
        
        Return:
            Dict with the original values. e.g,:
            {
                'personnumber': '22',
                'regionofbirth': None,
                'townofbirth': None,
                'addresses': [{'addladdressattribute1': None,
                'addladdressattribute2': hgtdkjr==,
                ...
            }
        """
        dict_silver = {}
        for c in df.columns:
            data_type = df.schema[c].dataType
            if isinstance(data_type, ArrayType):
                dict_silver[c] = [row_to_dict(row) for row in df.select(c).first()[0]]
            else:
                dict_silver[c] = df.select(c).first()[0]

        return dict_silver

    def prepare_data_to_merge(self, dict_silver: Dict, query: str, to_encrypt: bool = False, list_cols_to_encrypt: List[str] = None) -> DataFrame:
        df = self.spark.createDataFrame([Row(**dict_silver)], schema=self.spark.sql(query).schema)
        if to_encrypt:
            self.logger.info(f'Encrypting these cols: {list_cols_to_encrypt}')
            df = df.transform(encrypt_columns, [*set(list_cols_to_encrypt)])

        return df

    def execute_merge(self, list_data_expected: List[Dict]) -> None:
        query = f"select * from {self.table_full_name}"

        total_rows = len(list_data_expected)
        self.logger.info(f'Adding {total_rows} row(s) in {self.table_full_name}')
        for i in range(0, total_rows):
            df_tmp = self.spark.createDataFrame([Row(**list_data_expected[i])], schema=self.spark.sql(query).schema)
            self.logger.info(f'Merging this data:')
            df_tmp.display()
            UpsertHelper(self.spark, self.table_full_name).upsert_table(df_tmp)
