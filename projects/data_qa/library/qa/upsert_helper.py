import os
from typing import Dict, List
from datetime import datetime

import pyspark.pandas as pd

from pyspark.sql.types import StructType, ArrayType, StructField, DataType
from pyspark.sql import (
    DataFrame,
    SparkSession,
    Row,
)
from pyspark.sql.functions import col, regexp_replace, split
from delta.tables import DeltaTable

from library.datacleaner.defaults import Defaults


class UpsertHelper:
    def __init__(
        self,
        spark: SparkSession,
        table_full_name: str,
    ):
        self.spark = spark
        self.table_full_name = table_full_name

    def __get_primary_keys(self) -> List[str]:
        df = self.spark.sql(f'DESCRIBE EXTENDED {self.table_full_name}')\
            .filter("data_type like ('%PRIMARY KEY%')")\
            .select("data_type")

        df = df.withColumn('data_type', 
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(col('data_type'),'PRIMARY KEY ',''),'\(',""),'\)',""),'`',''))
        df = df.withColumn('prim_keys', split(df['data_type'], ','))
        if len(df.select('prim_keys').collect())> 0:
            prim_keys_list = df.select('prim_keys').collect()[0][0]
        else:
            prim_keys_list=[]
        
        return prim_keys_list

    @staticmethod
    def __create_merge_expression(business_key_columns: [str]) -> str:
        keys_expr = [f"existing.{x} = updates.{x}" for x in business_key_columns]
        return " and ".join(keys_expr)

    @staticmethod
    def __create_update_expression(business_key_columns: [str], update_df: DataFrame) -> str:
        non_key_columns = [
            col
            for col in update_df.columns
            if col not in business_key_columns
        ] 
        if len(non_key_columns) > 0:
            non_keys_expr = [
                f"coalesce(cast(existing.{x} as string),'') != coalesce(cast(updates.{x} as string), '')"
                for x in non_key_columns
            ]
        else:
            non_keys_expr = ['1=1']

        return " or ".join(non_keys_expr)

    @staticmethod
    def __create_matched_cols(update_df: DataFrame) -> Dict:
        cols_to_match = [col for col in update_df.columns]

        matched_cols = {}
        for col in cols_to_match:
            matched_cols.update({col:f"updates.{col}"})

        return matched_cols

    def upsert_table(self, df: DataFrame):
        schema = self.spark.read.table(self.table_full_name).schema
        key_columns = self.__get_primary_keys()

        if key_columns != []:
            merge_key_expression = self.__create_merge_expression(key_columns)
            update_expression = self.__create_update_expression(key_columns,df)
            matched_cols_dict = self.__create_matched_cols(df)
            delta_table = DeltaTable.forName(self.spark, self.table_full_name)

            delta_table.alias("existing")\
                .merge(source=df.alias("updates"), condition=merge_key_expression)\
                .whenMatchedUpdate(condition=update_expression, set={**matched_cols_dict})\
                .whenNotMatchedInsert(values={**matched_cols_dict})\
                .execute()
        else:
            df.write.format("delta").mode("append").insertInto(self.table_full_name)
