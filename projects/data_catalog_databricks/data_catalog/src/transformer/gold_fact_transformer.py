from typing import Dict

from pyspark.sql import (
    DataFrame,
    SparkSession,
)

from data_catalog.src.transformer.add_dw_cols import AddDWCols


class GoldFactTransformer(AddDWCols):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    def __join_dims_tags_and_fact(self, df_base, fact_name, list_df_dim, list_join_names):
        """
        Execute a join for each dim in df_base
        """
        df_base.createOrReplaceTempView("base")

        for df_dim, df_name in zip(list_df_dim, list_join_names):
            df_dim.createOrReplaceTempView("dim_tag")
            df_base = self.spark.sql(f"""
                SELECT *
                FROM base a
                LEFT JOIN dim_tag b ON a.{df_name} = b.{df_name}
            """)
            df_base.createOrReplaceTempView("base")

        return df_base

    def execute(
        self,
        fact_name: str,
        df_base: DataFrame,
        dict_df_dim_tags: Dict,
    ) -> DataFrame:
        """
        Execute data transformation on the provided DataFrame based on
        the list of dimensions and join names.

        Args:
            fact_name (str): Name of the fact. Singular name.
            df_base (DataFrame): Base DataFrame already joined with default dimensions.
            dict_df_dim_tags (Dict): a map df_dim_name with the dataframe.
                e.g.: {'tag_table_data_steward': DataFrame[tag_table_data_: string...,}

        Returns:
            DataFrame: Transformed DataFrame representing the fact without metrics

        Tests (tmp):
            To validate if the fact_fields is correct:
            new_rows = df_fact_fields_tmp.filter(df_fact_fields_tmp['sk_table'].isNull())
            new_rows.display() # 0

            new_rows = df_silver_fields.filter(df_silver_fields['table'].isNull())
            new_rows.display() # 0
        """
        df_base = self.__join_dims_tags_and_fact(
            df_base=df_base,
            fact_name=fact_name,
            list_df_dim=dict_df_dim_tags.values(), 
            list_join_names=dict_df_dim_tags.keys(),
        )
        list_not_dim = getattr(self, f"get_list_{fact_name}s_tags_not_dim")()
        df_base = self.select_only_fact_cols(df_base, fact_name, list_not_dim)
        df_fact = self.add_pk_col(df_base, fact_name)
        df_fact = self.apply_default_values_in_fact_tables(df_fact, fact_name)

        return df_fact
