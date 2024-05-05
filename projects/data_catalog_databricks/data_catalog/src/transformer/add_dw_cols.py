from typing import List

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
    monotonically_increasing_id,
    when,
    sum,
    count,
    expr,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from data_catalog.src.transformer.base_dw_transformer import DWTransformer


class AddDWCols(DWTransformer):
    """
    This class contains columns that can be add in data warehouse model.
    Examples: primary_key column, surrogate_key column, measures.
    """
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def add_sk_col(df_dim: DataFrame, dim_name: str) -> DataFrame:
        """
        Adds a new column to the df with a unique id.
        If the specified dimension is null, the id is 0, 
        otherwise it's the monotonically increasing id + 1.
        The new column is sorted in ascending order.

        Args:
            df_dim (DataFrame): The DataFrame to which the new column will be added.
            dim_name (str): The name of the dimension.

        Returns:
            DataFrame: The DataFrame with the new column added.

        Notes:
            + 1 starts the id with 1 instaed of 0
        """
        df_dim = df_dim.orderBy(dim_name) # used to create alphabetical order
        return df_dim \
            .withColumn(f"sk_{dim_name}", 
                        when(col(dim_name) == '', '').otherwise(monotonically_increasing_id() + 1)) \
            .orderBy(f"sk_{dim_name}")

    @staticmethod
    def add_pk_col(df_dim: DataFrame, fact_name: str) -> DataFrame:
        """
        Adds a primary key column to the DataFrame.

        Returns:
            DataFrame: The DataFrame with the added primary key column.
                e.g.:
                    +---------+--------------+
                    |sk_source|pk_fact_source|
                    +---------+--------------+
                    |        1|             1|
                    |        2|             2|
        """
        return df_dim \
            .orderBy(f"sk_{fact_name}") \
            .withColumn(f"pk_fact_{fact_name}", 
                        when(col(f'sk_{fact_name}') == '', '').otherwise(monotonically_increasing_id() + 1)) \
            .distinct()

    @staticmethod
    def add_qty_col(df: DataFrame, fact_name: str) -> DataFrame:
        """
        Adds a count column to the DataFrame.

        Returns:
            DataFrame: The DataFrame with the added count column.
                e.g.:
                ... +--------+---------+
                ... |sk_layer|qty_field|
                ... +--------+---------+
                ... |       3|        1|
        """
        return df.withColumn(f"qty_{fact_name}", lit(1))

    @staticmethod
    def add_col_total_cols_by_table(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy(f"sk_table")
        return  df \
            .withColumn("total_cols_by_table", count("sk_field").over(window_spec)) \
            .withColumn("total_cols_by_table", col("total_cols_by_table").cast(IntegerType()))

    @staticmethod
    def add_col_sum_cols_qty_by_table(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy(f"sk_table")
        return  df \
            .withColumn(f"sum_qty_field_documented_by_table", sum("qty_field_documented").over(window_spec))

    @staticmethod
    def add_col_sum_qty_table_documented_by_source(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source")
        return df \
            .withColumn("sum_qty_table_documented_by_source", sum("qty_table_documented").over(window_spec))

    @staticmethod
    def add_col_sum_qty_field_documented_by_source(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source")
        return df \
            .withColumn("sum_qty_field_documented_by_source", sum("qty_field_documented").over(window_spec))

    @staticmethod
    def add_col_total_cols_by_source(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source")
        return df \
            .withColumn("total_cols_by_source", count("sk_field").over(window_spec)) \
            .withColumn("total_cols_by_source", when(col('total_cols_by_source').isNull(), lit(0)).otherwise(col('total_cols_by_source')))  \
            .withColumn("total_cols_by_source", col("total_cols_by_source").cast(IntegerType()))

    @staticmethod
    def add_col_total_tables_by_source(df_fact_sources: DataFrame, df_fact_tables: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source")
        df_fact_tables =  df_fact_tables \
            .withColumn("total_tables_by_source", count("sk_table").over(window_spec))  \
            .withColumn("total_tables_by_source", when(col('total_tables_by_source').isNull(), lit(0)).otherwise(col('total_tables_by_source')))  \
            .withColumn("total_tables_by_source", col("total_tables_by_source").cast(IntegerType()))

        return df_fact_sources \
            .join(df_fact_tables, on='sk_source', how='left') \
            .select(*df_fact_sources.columns, 'total_tables_by_source', 'qty_table_documented')

    @staticmethod
    def add_col_total_cols_by_source_by_data_steward(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source", "tag_table_data_steward")
        return  df \
            .withColumn("total_cols_by_source_by_data_steward", count("sk_field").over(window_spec)) \
            .withColumn("total_cols_by_source_by_data_steward", col("total_cols_by_source_by_data_steward").cast(IntegerType()))

    @staticmethod
    def add_col_total_tables_by_source_by_data_steward(df: DataFrame) -> DataFrame:
        window_spec = Window.partitionBy("sk_source", "tag_table_data_steward")
        return  df \
            .withColumn("total_tables_by_source_by_data_steward", count("*").over(window_spec)) \
            .withColumn("total_tables_by_source_by_data_steward", col("total_tables_by_source_by_data_steward").cast(IntegerType()))

    @staticmethod
    def add_qty_to_document_col(
        df: DataFrame, fact_name: str, total_to_document: int
    ) -> DataFrame:
        """
        Add a column to count total columns to document, fill in.

        Returns:
            DataFrame: The DataFrame with the added count column.
                e.g.:
                ... +--------+-------------------+
                ... |sk_layer|qty_field_document |
                ... +--------+-------------------+
                ... |       3|                  6|
        """
        return df.withColumn(f"qty_{fact_name}_document", lit(total_to_document))

    def add_qty_documented_col(
        self, df_fact: DataFrame, df_dim: DataFrame, fact_name: str, list_cols_to_document: List[str], list_not_dim: List[str] 
    ) -> DataFrame:
        """
        Add a column, in df_base or df_dim, to count total columns that was documented.
        example:
            dim_field
                srk_field False
                Sensitive Info	True
                Business Relevant True
                Is Derived True
                Category True
                IMO Data Number	False
                Data Element True
                IMO Shared Field False
                Description	True
                -------------------------
                6 documented
        Returns:
            DataFrame: The DataFrame df_base or df_dim with a new column.
                e.g.:
                ... +---------------------+
                ... |qty_field_documented |
                ... +---------------------+
                ... |                    6|
        """
        # TODO: transform in a new func
        # prepare a list of cols that can be fill
        list_not_dim.append(f'{fact_name}_description')
        list_cols_to_document.remove(f'{fact_name}_description')
        list_cols_to_doc = ['sk_' + c for c in list_cols_to_document if c not in list_not_dim]
        list_cols_to_doc.extend(list_not_dim)
        if fact_name == 'table':
            list_cols_to_doc.remove('tag_table_last_data_steward')
            list_not_dim.remove('tag_table_last_data_steward')

        df_dim = df_dim.select(*list_not_dim, f'sk_{fact_name}')
        df_fact = df_fact.join(df_dim, f'sk_{fact_name}', 'inner')
        # ---------------------------------

        # Initialize the sum
        sum_expr = lit(0)

        for col_name in list_cols_to_doc:            
            sum_expr += when((col(col_name) != -1) | (col(col_name) != 'not_filled'), 1).otherwise(0)

        return df_fact.withColumn(f"qty_{fact_name}_documented", sum_expr)

    @staticmethod
    def add_col_qty_to_document_total(
            df: DataFrame, fact_name: str, to_doc_fields: int, to_doc_tables: int = None
    ) -> DataFrame:
        if fact_name == 'table':
            return (
                df.withColumn("qty_table_document_total",
                    when(
                        col('total_cols_by_table') == 0,  # it is 0 when the cols are from bronze
                        col('qty_table_document')
                    ).otherwise(expr(f"qty_table_document + (total_cols_by_table * {to_doc_fields})"))
                )
                .withColumn("qty_table_document_total", col("qty_table_document_total").cast(IntegerType()))
            )
        else:
            return (
                df.withColumn("qty_source_document_total", 
                    expr(f"qty_source_document + (total_cols_by_source * {to_doc_fields}) + (total_tables_by_source * {to_doc_tables})")
                )
                .withColumn("qty_source_document_total", col("qty_source_document_total").cast(IntegerType()))
            )

    @staticmethod
    def add_col_qty_documented_total(df: DataFrame, fact_name: str) -> DataFrame:
        if fact_name == 'table':
            return (
                df.withColumn("qty_table_documented_total",
                    when(
                        col('total_cols_by_table') == 0,  # it is 0 when the cols are from bronze
                        col('qty_table_documented')
                    ).otherwise(expr("qty_table_documented + sum_qty_field_documented_by_table"))
                )
                .withColumn("qty_table_documented_total", col("qty_table_documented_total").cast(IntegerType()))
            )
        else:
            return (
                df.withColumn("qty_source_documented_total", 
                    expr("qty_source_documented + sum_qty_table_documented_by_source + sum_qty_field_documented_by_source")
                )
                .withColumn("qty_source_documented_total", col("qty_source_documented_total").cast(IntegerType()))
            )
