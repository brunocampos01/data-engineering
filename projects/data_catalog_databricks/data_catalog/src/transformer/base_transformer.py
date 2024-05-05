from abc import abstractmethod
from typing import List

from pyspark.dbutils import DBUtils
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    concat,
    concat_ws,
    lit,
    split,
    when,
    lower,
)

from data_catalog.src.base_data_catalog import BaseDataCatalog
from data_catalog.src.loader.delta_table_loader import DeltaTableLoader
from data_catalog.src.transformer.add_sql_statement_col import AddSQLStatementCol
from data_catalog.src.transformer.add_time_cols import AddTimeCols


class BaseTransformer(BaseDataCatalog):
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)
        self.dbutils = DBUtils(self.spark)

    @abstractmethod
    def execute(self, df_origin: DataFrame, table_name: str):
        pass

    def get_delta_path(self, table_name: str) -> str:
        delta_path = DeltaTableLoader(
            spark=self.spark,
            layer_name=self.layer_name,
            container_name=self.layer_name,
            folder_name=self.folder_name,
        ).delta_path

        return f'{delta_path}/{table_name}/'

    def _load_delta_table_from_query(self, query_file_path: str) -> DataFrame:
        return DeltaTableLoader(
            spark=self.spark,
            layer_name=self.layer_name,
            container_name=self.layer_name,
            folder_name=self.folder_name,
        ).load_data_from_query(query_file_path)

    def _load_delta_table(
        self, table_name: str, layer_name: str = None, folder_name: str = None
    ) -> DataFrame:
        if layer_name == None:
            layer_name = self.layer_name

        if folder_name == None:
            folder_name = self.folder_name

        return DeltaTableLoader(
            spark=self.spark,
            layer_name=layer_name,
            container_name=layer_name,
            folder_name=folder_name,
        ).execute(table_name)

    @staticmethod
    def add_prefix_tag_cols(
            df: DataFrame, table_name: str, list_tag_names: List
    ) -> DataFrame:
        for col_name in list_tag_names:
            df = df.withColumnRenamed(col_name, f'tag_{table_name}_{col_name}')

        return df

    @staticmethod
    def clean_col_name(df: DataFrame, table_name: str, list_to_exclude: List[str]) -> DataFrame:
        list_cols = [c for c in df.columns if c not in list_to_exclude]
        return df.select(
            *[
                col(c).alias(c.lower().strip().replace(f'tag_{table_name}_', ''))
                if c.startswith(f'tag_{table_name}_') else col(c)
                for c in list_cols
            ]
        )

    @staticmethod
    def insert_value_in_null(df: DataFrame, table_name: str) -> DataFrame:
        """
        Switch nulls by 0. 
        It is necessary because all dimesions keep the NULL value as 0.

        Args:
            DataFrame:
                Example:
                +---------+------... +-------------+
                |sk_source|sk_tag... |pk_fact_field|
                +---------+------... +-------------+
                |2        |NULL  ... |5            |
                |1        |64    ... |8            |

        Return:
                +---------+------... +-------------+
                |sk_source|sk_tag... |pk_fact_field|
                +---------+------... +-------------+
                |2        |''    ... |5            |
                |1        |64    ... |8            |
        """
        col_name = f"{table_name}_description"
        df = df.withColumn(col_name,
                when(col(col_name) == "None", "").otherwise(col(col_name))
            )
        return df.fillna('')

    def add_id_cols(self, df: DataFrame, table_name: str = 'table', place_df: str = '') -> DataFrame:
        self.logger.info(f'Adding in df {place_df} new column: layer_raw')
        df = self.add_col_layer_raw(df)
        self.logger.info(f'Adding in df {place_df} new column: table_id')
        df = self.add_col_table_id(df)

        if table_name == 'fields':
            self.logger.info(f'Adding in df {place_df} new column: field_id')
            df = self.add_col_field_id(df)

        elif table_name == 'sources_tables':
            self.logger.info(f'Adding in df {place_df} new column: sources_table_id')
            df = self.add_col_sources_table_id(df)

        return df

    @staticmethod
    def add_col_id_with_backticks(df: DataFrame) -> DataFrame:
        """
        Add a new column table_id.

        Return:
            DataFrame, e.g.:
            +------------------------------------------+
            |table_id                                  |
            +------------------------------------------+
            | `dev_silver`.`imos_datalake_api`.`inqdtl`|
        """
        return df.withColumn('table_id_backtick', concat_ws(
                '',
                lit('`'), col("layer_raw"), lit('`.'),
                lit('`'), col('source_raw'), lit('`.'),
                lit('`'), col('table'), lit('`')
            )
        )

    def add_col_layer_raw(self, df: DataFrame) -> DataFrame:
        """
        Add a new column layer_raw.

        Return:
            DataFrame, e.g.:
            +----------+
            |layer_raw |
            +----------+
            |dev_gold  |
            |dev_silver|
            +----------+
        """
        return df.withColumn('layer_raw', concat(lit(f"{self.env}_"), col("layer")))

    @staticmethod
    def add_col_layer(df: DataFrame) -> DataFrame:
        """
        Add a new column layer.

        Return:
            DataFrame, e.g.:
            +----------+
            |layer.    |
            +----------+
            |gold      |
            |silver    |
            +----------+
        """
        return df.withColumn('layer', split(col("layer_raw"), "_")[1])

    @staticmethod
    def add_col_source(df: DataFrame) -> DataFrame:
        """
        Add a new column source_raw.

        Return:
            DataFrame, e.g.:
            +----------------------+
            |source_raw            |
            +----------------------+
            |shipsure_api          |
            |imos_datalake_api     |
        """
        return df.withColumn('source', split(col('source_raw'), '_').getItem(0))

    @staticmethod
    def add_col_type_extraction(df: DataFrame) -> DataFrame:
        return df.withColumn('type_extraction', split(col("source_raw"), "_")[1])

    @staticmethod
    def add_col_table_id(df: DataFrame) -> DataFrame:
        """
        Add a new column table_id.

        Return:
            DataFrame, e.g.:
            +------------------------------------+
            |table_id                            |
            +------------------------------------+
            | dev_silver.imos_datalake_api.inqdtl|
        """
        return df.withColumn('table_id', concat_ws(
                '.', col("layer_raw"), col('source_raw'), col('table')
            )
        ).withColumn("table_id", lower(col("table_id")))

    # TODO: analyze if is necessary
    @staticmethod
    def add_col_ingested_at(df: DataFrame) -> DataFrame:
        return df

    @staticmethod
    def add_col_sources_table_id(df: DataFrame) -> DataFrame:
        """
        Add a new column sources_table_id.

        Return:
            DataFrame, e.g.:
            +------------------------------------+
            |sources_table_id                    |
            +------------------------------------+
            | dev_silver.imos_datalake_api.inqdtl|
        
        NOTE:
         - the user insert source in source_system, not source_raw
        """
        return df.withColumn('sources_table_id', concat_ws(
                '.', col("layer_raw"), col('source_raw_updated'), col('table')
            )
        ).withColumn("sources_table_id", lower(col("sources_table_id")))

    @staticmethod
    def add_col_field_id(df: DataFrame) -> DataFrame:
        """
        Add a new column field_id.

        Return:
            DataFrame, e.g.:
            +----------------------------------------------+
            |field_id                                      |
            +----------------------------------------------+
            | dev_silver.imos_datalake_api.inqdtl.currTotal|
        """
        return df.withColumn('field_id', concat_ws(
                '.', col("layer_raw"), col('source_raw'), col('table'), col('field')
            )
        ).withColumn("field_id", lower(col("field_id")))

    def add_col_sql_statement(
        self, df: DataFrame, list_origin_tags_names: List[str], table_name: str
    ) -> DataFrame:
        """
        Encapsulated function to add new column.
        When column is more complex, was created classes add_col.py and encapsulate the logic.
        """
        add_col_sql_statement = AddSQLStatementCol(self.spark, self.layer_name)
        return add_col_sql_statement.execute(
            df=df, list_origin_tags_names=list_origin_tags_names, table_name=table_name
        )

    def add_cols_created_and_last_updated(
        self,
        df: DataFrame,
        table_name: str,
        is_first_execution: bool = False,
    ) -> DataFrame:
        """
        Add 'created_at' and 'last_updated_at' columns to a DataFrame.

        Args:
            df (DataFrame): The input DataFrame.
            table_name (str): The name of the table for which the columns are created.
            is_first_execution (bool, optional): A flag indicating if it's the first execution. Defaults to False.

        Returns:
            DataFrame: The df with the added 'created' and 'last_updated' columns.
                Example.:
                +-----------------------+-----------------------+
                |field_created_at       |field_last_updated_at  |
                +-----------------------+-----------------------+
                |2023-09-25 00:45:01.193|2023-09-25 00:45:01.193|
                |2023-07-28 21:34:09.896|2023-07-28 21:34:09.896|

        Notes:
            Encapsulated function to add new column.
            This function assumes that the table name is in plural form and derives the singular form by removing the last character.
        """
        singular_table_name = table_name[:-1]

        add_time_cols = AddTimeCols(self.spark, self.layer_name)
        return add_time_cols.execute(df, singular_table_name, is_first_execution)

    def add_col_data_steward(self, df: DataFrame) -> DataFrame:
        """
        Add data_steward col in fields(table_name) table based on tables(table_name)

        Return:
            + ... +--------------------+--------------------+------------+
            | ... |            table_id|            field_id|data_steward|
            + ... +--------------------+--------------------+------------+
            | ... |dev_silver.imos_d...|dev_silver.thor  ...|        NULL|
            | ... |dev_silver.imos_d...|dev_silver.imos_d...|        xpto|
        """
        df_tables = self._load_delta_table('tables') \
            .select('table_id', 'tag_table_data_steward') \
            .withColumnRenamed('table_id', 'table_id_tables') \
            .withColumnRenamed('tag_table_data_steward', 'tag_field_data_steward')
        join_cond = (df['table_id'] == df_tables['table_id_tables'])

        return df.join(df_tables, on=join_cond, how='inner') \
            .select(*df.columns, 'tag_field_data_steward') \
            .drop('table_id_tables')

    def execute(self):
        pass
