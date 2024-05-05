from pyspark.errors import AnalysisException
from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    split,
    explode,
    row_number,
    array,
    from_utc_timestamp,
    to_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from pyspark.sql.window import Window

from data_catalog.src.transformer.base_transformer import BaseTransformer


class SilverTransformer(BaseTransformer):
    """A transformer for processing silver step ETL."""
    def __init__(self, spark: SparkSession, layer_name: str):
        super().__init__(spark, layer_name)

    @staticmethod
    def __remove_info_schema_rows(df: DataFrame) -> DataFrame:
        """
        source information is related Unity Catalog data and not about the data lake
        """
        return df \
            .filter(df["source"] != 'information') \
            .filter(df["source"] != 'default')

    @staticmethod
    def __handle_null_values_source_system_col(df: DataFrame, col_name: str) -> DataFrame:
        """
        If the tables was not mapped yet, the 'tag_table_multiple_data_sources' col can be null.
        To handle this situation this function input the source value.

        Args:
            df (DataFrame): The input DataFrame.
                Example:
                    ... -------------------------------+
                    ... tag_table_multiple_data_sources|
                    ... -------------------------------+
                    ...                           null |
                    ... -------------------------------+

        Returns:
            DataFrame: The df with the 'tag_table_multiple_data_sources' without null values.
                Example:
                    ... -------------------------------+
                    ... tag_table_multiple_data_sources|
                    ... -------------------------------+
                    ...                           imos |
                    ... -------------------------------+
        """
        return df \
            .withColumn(col_name, when(
                    df[col_name].isNull(), df['source']
                ).otherwise(df[col_name])
            )

    def __parse_str_col_to_list(self, df: DataFrame, col_name: str) -> DataFrame:
        """
        Splits the 'tag_table_multiple_data_sources' col in the df into a list of str.

        Args:
            df (DataFrame): The input DataFrame.
                Example:
                    ... -------------------------------+
                    ... tag_table_multiple_data_sources|
                    ... -------------------------------+
                    ...                        a, b, c |
                    ... -------------------------------+

        Returns:
            DataFrame: The df with the 'tag_table_multiple_data_sources' col split into a list.
                Example:
                    ... -------------------------------+
                    ... tag_table_multiple_data_sources|
                    ... -------------------------------+
                    ...                       [a, b, c]|
                    ... -------------------------------+
        """
        try:
            return df \
                .withColumn(f'array_{col_name}', when(
                        df[col_name].isNotNull(), split(df[col_name], ', ')
                    ).otherwise(array())
                )
        except Exception:
            self.logger.warn(f'{col_name} not found in UC.')

    @staticmethod
    def _insert_null(df: DataFrame) -> DataFrame:
        """
        Insert null value when the content in a cell is empty or 'None'

        Args:
            df (DataFrame): 
                example
                +---------+--------------------+ ...
                |   source|  source_description| ...
                +---------+--------------------+ ...
                |bloomberg|                    | ...
                |   compas|Crew management s...| ...

        Return:
                +---------+--------------------+ ...
                |   source|  source_description| ...
                +---------+--------------------+ ...
                |bloomberg|                NULL| ...
                |   compas|Crew management s...| ...
        """
        for c in df.columns:
            df = df \
                .withColumn(c, when( col(c) == '', None).otherwise(col(c))) \
                .withColumn(c, when( col(c) == 'None', None).otherwise(col(c)))

        return df

    @staticmethod
    def __get_last_data_version(df_origin: DataFrame, identity_col: str, table_name: str) -> DataFrame:
        """
        To get the last data version,
        The func uses the Window function to partition the data based on the path_col_uc
        and then ordering by field_last_updated_at in descending order.
            e.g.:
                +------------------------------+--------------------... ---+
                | field_last_updated_at        | field_id           ...    |
                +------------------------------+--------------------... ---+
                | 2023-07-28T15:29:24.405+0000 | dev_silver.thor.abp... Id |
                | 2023-10-23T17:15:35.849+0000 | dev_silver.thor.abp... Id |
                +------------------------------+--------------------... ---+
        Return:
            DataFrame, e.g.:
                +------------------------------+--------------------... ---+
                | field_last_updated_at        | field_id           ...    |
                +------------------------------+--------------------... ---+
                | 2023-10-23T17:15:35.849+0000 | dev_silver.thor.abp... Id |
                +------------------------------+--------------------... ---+
        """
        window_spec = Window \
            .partitionBy(identity_col) \
            .orderBy(col(f"{table_name}_last_updated_at").desc())
        df_origin = df_origin.withColumn("row_number", row_number().over(window_spec))

        # Filter to keep only the rows with the latest values
        return df_origin.filter(df_origin.row_number == 1).drop("row_number")

    def __get_latest_operation_timestamp(self, table_id, type: str, describe_history: DataFrame):
        try:
            if type == 'data':
                result = describe_history \
                    .filter(col('operation').isin(['WRITE', 'MERGE', 'DELETE', 'TRUNCATE'])) \
                    .orderBy(col('timestamp').desc()) \
                    .limit(1)
            else:
                result = describe_history \
                    .filter(col('operation').isin(['CREATE TABLE', 'ALTER TABLE', 'CHANGE COLUMN'])) \
                    .orderBy(col('timestamp').desc()) \
                    .limit(1)
        except Exception as e: # AnalysisException
            # when the data has problem in Unity CataLog
            return None
        else:
            latest_timestamp = result.select('timestamp').first()
            return latest_timestamp[0] if latest_timestamp else None

    @staticmethod
    def _get_table_last_updated_at(df: DataFrame, table_id: str) -> str:
        last_updated = df \
            .select('table_last_updated_at') \
            .filter(col('table_id') == table_id) \
            .collect()[0].table_last_updated_at

        return last_updated

    def __add_cols_last_updated_at(self, df: DataFrame) -> DataFrame:
        table_id_list = df.select('table_id').rdd.map(lambda x: x[0]).collect()
        last_updated_at = df.select('table_last_updated_at').rdd.map(lambda x: x[0]).collect()
        last_changed_data = []
        last_changed_schema = []
        for t_id in table_id_list:
            is_table = df \
                .select('obj_type') \
                .filter(col('obj_type') == 'TABLE') \
                .collect()[0].obj_type
            if is_table:
                try:
                    describe_history = self.spark.sql(f"DESCRIBE HISTORY {t_id}")
                except Exception:
                    describe_history = None

                latest_timestamp_data = None
                latest_timestamp_schema = None

                try:
                    latest_timestamp_data = self.__get_latest_operation_timestamp(t_id, 'data', describe_history)
                except Exception as e: # AnalysisException
                    # when the data has problem in Unity Catlog
                    latest_timestamp_data = None

                try:
                    latest_timestamp_schema = self.__get_latest_operation_timestamp(t_id, 'schema', describe_history)
                except Exception as e: # AnalysisException
                    # when the data has problem in Unity CataLog
                    latest_timestamp_schema = None

            if latest_timestamp_data == None:
                latest_timestamp_data = self._get_table_last_updated_at(df, t_id)

            last_changed_data.append(latest_timestamp_data)

            if latest_timestamp_schema == None:
                latest_timestamp_schema = self._get_table_last_updated_at(df, t_id)

            last_changed_schema.append(latest_timestamp_schema)

        # prepare join
        schema = StructType([
            StructField("table_id", StringType(), True),
            StructField("table_last_updated_at", TimestampType(), True),
            StructField("table_last_data_updated_at", TimestampType(), True),
            StructField("table_last_schema_updated_at", TimestampType(), True),
        ])

        data = list(zip(table_id_list, last_updated_at, last_changed_data, last_changed_schema))
        df_new_timestamps = self.spark.createDataFrame(data, schema=schema) \
            .withColumn("table_last_data_updated_at", from_utc_timestamp(col("table_last_data_updated_at"), "UTC")) \
            .withColumn("table_last_data_updated_at", when(
                    col("table_last_data_updated_at").isNull(), col("table_last_updated_at")
                ).otherwise(col('table_last_data_updated_at'))) \
            .withColumn("table_last_data_updated_at", to_timestamp("table_last_data_updated_at", "yyyy-MM-dd HH:mm:ss.SSSSSS")) \
            .withColumn("table_last_schema_updated_at", from_utc_timestamp(col("table_last_schema_updated_at"), "UTC")) \
            .withColumn("table_last_schema_updated_at", when(
                    col("table_last_schema_updated_at").isNull(), col("table_last_updated_at")
                ).otherwise(col('table_last_schema_updated_at')))

        return df.join(df_new_timestamps, on='table_id', how='inner').drop('table_last_updated_at')

    def __add_col_last_data_steward(self, df: DataFrame) -> DataFrame:
        try:
            df_silver = self._load_delta_table(table_name='tables', layer_name='silver')
            df_silver = df_silver \
                .select(col('table_id'), col('tag_table_data_steward').alias('tag_table_silver_data_steward'))

            if df_silver.count() == 0:
                raise Exception('')

            df = df \
                .join(df_silver, df.table_id == df_silver.table_id, 'left') \
                .select(df['*'], df_silver['tag_table_silver_data_steward'])
            
            df = df.withColumn("tag_table_last_data_steward", when(
                    col("tag_table_data_steward") == col("tag_table_silver_data_steward"), col("tag_table_silver_data_steward")
                ).otherwise(col("tag_table_data_steward")))
            return df.select(*df.columns).drop('tag_table_silver_data_steward')

        except Exception as e:
            self.logger.warning(f'Delta Table: tables on Azure '
                                f'silver/{self.folder_name}/tables not found values.\n{e}')
            return df.withColumn("tag_table_last_data_steward", lit(''))

    @staticmethod
    def __process_data_stewards(df: DataFrame) -> DataFrame:
        return df

    @staticmethod
    def __process_layers(df: DataFrame) -> DataFrame:
        """
        If the layer_description column is completely null, the bronze layer will not save.
        In silver the code check and if not exits, add it.
        """
        if 'layer_description' not in df.columns:
            return df.withColumn('layer_description', lit(None).cast('string'))
        else:
            return df

    def __process_sources(self, df_origin: DataFrame) -> DataFrame:
        """
        Process the sources df by removing specified cols and dropping duplicate rows.

        Args:
            df_origin (DataFrame): Input DataFrame to be processed.

        Returns:            Processed DataFrame after dropping specified columns and duplicates.
        """
        df = self.__remove_info_schema_rows(df_origin)
        df = self.insert_value_in_null(df, 'source')
        list_cols_to_drop = ['layer', 'layer_raw', 'source_raw']
        self.logger.info(f'Removing the columns: {list_cols_to_drop}')
        return df \
            .drop(*list_cols_to_drop) \
            .dropDuplicates(['source'])

    def __process_tables(self, df_origin: DataFrame) -> DataFrame:
        """
        Args:
            df_origin (DataFrame): Input DataFrame to be processed.

        Returns:
            DataFrame: Processed DataFrame
        """
        df = self.__get_last_data_version(df_origin, 'table_id', 'table')
        df = self.__handle_null_values_source_system_col(df, 'tag_table_source_system')
        df = self.__remove_info_schema_rows(df)
        df = self.__parse_str_col_to_list(df,'tag_table_source_system')
        df = self.__add_cols_last_updated_at(df)
        df = self.__add_col_last_data_steward(df)
        df = self.insert_value_in_null(df, 'table')

        return df
  
    def __process_fields(self, df_origin: DataFrame) -> DataFrame:
        """
        Obtaining the last data version based on 'field_id'.

        Args:
            df_origin (DataFrame): Input DataFrame to be processed.

        Returns:
            Processed df after 'data_steward' columns
            and obtaining the last data version based on 'field_id'.
        """
        df = self.__get_last_data_version(df_origin, 'field_id', 'field')
        df = self.__parse_str_col_to_list(df, 'tag_field_data_usage')
        df = self.insert_value_in_null(df, 'field')
        return self.__remove_info_schema_rows(df)

    def __process_sources_tables(self, df: DataFrame) -> DataFrame:
        """
        explode_outer() keep the null values
        Table used to do a union

        Args:
            df_origin (DataFrame): Input DataFrame to be processed.
        """
        df = df \
            .withColumn('source_exploded', explode(df['array_tag_table_source_system'])) \
            .withColumn('source_exploded', when(
                    col('source_exploded') == '', col('source')
                ).otherwise(col('source_exploded')))

        # update the source_raw and tag_table_source_system
        df = df.withColumn('source_raw_updated', when(
                col('source') == col('source_exploded'), col('source_raw')
            ).otherwise(col('source_exploded')))
        df = df.withColumn('tag_table_source_system', col('source_raw_updated'))

        # recreate index
        df = self.add_id_cols(df, 'sources_tables')
        df = df.withColumnRenamed('table_id', 'origin_table_id')

        return df

    def __process_fields_data_usage(self, df: DataFrame) -> DataFrame:
        """
        explode_outer() keep the null values
        Table used to do a union

        Args:
            df (DataFrame): Input DataFrame to be processed.
        """
        df = df \
            .withColumn('fields_exploded', explode(df['array_tag_field_data_usage'])) \
            .withColumn('fields_exploded', when(
                    col('fields_exploded') == '', col('tag_field_data_usage')
                ).otherwise(col('fields_exploded')))
      
        df = df.select(
            col('field_id'),
            col('fields_exploded').alias('field_data_usage'), 
            col('field_created_in_uc_at'),
            col('field_last_updated_at')
        ).filter(col('fields_exploded') != '')
        
        return df

    def execute(self, df_origin: DataFrame, table_name: str) -> DataFrame:        
        if table_name == 'sources':
            return self.__process_sources(df_origin)

        elif table_name == 'tables':
            return self.__process_tables(df_origin)

        elif table_name == 'fields_datausage':
            return self.__process_fields_data_usage(df_origin)

        elif table_name == 'fields':
            return self.__process_fields(df_origin)
        
        elif table_name == 'layers':
            return self.__process_layers(df_origin)

        elif table_name == 'data_stewards':
            return self.__process_data_stewards(df_origin)

        elif table_name == 'sources_tables':
            return self.__process_sources_tables(df_origin)

        else:
            raise ValueError(f"Unsupported table_name: {table_name}")
