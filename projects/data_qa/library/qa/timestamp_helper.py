import os
from typing import (
    List,
    Dict,
    Tuple,
)

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql.functions import (
    col, 
    date_trunc,
    ceil,
    year,
    month,
    dayofmonth,
    hour,
    minute,
    second,
    substring,
    concat_ws,
    round,
    when,
    concat,
    lit,
    lpad,
)


class TimestampHelper:
    @staticmethod
    def get_timestamp_cols(df_only_properties: DataFrame) -> list:
        """
        ### Extracts timestamp columns from a DataFrame containing column names and data types.
        
        #### Args:
            * df_only_properties (DataFrame): DataFrame containing `COLUMN_NAME` and `DATA_TYPE` columns.

        #### Returns:
            * list: list containing `COLUMN_NAME` and `DATA_TYPE` columns
        """
        return (df_only_properties
            .select('COLUMN_NAME', 'DATA_TYPE')
            .filter(col('DATA_TYPE') == 'datetime')
            .collect()
        )

    @staticmethod
    def extract_timestamp_components(df: DataFrame, col_name: str) -> DataFrame:
        """
        ### Extracts various components (year, month, day, hour, etc.) from a timestamp column.
        
        #### Args:
            * df (DataFrame): Input DataFrame.
            * col_name (str): Name of the timestamp column.
        
        #### Returns:
            * DataFrame: DataFrame with added timestamp components columns.
        """
        return df.withColumn("year", year(col_name)) \
                .withColumn("month", month(col_name)) \
                .withColumn("day", dayofmonth(col_name)) \
                .withColumn("hour", hour(col_name)) \
                .withColumn("minute", minute(col_name)) \
                .withColumn("second", second(col_name)) \
                .withColumn("millisecond", substring(col_name, 21, 3))

    @staticmethod
    def __calculate_seconds_milliseconds(df: DataFrame) -> DataFrame:
        """
        Concatenates 'second' and 'millisecond' columns of a DataFrame into a new column 'sec_millisec'.
        Casts the concatenated column into double type and rounds it to the nearest second.
        
        Parameters:
            df (DataFrame): Input DataFrame containing 'second' and 'millisecond' columns.
            
        Returns:
            DataFrame: DataFrame with additional columns 'sec_millisec' and 'rounded_double_sec_millisec'.
        """
        return df.withColumn("sec_millisec", concat_ws('.', col('second'), col('millisecond'))) \
                .withColumn("double_sec_millisec", col("sec_millisec").cast("double")) \
                .withColumn("rounded_double_sec_millisec", 
                            when((col("double_sec_millisec") % 1) >= 0.5, round(col("double_sec_millisec") + 0.5, 0))
                            .otherwise(col("double_sec_millisec")))

    @staticmethod
    def __update_seconds_minutes_hours(df: DataFrame) -> DataFrame:
        return df.withColumn("updated_sec",
                            when((col("second") == 59) & ((col("double_sec_millisec") % 1) >= 0.5), 0)
                            .otherwise(col("rounded_double_sec_millisec"))) \
                .withColumn("pre_updated_minute", 
                            when((col("second") == 59) & ((col("double_sec_millisec") % 1) >= 0.5), col('minute') + lit(1))
                            .otherwise(col("minute"))) \
                .withColumn("updated_minute", 
                            when((col("pre_updated_minute") == 60), 0)
                            .otherwise(col("pre_updated_minute"))) \
                .withColumn("updated_hour",
                            when((col("minute") == 59) & (col("second") == 59) & ((col("double_sec_millisec") % 1) >= 0.5), col("hour") + lit(1))
                            .otherwise(col("hour")))
    @staticmethod
    def __format_cols(df: DataFrame, timestamp_col: str) -> DataFrame:
        """
        Formats columns in a DataFrame and truncates timestamp to minutes.

        Args:
            df (DataFrame): Input DataFrame.
            timestamp_col (str): Name of the column to store the truncated timestamp.

        Returns:
            DataFrame: DataFrame with formatted columns and truncated timestamp column.
        """
        return df.withColumn('formatted_rounded_double_sec_millisec',
                            concat(col("year"), lit("-"), col("month"), lit("-"), col("day"), lit("T"),
                                   col("updated_hour"), lit(":"), col("updated_minute"), lit(":"),
                                   col("updated_sec"), lit("+0000"))) \
                .withColumn(timestamp_col, date_trunc('minute', col("formatted_rounded_double_sec_millisec")))

    @staticmethod
    def __drop_unnecessary_cols(df: DataFrame) -> DataFrame:
        """
        Drops unnecessary columns from the DataFrame.

        Parameters:
        - df (DataFrame): The input DataFrame from which columns need to be dropped.

        Returns:
        - DataFrame: A new DataFrame with specified columns dropped.
        """
        list_cols_to_drop = ['year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'sec_millisec',
                            'double_sec_millisec', 'rounded_double_sec_millisec', 'formatted_rounded_double_sec_millisec',
                            'updated_hour', 'updated_minute', 'updated_sec', 'pre_updated_minute']
        return df.drop(*list_cols_to_drop)

    @staticmethod
    def prepare_datetime_col(df: DataFrame, list_timestamp_cols: list) -> DataFrame:
        """
        ### Prepares timestamp columns applying some rules
        
        #### Args:
            * df (DataFrame): Input DataFrame
            * list_timestamp_cols (list): list of timestamp columns to apply rules
        
        #### Returns:
            * DataFrame: DataFrame after applying rules
        """
        for col_name in list_timestamp_cols:
            df = TimestampHelper.extract_timestamp_components(df, col_name)
            df = TimestampHelper.__calculate_seconds_milliseconds(df)
            df = TimestampHelper.__update_seconds_minutes_hours(df)
            df = TimestampHelper.__format_cols(df, col_name)
        
        return TimestampHelper.__drop_unnecessary_cols(df)
