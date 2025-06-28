import IPython
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, coalesce, lit
from library.dataloader.defaults import Defaults


class CreateReferenceData:
    """
    This class is responsible for creating the reference input and output file for qa validation from bronze to silver
    """

    def __init__(self):
        """Init Class"""
        self.dbutils = IPython.get_ipython().user_ns["dbutils"]

    @staticmethod
    def __create_file(self, df: DataFrame, destination_path: str, file_format: str) -> None:
        """This function creates a parquet file in dbfs, rename the file, and deletes generated SUCCESS files

        Args:
            df (DataFrame): data to be saved
            destination_path (str): destination path in Repos folder
            file_format (str): file format for saving
        """
        # getting current timestamp
        current_datetime = datetime.now()
        current_datetime = current_datetime.strftime("%Y%m%d%H%M%S")

        # saving dataframe in dbfs and moving to Repos
        if file_format == "csv":
            df.coalesce(1).write.option("header", "true").option("sep", "|").format(file_format).save(
                f"output_{current_datetime}"
            )
        elif file_format == "parquet":
            df.coalesce(1).write.format(file_format).save(f"output_{current_datetime}")

        self.dbutils.fs.mv(
            [
                x.path
                for x in self.dbutils.fs.ls(f"output_{current_datetime}")
                if x.name.startswith("part-00000") == True
            ][0],
            f"{destination_path}",
        )
        self.dbutils.fs.rm(f"output_{current_datetime}", True)

    @staticmethod
    def ignore_deleted_rows(df: DataFrame) -> DataFrame:
        """
        Remove rows identified as *deleted* from the dataframe
        Args:
            df (DataFrame): DataFrame to be filtered

        Returns:
            df (DataFrame): filtered DataFrame

        """
        if Defaults.deleted_flag_column in df.columns:
            return df.filter(coalesce(col(Defaults.deleted_flag_column), lit(False)) == False)
        return df

    def create_file(self, df: DataFrame, destination_path: str, file_format: str) -> None:
        """Function to save the dataframe to the output

        Args:
            df (DataFrame): output data to be saved
            destination_path (str): destination path in Repos folder
            file_format (str): file format for saving
        """
        # removing "|" and treating ByteType
        for df_tuple in df.dtypes:
            if df_tuple[1] == "string":
                df = df.withColumn(df_tuple[0], regexp_replace(col(df_tuple[0]), "|", ""))
            elif df_tuple[1] in ("byte", "tinyint"):
                df = df.withColumn(df_tuple[0], col(df_tuple[0]).cast("integer"))

        # ignoring deleted rows
        df = df.transform(self.ignore_deleted_rows)

        # creating reference file
        self.__create_file(self, df, destination_path, file_format)
