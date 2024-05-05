import IPython
from pyspark.sql import SparkSession


def get_dbutils():
    """
    Return a Databricks Utilities (dbutils) instance
    """
    ipython = IPython.get_ipython()
    if ipython is None:
        from pyspark.dbutils import DBUtils

        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)
    else:
        dbu = ipython.user_ns["dbutils"]
        return dbu
