import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_remote_spark() -> SparkSession:
    try:
        # set up in local environment
        HOST_DATABRICKS = os.getenv('HOST_DATABRICKS')
        TOKEN_DATABRICKS = os.getenv('TOKEN_DATABRICKS')
        CLUSTER_ID_DATABRICKS = os.getenv('CLUSTER_ID_DATABRICKS')

        return SparkSession \
            .builder \
            .remote(f'sc://{HOST_DATABRICKS}:443/;'
                    f'token={TOKEN_DATABRICKS};'
                    f'use_ssl=true;'
                    f'x-databricks-cluster-id={CLUSTER_ID_DATABRICKS}') \
            .appName('spark_remote_connect') \
            .getOrCreate()
    except Exception:
        return SparkSession.builder.getOrCreate()


# ---------------------------------
# dbutils for handle remote spark
# ---------------------------------
class DbUtils:
    """
    Wraps dbutils.widgets so that notebooks and remote Spark sessions
    share the same API. In remote mode, widget values are persisted to
    /tmp/<key_name>.txt as a fallback.
    """
    def __init__(self, spark_session, dbutils):
        self.spark = spark_session
        try:
            self.dbutils = dbutils.widgets
        except AttributeError as e:
            logger.warning(e)
            self.dbutils = None

    def removeAll(self):
        try:
            self.dbutils.removeAll()
        except Exception:
            logger.warning('remote mode: removeAll is a no-op')

    def text(self, key_name, value):
        try:
            self.dbutils.text(key_name, value)
        except Exception:
            logger.warning('remote mode: writing widget value to /tmp')
            path_tmp_file = f'/tmp/{key_name}.txt'
            with open(path_tmp_file, 'a') as file:
                file.write(value)

    def get(self, key_name):
        try:
            return self.dbutils.get(key_name)
        except Exception:
            logger.warning('remote mode: reading widget value from /tmp')
            path_tmp_file = f'/tmp/{key_name}.txt'
            with open(path_tmp_file, 'r') as file:
                return file.read()
