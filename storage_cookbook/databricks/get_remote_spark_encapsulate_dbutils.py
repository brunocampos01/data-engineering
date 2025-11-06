def get_remote_spark() -> SparkSession:
    try:
        # set up in local environment
        HOST_DATABRICKS = os.getenv('HOST_DATABRICKS')
        TOKEN_DATABRICKS = os.getenv('TOKEN_DATABRICKS')
        CLUSTER_ID_DATABRICKS = os.getenv('CLUSTER_ID_DATABRICKS')

        return SparkSession \
            .builder \
            .remote(f'sc://{HOST_DATABRICKS}:433/;'
                    f'token={TOKEN_DATABRICKS};'
                    f'use_ssl=true;'
                    f'x-databricks-cluster-id={CLUSTER_ID_DATABRICKS}') \
            .appName('spark_remote_connect') \
            .getOrCreate()
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()


# ---------------------------------
# dbutils for handle remote spark
# ---------------------------------
class DbUtils:
    """
    Class to encapsulate the dbutils
    """
    def __init__(self, spark_session, dbutils):
        self.spark = spark_session
        try:
            self.dbutils = dbutils.widgets
        except AttributeError as e:
            print(e)
            self.dbutils = None

    def removeAll(self):
        print('cdjn;ias gfliasgedofru as')
        try:
            self.dbutils.removeAll()
        except Exception:
            logger.warning('remote mode')

    def text(self, key_name, value):
        try:
            self.dbutils.text(key_name, value)
        except Exception:
            logger.warning('remote mode')
            path_tmp_file = ''.join('/tmp' + f'/{key_name}.txt')
            with open(path_tmp_file, 'a') as file:
                file.write(value)

    def get(self, key_name):
        try:
            return self.dbutils.get(key_name)
        except Exception:
            logger.warning('remote mode')

            path_tmp_file = ''.join('/tmp' + f'/{key_name}.txt')
            with open(path_tmp_file, 'r') as file:
                return file.read()
