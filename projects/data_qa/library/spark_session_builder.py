from pyspark.sql import SparkSession
from delta import *


class SparkSessionBuilder:
    """
    This class is responsible for giving a valid spark session
    """
    @staticmethod
    def get_session() -> SparkSession:
        """
        To get the reference of the existing spark session in databricks
        """
        spark = SparkSession.builder.getOrCreate()
        return spark

    @staticmethod
    def get_test_session() -> SparkSession:
        """
        A spark session to use for local testing
        """
        builder = SparkSession.builder \
            .appName("test-application") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.warehouse.dir", "output/warehouse") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.sql.ui.retainedExecutions", "1") \
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=output/tmp/derby/") \
            .config("spark.driver.memory", "4g") \
            .config("dfs.client.read.shortcircuit.skip.checksum", "true") \
            .config("spark.executor.memory", "4g") \
            .config("spark.databricks.delta.snapshotPartitions", "1") \
            .config("spark.worker.ui.retainedExecutors",  "1") \
            .config("spark.worker.ui.retainedDrivers", "1") \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.ui.retainedTasks", "1") \
            .config("spark.ui.retainedStages", "1") \
            .config("spark.ui.retainedJobs", "1") \
            .config("spark.ui.dagGraph.retainedRootRDDs", "1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.rdd.compress", "false") \
            .config("spark.shuffle.compress", "false") \
            .config("spark.shuffle.spill.compress", "false") \
            .enableHiveSupport()

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark

