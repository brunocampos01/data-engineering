import logging
from typing import Tuple

import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def init_spark(
    step: str,
    dag_name: str,
    layer: str,
    env: str,
    executor_cores: str,
    executor_memory: str,
    executor_instances: str,
    driver_memory: str,
    app_name: str,
    path_native_lib: str,
    path_ojdbc: str = '',
    path_spark_avro: str = '',
    path_spark_xml: str = ''
) -> Tuple[pyspark.sql.session.SparkSession, pyspark.context.SparkContext]:
    conf = SparkConf()
    conf.setAll(
        [
            ('spark.jars', f'{path_ojdbc},{path_spark_avro},{path_spark_xml}'),
            ('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'),
            ('spark.kryoserializer.buffer.max', '1g'),
            ('spark.executor.cores', executor_cores),
            ('spark.executor.memory', executor_memory),
            ('spark.executor.memoryOverhead', '1g'),
            ('spark.executor.instances', executor_instances),
            ('spark.executor.extraJavaOptions', '-XX:+UseParallelGC'),
            ('spark.executor.extraLibraryPath', path_native_lib),
            ('spark.executor.rpc.bindToAl', 'true'),
            ('spark.driver.cores', '4'),
            ('spark.driver.memory', driver_memory),
            ('spark.driver.memoryOverhead', '1g'),
            ('spark.driver.extraJavaOptions', '-XX:+UseParallelGC'),
            ('spark.driver.extraLibraryPath', path_native_lib),
            ('spark.driver.maxResultSize', '0'),
            ('spark.driver.log.dfsDir', '/user/spark/driverLogs'),
            ('spark.driver.log.persistToDfs.enabled', 'true'),
            ('spark.memory.offHeap.enabled', 'true'),
            ('spark.memory.offHeap.size', '4g'),
            ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
            ('spark.sql.repl.eagerEval.enabled', 'True'),
            ('spark.sql.broadcastTimeout', '600'),
            ('spark.stage.maxConsecutiveAttempts', '10'),
            ('spark.shuffle.compress', 'true'),
            ('spark.io.compression.codec', 'snappy'),
            ('spark.rdd.compress', 'true'),
            ('spark.broadcast.compress', 'true'),
            ('spark.debug.maxToStringField', '5000'),
            ('spark.authenticate', 'true'),
            ('spark.network.timeout', '600'),
            ('spark.yarn.queue', f'root.{env}'),
            ('spark.yarn.am.extraLibraryPath', path_native_lib),
            ('spark.yarn.config.gatewayPath', '/opt/cloudera/parcels'),
            ('spark.yarn.config.replacementPath', '{{HADOOP_COMMON_HOME}}/../../..'),
            ('spark.yarn.historyServer.allowTracking', 'true'),
            ('spark.yarn.tags', f'{step},{dag_name},{layer}'),
            ('spark.yarn.appMasterEnv.MKL_NUM_THREADS', '4'),
            ('spark.yarn.appMasterEnv.OPENBLAS_NUM_THREADS', '4'),
            ('spark.executorEnv.MKL_NUM_THREADS', '4'),
            ('spark.executorEnv.OPENBLAS_NUM_THREADS', '4')
        ]
    )

    spark = SparkSession \
        .builder \
        .master('yarn') \
        .appName(app_name) \
        .config(conf=conf) \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    logging.getLogger('py4j').setLevel('WARN')

    return spark, sc
