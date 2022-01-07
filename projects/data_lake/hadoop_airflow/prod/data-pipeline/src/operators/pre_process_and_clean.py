import time

from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from hooks.hdfs.hdfs_helper import HdfsHelper
from utils.data_helper import calculate_partitions
from utils.data_helper import cleansing_data
from utils.data_helper import pre_process_content
from utils.init_spark import init_spark


class PreProcessAndCleanData(LoggingMixin):
    def __init__(self, *args, **kwargs):
        super(PreProcessAndCleanData, self).__init__(*args, **kwargs)

    def execute(self, **context):
        template = '-' * 79
        start = time.time()
        hdfs = HdfsHelper(hdfs_conn=context['hdfs_conn_id'])
        total_registry = int(
            Variable.get(f'{context["dag_name"]}'
                         f'_{context["origin_layer"]}'
                         f'_total_registry'
                         f'_{context["date"]}',
                         default_var='1')
        )
        n_partitions = calculate_partitions(
            total_registry=total_registry,
            max_registry_by_avro=int(context['max_registry_by_avro'])
        )
        spark, sc = init_spark(
            app_name=f'pre_process_{context["dag_name"]}-{context["date"]}',
            step=context['step'],
            env=context['env'],
            dag_name=context['dag_name'],
            layer=context['origin_layer'],
            path_spark_avro=context['path_spark_avro'],
            path_spark_xml=context['path_spark_xml'],
            path_ojdbc=context['path_ojdbc'],
            path_native_lib=context['path_native_lib'],
            executor_cores=context['executor_cores'],
            executor_memory=context['executor_memory'],
            executor_instances='1',
            driver_memory=context['driver_memory']
        )
        avro_schema = hdfs.read_avro_schema(
            path_avro_schema=context['path_avro_schema'],
            layer=context['dest_layer'],
            dag_name=context['dag_name']
        )
        hdfs_raw_path = hdfs.generate_hdfs_path(
            dag_id=context["dag_name"],
            env=context["env"],
            layer=context["origin_layer"],
            date=context['date']
        )
        hdfs_pre_processed_path = hdfs.generate_hdfs_path(
            dag_id=f'{context["dag_name"]}',
            env=context["env"],
            layer=context["dest_layer"],
            date=context['date']
        )

        self.log.info(f'\n{template}\npre_processing data: {context["date"]}\n{template}')
        df = hdfs.load_pyspark_df(
            spark=spark,
            data_format='avro',
            path=hdfs_raw_path
        )

        df_pre_processed = pre_process_content(
            df=df,
            result_col_name='file',
            table_blob_col_blob=context['table_blob_col_blob']
        )

        self.log.info(f'Cleaning up registries...')
        df_cleansing = cleansing_data(
            df=df_pre_processed,
            pre_processed='file',
            result_col_name='file'
        )

        self.log.info(f'Writing data at {hdfs_pre_processed_path}')
        hdfs.save_pyspark_df(
            df=df_cleansing,
            avro_schema=avro_schema,
            compress_type='snappy',
            mode='overwrite',
            format='avro',
            partitions=n_partitions,
            hdfs_path=hdfs_pre_processed_path
        )

        self.log.info(f'\n***** REPORT *****\n'
                      f'Date             = {context["date"]}\n'
                      f'Local            = {hdfs_pre_processed_path}\n'
                      f'Total time       = {time.time() - start} sec\n'
                      f'Total rows       = {df_cleansing.count()}\n'
                      f'Total partitions = {df_cleansing.rdd.getNumPartitions()}')
