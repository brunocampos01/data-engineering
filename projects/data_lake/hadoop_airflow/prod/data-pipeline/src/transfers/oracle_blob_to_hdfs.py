import time

import pyspark
from airflow.models import BaseOperator
from airflow.models import Variable
from hooks.hdfs.hdfs_helper import HdfsHelper
from pyspark.util import Py4JJavaError
from utils.data_helper import get_pyspark_df_to_process
from utils.data_helper import preprocess_data_doc
from utils.init_spark import init_spark
from utils.pandas_helper import get_pandas_df_to_process
from utils.pandas_helper import pandas_preprocess_acc
from utils.pandas_helper import pandas_preprocess_data_doc
from utils.pandas_helper import pandas_preprocess_dimp


class OracleBlobToHdfsTransfer(BaseOperator):
    def __init__(
        self,
        dag_name,
        current_dag_name,
        query_id,
        table_ctrl_col_fk,
        extra_cols,
        oracle_conn_blob,
        table_blob_col_pk,
        table_blob_col_blob,
        path_avro_schema,
        path_local_avro_schemas,
        total_pg,
        layer,
        env,
        step,
        executor_cores,
        executor_memory,
        executor_instances,
        driver_memory,
        path_ojdbc,
        path_spark_avro,
        path_native_lib,
        col_type_pk,
        compress_type,
        oracle_driver,
        list_current_dates,
        hdfs_conn_id,
        oracle_conn_id,
        *args,
        **kwargs
    ):
        super(OracleBlobToHdfsTransfer, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.current_dag_name = current_dag_name
        self.oracle_conn_id = oracle_conn_id
        self.query_id = query_id
        self.table_ctrl_col_fk = table_ctrl_col_fk
        self.extra_cols = extra_cols
        self.oracle_conn_blob = oracle_conn_blob
        self.table_blob_col_pk = table_blob_col_pk
        self.table_blob_col_blob = table_blob_col_blob
        self.path_avro_schema = path_avro_schema
        self.path_local_avro_schemas = path_local_avro_schemas
        self.total_pg = total_pg
        self.layer = layer
        self.env = env
        self.step = step
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.executor_instances = executor_instances
        self.driver_memory = driver_memory
        self.path_ojdbc = path_ojdbc
        self.path_spark_avro = path_spark_avro
        self.path_native_lib = path_native_lib
        self.col_type_pk = col_type_pk
        self.compress_type = compress_type
        self.hdfs_conn_id = hdfs_conn_id
        self.oracle_driver = oracle_driver
        self.list_current_dates = list_current_dates

    def execute(self, context):
        template = '-' * 79
        hdfs = HdfsHelper(hdfs_conn=self.hdfs_conn_id)
        spark, sc = init_spark(
            app_name=f'{self.step}_{self.dag_name}',
            step=self.step,
            env=self.env,
            dag_name=self.dag_name,
            layer=self.layer,
            path_ojdbc=self.path_ojdbc,
            path_spark_avro=self.path_spark_avro,
            path_native_lib=self.path_native_lib,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            executor_instances=self.executor_instances,
            driver_memory=self.driver_memory
        )
        avro_schema = hdfs.read_avro_schema(
            path_avro_schema=self.path_avro_schema,
            layer=self.layer,
            dag_name=self.dag_name
        )

        for date in self.list_current_dates:
            start = time.time()
            query_blob = Variable.get(f'{self.current_dag_name}_sql_blob_{date}')
            n_partitions = int(
                Variable.get(f'{self.current_dag_name}_total_pg_{date}'))  # 1 pg = 1000 rows
            hdfs_path = hdfs.generate_hdfs_path(
                dag_id=self.dag_name,
                env=self.env,
                layer=self.layer,
                date=date
            )

            self.log.info(f'\n{template}\nExtracting from Oracle: {date}\n{template}')
            try:
                if self.dag_name in ['acc', 'dimp']:
                    raise ValueError('Switch execution pyspark to pandas ...')

                df = get_pyspark_df_to_process(
                    oracle_conn_id=self.oracle_conn_id,
                    oracle_conn_blob=self.oracle_conn_blob,
                    oracle_driver=self.oracle_driver,
                    spark=spark,
                    n_partitions=n_partitions,
                    query_blob=query_blob,
                    table_blob_col_pk=self.table_blob_col_pk,
                    table_blob_col_blob=self.table_blob_col_blob,
                    current_dag_name=self.current_dag_name,
                    extra_cols=self.extra_cols,
                    date=date
                )

                df_preprocessed = preprocess_data_doc(
                    df=df,
                    col_type_pk=self.col_type_pk,
                    table_blob_col_pk=self.table_blob_col_pk,
                    table_blob_col_blob=self.table_blob_col_blob
                )

                # load dataframe into HDFS before preprocess decrease shuffle operation
                self.log.info(f'\n{template}\nWriting avro in HDFS\n{template}')
                df_preprocessed.explain()

                hdfs.save_pyspark_df(
                    df=df_preprocessed,
                    avro_schema=avro_schema,
                    compress_type=self.compress_type,
                    mode='append',
                    format='avro',
                    partitions=n_partitions,
                    hdfs_path=hdfs_path
                )

                self.log.info(f'\n***** REPORT *****\n'
                              f'Date             = {date}\n'
                              f'Local            = {hdfs_path}\n'
                              f'Total time       = {time.time() - start} sec\n'
                              f'Total rows       = {df_preprocessed.count()}\n'
                              f'Total partitions = {df_preprocessed.rdd.getNumPartitions()}')

            except (ValueError, Py4JJavaError, pyspark.sql.utils.PythonException) as err:
                "E.g: ValueError: can not serialize object larger than 2G"
                self.log.error(f"\n{template}\n{err.__class__} occurred !!!\n{template}\n")
                self.log.info(f'\n{template}\nExecuting Pandas\n{template}')

                pdf = get_pandas_df_to_process(
                    oracle_conn_id=self.oracle_conn_id,
                    oracle_conn_blob=self.oracle_conn_blob,
                    query_blob=query_blob,
                    table_blob_col_pk=self.table_blob_col_pk,
                    table_blob_col_blob=self.table_blob_col_blob,
                    extra_cols=self.extra_cols,
                    current_dag_name=self.current_dag_name,
                    date=date
                )

                # TODO: create function
                # >========================================================
                if self.dag_name == 'acc':
                    data = pandas_preprocess_acc(
                        pdf=pdf,
                        table_blob_col_blob=self.table_blob_col_blob,
                        compress_type=self.compress_type,
                        avro_schema=self.path_local_avro_schemas
                    )
                elif self.dag_name == 'dimp':
                    data = pandas_preprocess_dimp(
                        pdf=pdf,
                        table_blob_col_blob=self.table_blob_col_blob,
                        compress_type=self.compress_type,
                        avro_schema=self.path_local_avro_schemas
                    )
                else:
                    data = pandas_preprocess_data_doc(
                        pdf=pdf,
                        table_blob_col_pk=self.table_blob_col_pk,
                        table_blob_col_blob=self.table_blob_col_blob,
                        compress_type=self.compress_type,
                        avro_schema=self.path_local_avro_schemas
                    )
                # >========================================================

                self.log.info(f'\n{template}\nWriting avro in HDFS\n{template}')
                hdfs \
                    .load_data(hdfs_path=hdfs_path,
                               data=data,
                               hdfs_filename=f'part-0-mapred_{self.current_dag_name}_{date}.avro')

                self.log.info(f'\n***** REPORT *****\n'
                              f'Date             = {date}\n'
                              f'Local            = {hdfs_path}\n'
                              f'Total time       = {time.time() - start} sec\n'
                              f'Total rows       = {len(pdf.index)}')
