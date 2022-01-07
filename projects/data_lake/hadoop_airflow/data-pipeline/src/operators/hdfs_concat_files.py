from typing import Optional

from airflow.models import BaseOperator
from airflow.models import Variable
from hooks.hdfs.hdfs_helper import HdfsHelper
from utils.data_helper import calculate_partitions
from utils.init_spark import init_spark


class HdfsConcatFiles(BaseOperator):

    def __init__(
        self,
        dag_name,
        layer,
        env,
        col_name_control_var,
        compress_type,
        format_data,
        path_avro_schema,
        executor_cores,
        executor_memory,
        driver_memory,
        path_ojdbc,
        path_spark_avro,
        path_native_lib,
        max_registry_by_avro,
        hdfs_conn_id,
        date: Optional[str] = None,
        *args,
        **kwargs
    ):
        super(HdfsConcatFiles, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.date = date
        self.layer = layer
        self.env = env
        self.col_name_control_var = col_name_control_var
        self.path_avro_schema = path_avro_schema
        self.hdfs_conn_id = hdfs_conn_id
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.driver_memory = driver_memory
        self.path_ojdbc = path_ojdbc
        self.path_spark_avro = path_spark_avro
        self.path_native_lib = path_native_lib
        self.compress_type = compress_type
        self.format_data = format_data
        self.max_registry_by_avro = max_registry_by_avro

    def execute(self, context):  # sourcery skip: none-compare
        hdfs = HdfsHelper(hdfs_conn=self.hdfs_conn_id)

        spark, sc = init_spark(
            app_name=f'concatenate_{self.dag_name}_{self.date}',
            step='concatenate',
            env=self.env,
            dag_name=self.dag_name,
            layer=self.layer,
            path_ojdbc=self.path_ojdbc,
            path_spark_avro=self.path_spark_avro,
            path_native_lib=self.path_native_lib,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            executor_instances='1',
            driver_memory=self.driver_memory
        )

        if self.date is None:
            path = hdfs.generate_hdfs_path(
                dag_id=self.dag_name,
                env=self.env,
                layer=self.layer,
                is_partitioned=False
            )
        else:
            path = hdfs.generate_hdfs_path(
                dag_id=self.dag_name,
                env=self.env,
                layer=self.layer,
                date=self.date
            )

        list_filename = hdfs.list_filename(path)
        self.log.info(f'list_filename = {list_filename}')

        if len(list_filename) > 1:
            avro_schema = hdfs.read_avro_schema(
                path_avro_schema=self.path_avro_schema,
                layer=self.layer,
                dag_name=self.dag_name
            )
            df = hdfs.load_pyspark_df(
                spark=spark,
                data_format=self.format_data,
                path=f'hdfs://{path}'
            )
            total_registry = df.count()
            self.log.info(f'Total registry = {total_registry}')

            if self.date is not None:
                Variable.set(key=f'{self.dag_name}_{self.layer}_total_registry_{self.date}',
                             value=total_registry)

            n_files = calculate_partitions(total_registry=total_registry,
                                           max_registry_by_avro=int(self.max_registry_by_avro))

            self.log.info(
                f'Concatenating {total_registry} registry at {path}. Generating {n_files} files.')
            hdfs.save_pyspark_df(
                df=df,
                avro_schema=avro_schema,
                compress_type=self.compress_type,
                mode='append',
                format=self.format_data,
                partitions=n_files,
                hdfs_path=path
            )

            self.log.info(f'Deleting file {path}')
            hdfs.remove_list_files(path=path, list_filename=list_filename)

        else:
            self.log.info(f'Files already concatenate')
