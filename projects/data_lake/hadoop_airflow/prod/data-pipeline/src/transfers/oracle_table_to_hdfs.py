import time

from airflow.models import BaseOperator
from hooks.db.oracle_helper import OracleHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from utils.config_helper import read_data_config
from utils.data_helper import calculate_partitions
from utils.data_helper import convert_type_oracle_to_spark
from utils.data_helper import preprocess_data_table
from utils.init_spark import init_spark


class OracleTableToHdfsTransfer(BaseOperator):
    def __init__(
        self,
        dag_name,
        current_dag_name,
        sql_get_data,
        dict_bind,
        oracle_conn_id,
        col_control_var,
        path_avro_schema,
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
        compress_type,
        max_registry_by_file,
        oracle_driver,
        hdfs_conn_id,
        *args,
        **kwargs
    ):
        super(OracleTableToHdfsTransfer, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.current_dag_name = current_dag_name
        self.sql_get_data = sql_get_data
        self.dict_bind = dict_bind
        self.oracle_conn_id = oracle_conn_id
        self.col_control_var = col_control_var
        self.path_avro_schema = path_avro_schema
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
        self.compress_type = compress_type
        self.hdfs_conn_id = hdfs_conn_id
        self.oracle_driver = oracle_driver
        self.max_registry_by_file = max_registry_by_file

    def execute(self, context):
        template = '-' * 79
        start = time.time()
        hdfs = HdfsHelper(hdfs_conn=self.hdfs_conn_id)
        oracle = OracleHelper(self.oracle_conn_id)

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
        avro_schema = hdfs \
            .read_avro_schema(path_avro_schema=self.path_avro_schema,
                              layer=self.layer,
                              dag_name=self.dag_name)

        hdfs_path = hdfs \
            .generate_hdfs_path(dag_id=self.dag_name,
                                env=self.env,
                                layer=self.layer,
                                is_partitioned=False)

        self.log.info(f'\n{template}\nGetting data from Oracle\n{template}')
        self.log.info(f'query:{self.sql_get_data}\n parameters:\n{self.dict_bind}')
        records = oracle.get_rows_with_bind(sql=self.sql_get_data,
                                            bind=self.dict_bind)
        list_dict_cols = read_data_config(self.dag_name)[self.dag_name]['hdfs_data_schema']['raw'][
            'cols']
        df_oracle_table = convert_type_oracle_to_spark(spark=spark,
                                                       records=records,
                                                       list_dict_cols=list_dict_cols)

        df_preprocessed = preprocess_data_table(df_oracle_table)
        df_preprocessed.explain()
        df_preprocessed.printSchema()
        df_preprocessed.show(n=1)

        total_registry = df_oracle_table.count()
        n_partitions = calculate_partitions(total_registry=total_registry,
                                            max_registry_by_avro=int(self.max_registry_by_file))
        # TODO: analyze and test ORC (accept ACID)
        self.log.info(f'\n{template}\nWriting table in HDFS\n{template}')
        hdfs.save_pyspark_df(
            df=df_preprocessed,
            format='parquet',
            avro_schema=avro_schema,
            compress_type=self.compress_type,
            mode='append',
            partitions=n_partitions,
            hdfs_path=hdfs_path
        )

        self.log.info(f'\n***** REPORT *****\n'
                      f'Local            = {hdfs_path}\n'
                      f'Total time       = {time.time() - start} sec\n'
                      f'Total rows       = {total_registry}\n'
                      f'Total partitions = {df_preprocessed.rdd.getNumPartitions()}')
