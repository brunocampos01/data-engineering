from airflow.models import BaseOperator
from hooks.db.oracle_helper import OracleHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from pyspark.sql.functions import col
from pyspark.sql.types import LongType
from pyspark.sql.types import StringType
from utils.data_helper import calculate_partitions
from utils.init_spark import init_spark


class SyncData(BaseOperator):
    """
    Analyse row in Oracle and delete row from HDFS

    NOTE:
    Example:
        Oracle = 26902
        HDFS   = 70991

    In this case, the execution will be done to erase all the extra lines in HDFS. Result:
        Oracle = 26902
        HDFS   = 26691
    There was a difference between the source and the origin but this is to be expected!
    Import works with D-1 load, so this difference is due to changes that were made on the last day on Oracle.
    """

    def __init__(
        self,
        dag_name,
        layer,
        env,
        hdfs_conn_id,
        oracle_conn_id,
        oracle_driver,
        db_name,
        table_name,
        col_name_control_var,
        col_name_dt_ref,
        compress_type,
        path_avro_schema,
        executor_cores,
        executor_memory,
        executor_instances,
        driver_memory,
        path_ojdbc,
        path_spark_avro,
        path_native_lib,
        max_registry_by_file,
        *args,
        **kwargs
    ):
        super(SyncData, self).__init__(*args, **kwargs)
        self.dag_name = dag_name
        self.layer = layer
        self.env = env
        self.hdfs_conn_id = hdfs_conn_id
        self.oracle_conn_id = oracle_conn_id
        self.oracle_driver = oracle_driver
        self.db_name = db_name
        self.table_name = table_name
        self.col_name_control_var = col_name_control_var
        self.col_name_dt_ref = col_name_dt_ref
        self.path_avro_schema = path_avro_schema
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory
        self.executor_instances = executor_instances
        self.driver_memory = driver_memory
        self.path_ojdbc = path_ojdbc
        self.path_spark_avro = path_spark_avro
        self.path_native_lib = path_native_lib
        self.compress_type = compress_type
        self.max_registry_by_file = max_registry_by_file

    def execute(self, **context):
        template = '-' * 79
        hdfs = HdfsHelper(hdfs_conn=self.hdfs_conn_id)
        spark, sc = init_spark(
            app_name=f'sync_data-{self.dag_name}',
            step='sync',
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
        hdfs_path = hdfs.generate_hdfs_path(
            env=self.env,
            layer=self.layer,
            dag_id=self.dag_name,
            is_partitioned=False
        )

        sql_get_data = f'''
        SELECT
            {self.col_name_control_var},
            {self.col_name_dt_ref}
        FROM {self.db_name}.{self.table_name}
        WHERE
            TO_DATE(to_char({self.col_name_dt_ref}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
            < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')
        ORDER BY {self.col_name_control_var} ASC
        '''

        self.log.info(f'\n{template}\nGetting data from Oracle\n{template}')
        df_oracle_table = OracleHelper(self.oracle_conn_id) \
            .get_pyspark_df_from_table(oracle_driver=self.oracle_driver,
                                       spark=spark,
                                       table=f'({sql_get_data})',
                                       partition_col=self.col_name_control_var,
                                       n_partitions=250) \
            .orderBy(self.col_name_control_var) \
            .withColumn(self.col_name_control_var,
                        col(self.col_name_control_var).cast(LongType())) \
            .withColumn(self.col_name_dt_ref,
                        col(self.col_name_dt_ref).cast(StringType()))

        total_oracle = df_oracle_table.count()
        self.log.info(f'Total row from Oracle = {total_oracle}')

        self.log.info(f'\n{template}\nGetting data from HDFS\n{template}')
        hdfs.mv_files(hdfs_src_path=hdfs_path,
                      hdfs_dst_path=f'{hdfs_path}/../.tmp_{self.dag_name}')

        df_hdfs = hdfs \
            .load_pyspark_df(spark=spark,
                             data_format='parquet',
                             path=f'../../{hdfs_path}/../.tmp_{self.dag_name}') \
            .orderBy(self.col_name_control_var) \
            .withColumn(self.col_name_control_var, col(self.col_name_control_var).cast(LongType())) \
            .withColumn(self.col_name_dt_ref, col(self.col_name_dt_ref).cast(StringType()))

        df_hdfs_filtered = df_hdfs \
            .select(col(self.col_name_control_var),
                    col(self.col_name_dt_ref))

        total_hdfs = df_hdfs_filtered.count()
        self.log.info(f'Total row from HDFS = {total_hdfs}')

        if total_hdfs > total_oracle:
            self.log.warning(f'\n{template}\nTotal rows are not same equals!\n{template}')
            self.log.warning(f'\nOracle = {total_oracle}'
                             f'\nHDFS   = {total_hdfs}')

            self.log.info(f'\n{template}\nExecuting: df_hdfs - df_oracle_table\n{template}')
            df_row_to_delete_hdfs = df_hdfs_filtered.subtract(df_oracle_table)
            list_row_to_delete_hdfs = [
                row[0] for row in df_row_to_delete_hdfs.select(self.col_name_control_var).collect()
            ]
            self.log.info(f'Total row to delete = {df_row_to_delete_hdfs.count()}')

            self.log.info(f'\n{template}\nDeleting rows from HDFS\n{template}')
            df = df_hdfs.filter(~df_hdfs[self.col_name_control_var].isin(list_row_to_delete_hdfs))
            total_registry = df.count()
            self.log.info(f'Total row new df = {total_registry}')
            df.show(n=1, truncate=False)

            n_files = calculate_partitions(total_registry=total_registry,
                                           max_registry_by_avro=int(self.max_registry_by_file))

            self.log.info(f'\n{template}\nWriting table in HDFS\n{template}')
            hdfs.save_pyspark_df(
                df=df,
                format='parquet',
                avro_schema=avro_schema,
                compress_type=self.compress_type,
                mode='overwrite',
                partitions=n_files,
                hdfs_path=hdfs_path
            )
            hdfs.remove_all_files(hdfs_path=f'{hdfs_path}/../.tmp_{self.dag_name}')

        try:
            hdfs.mv_files(hdfs_src_path=f'{hdfs_path}/../.tmp_{self.dag_name}',
                          hdfs_dst_path=hdfs_path)
        except Exception as e:
            print(e)
