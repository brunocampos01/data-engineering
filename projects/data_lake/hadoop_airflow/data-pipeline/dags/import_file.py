import json
import os
from datetime import datetime
from datetime import timedelta

import airflow
import urllib3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.db.redis_helper import RedisHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from operators.create_partitions import CreatePartitions
from operators.generate_statistics_by_partition import GenerateStatistics
from operators.hdfs_concat_files import HdfsConcatFiles
from operators.hdfs_prepare_concat import HdfsPrepareConcat
from operators.oracle_get_results import OracleGetResults
from transfers.oracle_blob_to_hdfs import OracleBlobToHdfsTransfer
from transfers.oracle_to_redis import OracleToRedisTransfer
from utils.config_helper import get_list_docs
from utils.config_helper import read_data_config
from utils.dag_helper import check_len_list_processed_dates
from utils.dag_helper import check_next_dag
from utils.dag_helper import clear_environment
from utils.dag_helper import crash_dag
from utils.time_handler import end_time
from utils.time_handler import start_time
from validators.compare_data_oracle_impala import CompareDataOracleImpala
from validators.prepare_reprocessing_inconsistency_data import PrepareReprocessingInconsistencyData

urllib3.disable_warnings()

# configs
here = os.path.abspath(os.path.dirname(__file__))
step = os.path.splitext(os.path.basename(__file__))[0]
with open(''.join(here + '/../configs/dag_config.json'), 'r') as f:
    dag_config = json.load(f)

path_libs = dag_config['paths']['path_libs']
path_ojdbc = os.path.join(path_libs + '/' + dag_config['libs']['ojdbc'])
path_avro_tools = os.path.join(here + '/../libs/' + dag_config['libs']['avro_tools'])
path_spark_avro = os.path.join(path_libs + '/' + dag_config['libs']['spark_avro'])
path_native_lib = dag_config['paths']['path_native_lib']


def create_dag(
    dag_name: str,
    agg_by: str,
    doc_type: str,
    cache_blob: str,
    path_avro_schema: str,
    path_local_avro_schemas: str,
    executor_cores: str,
    executor_memory: str,
    executor_instances: str,
    driver_memory: str,
    col_type_pk: str,
    extra_cols: str,
    max_registry_by_file: str,
    oracle_conn_id: str,
    table_ctrl: str,
    table_ctrl_col_control_var: str,
    table_ctrl_col_fk: str,
    table_ctrl_col_dt_ref: str,
    table_ctrl_col_dt_created: str,
    oracle_conn_blob: str,
    table_blob: str,
    table_blob_col_pk: str,
    table_blob_col_blob: str
) -> airflow.models.dag.DAG:
    # -----------------
    #        DAG
    # -----------------
    args = {
        'owner': 'job',
        'run_as_user': 'job',
        'start_date': datetime(2021, 8, 17),
        'do_xcom_push': False,
        'depends_on_past': True,
        'retries': 10,
        'retry_delay': timedelta(seconds=60),
        'dag_name': dag_name
    }

    with DAG(dag_id=f'{step}_{dag_name}',
             description=f'Import data from {dag_name}',
             schedule_interval='00 19 * * *',
             catchup=False,
             default_args=args) as dag:
        dag.doc_md = __doc__
        dag.doc_md = """![image alt <](../big_data.wiki/.attachments/xpto_company.png)"""

        layer = 'raw'
        env = Variable.get('env', default_var='dev')
        control_var = f"{int(Variable.get(f'{dag_name}_control_var', default_var='000000000000000')):015d}"
        last_control_var = Variable.get(f'{dag_name}_last_control_var',
                                        default_var='000000000000000')
        current_dag_name = dag_name + '_' + control_var
        total_pg = int(Variable.get(f'{current_dag_name}_total_pg', default_var=1))
        list_all_dates = eval(Variable.get(f'{dag_name}_list_all_dates', default_var='[]'))
        list_current_dates = eval(
            Variable.get(f'{current_dag_name}_current_dates', default_var='[]'))
        list_dags = eval(Variable.get(f'{dag_name}_list_dags', default_var='[]'))
        total_rows = Variable.get('total_rows', default_var='100000')
        items_by_query = 1000

        sql_id = f'''
        SELECT
            {table_ctrl_col_fk} id,
            {table_ctrl_col_control_var} control_var,
            to_char({table_ctrl_col_dt_ref}, 'DD-MM-YYYY') dt_ref
        FROM {table_ctrl}
        WHERE
           {table_ctrl_col_control_var} > :control_var
           AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')
        ORDER BY {table_ctrl_col_control_var} ASC
        FETCH FIRST :total_rows ROWS ONLY'''
        dict_bind_sql_get_data = {'control_var': f'{control_var}',
                                  'total_rows': f'{total_rows}'}

        sql_count_id = f'''
        SELECT COUNT({table_ctrl_col_fk})
        FROM {table_ctrl}
        WHERE
           {table_ctrl_col_control_var} > :control_var
           AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')'''
        dict_bind_sql_count_id = {'control_var': f'{control_var}'}

        # -----------------
        #      TASKS
        # -----------------
        task_start = PythonOperator(
            task_id='start',
            python_callable=start_time,
            depends_on_past=False,
            op_kwargs={'dag_name': dag_name,
                       'execution_date': '{{ ts }}'}
        )

        task_oracle_execute_count = OracleGetResults(
            task_id='oracle_execute_count',
            current_dag_name=current_dag_name,
            oracle_conn_id=oracle_conn_id,
            sql_count_id=sql_count_id,
            dict_bind=dict_bind_sql_count_id
        )

        task_check_if_contains_data_in_oracle = BranchPythonOperator(
            task_id='check_if_contains_data_in_oracle',
            python_callable=AirflowMetaStoreHelper().check_if_contains_data_in_oracle,
            op_kwargs={'control_var': control_var,
                       'last_control_var': last_control_var,
                       'current_dag_name': current_dag_name,
                       'redis_conn_id': cache_blob,
                       'redis_key': f'{dag_name}_original',
                       'true_case': 'get_id',
                       'false_case': 'check_len_list_processed_dates'}
        )

        task_get_id = OracleToRedisTransfer(
            task_id='get_id',
            oracle_conn_id=oracle_conn_id,
            redis_conn_id=cache_blob,
            sql=sql_id,
            dict_bind=dict_bind_sql_get_data,
            name_redis_key=f'{dag_name}_original'
        )

        task_fill_data_gap = PythonOperator(
            task_id='fill_data_gap',
            python_callable=RedisHelper(cache_blob).fill_data_gaps,
            op_kwargs={'current_dag_name': current_dag_name,
                       'redis_conn_id': cache_blob,
                       'redis_key': f'{dag_name}_original'}
        )

        task_get_dag_name = PythonOperator(
            task_id='get_dag_name',
            python_callable=AirflowMetaStoreHelper().get_dag_name,
            op_kwargs={'current_dag_name': current_dag_name,
                       'name_list_dags': f'{dag_name}_list_dags',
                       'list_dags': list_dags}
        )

        task_get_date = PythonOperator(
            task_id='get_date',
            python_callable=RedisHelper(cache_blob).get_date,
            op_kwargs={'dag_name': dag_name,
                       'current_dag_name': current_dag_name,
                       'list_columns': "['id', 'control_var', 'date']",
                       'redis_key': current_dag_name}
        )

        task_split_id_by_date = PythonOperator(
            task_id='split_id_by_date',
            python_callable=RedisHelper(cache_blob).split_id_by_date,
            op_kwargs={'current_dag_name': current_dag_name,
                       'list_current_dates': list_current_dates,
                       'redis_key': current_dag_name}
        )

        task_generate_pagination = PythonOperator(
            task_id='generate_pagination',
            python_callable=RedisHelper(cache_blob).generate_pagination,
            op_kwargs={'current_dag_name': current_dag_name,
                       'items_by_query': items_by_query,
                       'list_current_dates': list_current_dates,
                       'redis_key': current_dag_name}
        )

        task_generate_sql_by_date = PythonOperator(
            task_id='generate_sql_by_date',
            python_callable=RedisHelper(cache_blob).generate_sql_by_date,
            op_kwargs={'current_dag_name': current_dag_name,
                       'list_current_dates': list_current_dates,
                       'oracle_conn': oracle_conn_blob,
                       'table_ctrl': table_ctrl,
                       'table_ctrl_col_fk': table_ctrl_col_fk,
                       'table_blob': table_blob,
                       'table_blob_col_pk': table_blob_col_pk,
                       'table_blob_col_blob': table_blob_col_blob,
                       'items_by_query': items_by_query,
                       'total_pg': total_pg,
                       'extra_cols': extra_cols,
                       'redis_key': current_dag_name}
        )

        task_extract_decompress_load = OracleBlobToHdfsTransfer(
            task_id=f'extract_decompress_load',
            retries=20,
            dag_name=dag_name,
            current_dag_name=current_dag_name,
            oracle_conn_id=oracle_conn_id,
            query_id=sql_id,
            table_ctrl_col_fk=table_ctrl_col_fk,
            extra_cols=extra_cols,
            oracle_conn_blob=oracle_conn_blob,
            table_blob_col_pk=table_blob_col_pk,
            table_blob_col_blob=table_blob_col_blob,
            path_avro_schema=path_avro_schema,
            path_local_avro_schemas=f'{path_local_avro_schemas}/{layer}/{dag_name}.avsc',
            total_pg=total_pg,
            layer=layer,
            env=env,
            step=step,
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            executor_instances=executor_instances,
            driver_memory=driver_memory,
            path_ojdbc=path_ojdbc,
            path_spark_avro=path_spark_avro,
            path_native_lib=path_native_lib,
            col_type_pk=col_type_pk,
            compress_type='snappy',
            hdfs_conn_id='webhdfs',
            oracle_driver='oracle.jdbc.driver.OracleDriver',
            list_current_dates=list_current_dates
        )

        task_update_control_var = PythonOperator(
            task_id='update_control_var',
            python_callable=AirflowMetaStoreHelper().update_control_var,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            depends_on_past=True,
            op_kwargs={'control_var': control_var,
                       'dag_name': dag_name,
                       'current_dag_name': current_dag_name,
                       'redis_conn_id': cache_blob,
                       'last_control_var': last_control_var,
                       'list_dags': list_dags,
                       'total_pg': total_pg,
                       'list_current_dates': list_current_dates,
                       'list_all_dates': list_all_dates,
                       'redis_key': current_dag_name}
        )

        task_clear_environment = PythonOperator(
            task_id='clear_environment',
            python_callable=clear_environment,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            op_kwargs={'control_var': control_var,
                       'dag_name': dag_name,
                       'redis_conn_id': cache_blob,
                       'airflow_conn_id': 'airflow_db',
                       'last_control_var': last_control_var,
                       'list_dags': list_dags,
                       'redis_key': current_dag_name}
        )

        task_check_len_list_processed_dates = BranchPythonOperator(
            task_id='check_len_list_processed_dates',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=check_len_list_processed_dates,
            op_kwargs={'dag_name': dag_name,
                       'list_all_dates': list_all_dates,
                       'true_case': 'prepare_execution',
                       'false_case': 'waiting_execution'}
        )

        task_prepare_execution = DummyOperator(
            task_id='prepare_execution'
        )

        with TaskGroup(group_id='group_hdfs_concat_file') as group_hdfs_concat_file:
            task_hdfs_prepare_concat = PythonOperator(
                task_id='hdfs_prepare_concat',
                trigger_rule=TriggerRule.ALL_SUCCESS,
                python_callable=HdfsPrepareConcat('webhdfs').execute,
                op_kwargs={'dag_name': dag_name,
                           'current_dag_name': current_dag_name,
                           'hdfs_path': f'/data/{env}/{layer}/{dag_name}',
                           'agg_by': agg_by,
                           'layer': layer,
                           'env': env,
                           'list_all_dates': list_all_dates,
                           'path_avro_tools': path_avro_tools}
            )

            # TODO: refactor -> create a task
            list_all_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                                      agg_by=agg_by)
            for date in list_all_dates:
                task_concat_file = HdfsConcatFiles(
                    task_id=f'hdfs_concat_file-{date}',
                    retries=100,
                    dag_name=dag_name,
                    date=date,
                    layer=layer,
                    env=env,
                    col_name_control_var=table_ctrl_col_control_var,
                    path_avro_schema=path_avro_schema,
                    hdfs_conn_id='webhdfs',
                    executor_cores=executor_cores,
                    executor_memory=executor_memory,
                    driver_memory=driver_memory,
                    path_ojdbc=path_ojdbc,
                    path_spark_avro=path_spark_avro,
                    path_native_lib=path_native_lib,
                    format_data='avro',
                    compress_type='snappy',
                    max_registry_by_avro=max_registry_by_file
                )
                task_hdfs_prepare_concat >> task_concat_file

        task_create_partitions = PythonOperator(
            task_id='create_partitions',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=CreatePartitions().execute,
            op_kwargs={'dag_name': dag_name,
                       'current_dag_name': current_dag_name,
                       'list_all_dates': list_all_dates,
                       'hive_conn_id': 'hive',
                       'impala_conn_id': 'impala',
                       'agg_by': agg_by,
                       'layer': layer,
                       'env': env}
        )

        task_save_execution_state_hdfs = PythonOperator(
            task_id='save_execution_state_hdfs',
            python_callable=HdfsHelper('webhdfs').save_execution_state_hdfs,
            op_kwargs={'dag_name': dag_name,
                       'layer': layer,
                       'control_var': control_var}
        )

        with TaskGroup(group_id='group_generate_statistics') as group_generate_statistics:
            # TODO: refactor -> create a task
            list_all_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                                      agg_by=agg_by)

            for date in list_all_dates:
                PythonOperator(
                    task_id=f'generate_statistics-{date}',
                    retries=50,
                    python_callable=GenerateStatistics().execute,
                    op_kwargs={'dag_name': dag_name,
                               'date': date,
                               'layer': layer,
                               'impala_conn_id': 'impala',
                               'hive_conn_id': 'hive'}
                )

        with TaskGroup(group_id='group_check_data_quality') as group_check_data_quality:
            # TODO: refactor -> create a task
            list_all_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                                      agg_by=agg_by)

            for date in list_all_dates:
                CompareDataOracleImpala(
                    task_id=f'compare_oracle_impala_{date}',
                    retries=100,
                    dag_name=dag_name,
                    last_control_var=last_control_var,
                    layer=layer,
                    date=date,
                    table_ctrl=table_ctrl,
                    dt_ref=table_ctrl_col_dt_ref,
                    agg_by=agg_by,
                    oracle_conn_id=oracle_conn_id,
                    hive_conn='impala',
                    table_ctrl_col_fk=table_ctrl_col_fk,
                    table_ctrl_col_dt_created=table_ctrl_col_dt_created
                )

        task_check_if_contains_inconsistency = BranchPythonOperator(
            task_id=f'check_if_contains_inconsistency',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            wait_for_downstream=True,
            python_callable=AirflowMetaStoreHelper('airflow_db').check_if_contains_inconsistency,
            op_kwargs={'dag_name': dag_name,
                       'last_control_var': last_control_var,
                       'layer': layer,
                       'true_case': 'prepare_reprocessing_inconsistency_data',
                       'false_case': f'check_next_dag',
                       'redis_conn_id': cache_blob,
                       'redis_key': f'{dag_name}_inconsistency_date'}
        )

        task_prepare_reprocessing_inconsistency_data = PrepareReprocessingInconsistencyData(
            task_id=f'prepare_reprocessing_inconsistency_data',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            dag_name=dag_name,
            current_dag_name=current_dag_name,
            layer=layer,
            last_control_var=last_control_var,
            list_all_dates=list_all_dates,
            table_ctrl=table_ctrl,
            table_ctrl_col_fk=table_ctrl_col_fk,
            table_ctrl_col_control_var=table_ctrl_col_control_var,
            table_ctrl_col_dt_ref=table_ctrl_col_dt_ref,
            table_ctrl_col_dt_created=table_ctrl_col_dt_created,
            hive_conn_id='impala',
            hdfs_conn_id='webhdfs',
            airflow_conn_id='airflow_db',
            oracle_conn_id=oracle_conn_id
        )

        task_crash_dag = PythonOperator(
            task_id=f'crash_dag',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=crash_dag,
        )

        task_check_next_dag = BranchPythonOperator(
            task_id='check_next_dag',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=check_next_dag,
            op_kwargs={'dag_name': dag_name,
                       'doc_type': doc_type,
                       'true_case': f'trigger_pre_process_{dag_name}',
                       'false_case': f'trigger_parser_{dag_name}'}
        )

        task_trigger_pre_process = TriggerDagRunOperator(
            task_id=f'trigger_pre_process_{dag_name}',
            trigger_dag_id=f"pre_process_{dag_name}"
        )

        task_trigger_parser = TriggerDagRunOperator(
            task_id=f'trigger_parser_{dag_name}',
            trigger_dag_id=f"parser_{dag_name}"
        )

        task_trigger_import_file = TriggerDagRunOperator(
            task_id=f'trigger_import_file_{dag_name}',
            trigger_dag_id=dag.dag_id
        )

        task_waiting_execution = DummyOperator(
            trigger_rule=TriggerRule.ALL_DONE,
            task_id='waiting_execution'
        )

        task_end = PythonOperator(
            task_id='end',
            python_callable=end_time,
            op_kwargs={'current_dag_name': current_dag_name,
                       'dag_name': dag_name,
                       'last_control_var_name': f'{dag_name}_last_control_var',
                       'list_dates': f'{current_dag_name}_list_dates',
                       'postgres_conn_id': 'airflow_db'}
        )

    # -----------------
    #      GRAPH
    # -----------------
    # task_check_if_contains_data_in_oracle: true
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_get_id >> task_fill_data_gap >> [
        task_get_date,
        task_get_dag_name] >> task_split_id_by_date >> task_generate_pagination >> task_generate_sql_by_date >> task_extract_decompress_load >> task_update_control_var >> [
        task_clear_environment, task_trigger_import_file] >> task_waiting_execution >> task_end

    # task_check_if_contains_data_in_oracle: false
    #   task_check_len_list_processed_dates: true
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_check_len_list_processed_dates >> task_prepare_execution >> [
        group_hdfs_concat_file, task_save_execution_state_hdfs] >> task_create_partitions >> [
        group_check_data_quality, group_generate_statistics] >> task_check_if_contains_inconsistency
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_check_len_list_processed_dates >> task_prepare_execution >> [
        group_hdfs_concat_file,
        task_save_execution_state_hdfs] >> task_create_partitions >> task_check_if_contains_inconsistency >> task_prepare_reprocessing_inconsistency_data >> task_crash_dag

    # task_check_next_dag: true
    task_check_if_contains_inconsistency >> task_check_next_dag >> task_trigger_pre_process >> task_waiting_execution >> task_end
    # task_check_next_dag: false
    task_check_if_contains_inconsistency >> task_check_next_dag >> task_trigger_parser >> task_waiting_execution >> task_end

    # task_check_if_contains_data_in_oracle: false
    #   task_check_len_list_processed_dates: false
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_check_len_list_processed_dates >> task_waiting_execution >> task_end

    return dag


# -----------------
# DAG GENERATOR
# -----------------
for doc in get_list_docs(path=''.join(here + '/../configs/data/')):
    config = read_data_config(data_name=doc)

    if config[doc]['doc_type'] != 'table':
        globals()[doc] = create_dag(
            dag_name=config[doc]['name'],
            agg_by=config[doc]['agg_by'],
            doc_type=config[doc]['doc_type'],
            path_avro_schema=dag_config['paths']['path_hdfs']['path_avro_schemas'],
            path_local_avro_schemas=dag_config['paths']['path_local_avro_schemas'],
            executor_cores=config[doc]['executor_cores'],
            executor_memory=config[doc]['executor_memory'],
            executor_instances=config[doc]['executor_instances'],
            driver_memory=config[doc]['driver_memory'],
            cache_blob=config[doc]['cache_blob'],
            max_registry_by_file=config[doc]['max_registry_by_avro'],
            col_type_pk=config[doc]['hdfs_data_schema']['raw']['cols'][0]['type'],
            extra_cols=config[doc]['source_ctrl']['cols']['extra_cols'],
            oracle_conn_id=config[doc]['source_ctrl']['oracle_conn'],
            table_ctrl=config[doc]['source_ctrl']['table_name'],
            table_ctrl_col_fk=config[doc]['source_ctrl']['cols']['fk'],
            table_ctrl_col_control_var=config[doc]['source_ctrl']['cols']['control_var'],
            table_ctrl_col_dt_ref=config[doc]['source_ctrl']['cols']['dt_ref'],
            table_ctrl_col_dt_created=config[doc]['source_ctrl']['cols']['dt_created'],
            oracle_conn_blob=config[doc]['source_blob']['oracle_conn'],
            table_blob=config[doc]['source_blob']['table_name'],
            table_blob_col_pk=config[doc]['source_blob']['cols']['pk'],
            table_blob_col_blob=config[doc]['source_blob']['cols']['blob']
        )
