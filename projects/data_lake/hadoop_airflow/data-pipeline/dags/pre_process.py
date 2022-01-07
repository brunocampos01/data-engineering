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
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from operators.create_partitions import CreatePartitions
from operators.pre_process_and_clean import pre_processAndCleanData
from utils.config_helper import get_list_docs
from utils.config_helper import read_data_config
from utils.dag_helper import clear_airflow_var
from utils.dag_helper import crash_dag
from utils.time_handler import end_time
from utils.time_handler import start_time
from validators.compare_data_impala_impala import CompareDataImpalaImpala

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
path_spark_xml = os.path.join(path_libs + '/' + dag_config['libs']['spark_xml'])
path_native_lib = dag_config['paths']['path_native_lib']


def create_dag(
    dag_name: str,
    agg_by: str,
    cache_blob: str,
    path_avro_schema: str,
    executor_cores: str,
    executor_memory: str,
    driver_memory: str,
    max_registry_by_avro: str,
    table_id: str,
    table_id_col_fk: str,
    table_id_col_dt_ref: str,
    table_id_col_dt_created: str,
    table_blob_col_blob: str
) -> airflow.models.dag.DAG:
    # -----------------
    #        DAG
    # -----------------
    args = {
        'owner': 'job',
        'run_as_user': 'job',
        'start_date': datetime(2021, 4, 12),
        'do_xcom_push': False,
        'depends_on_past': True,
        'retries': 100,
        'retry_delay': timedelta(seconds=90),
        'dag_name': dag_name
    }

    with DAG(dag_id=f'{step}_{dag_name}',
             description=f'pre_processe e limpa os dados de {dag_name}',
             schedule_interval=None,
             default_args=args) as dag:
        dag.doc_md = __doc__
        dag.doc_md = """![image alt <](http://www.xpto.company/layout/site/images/logo_xpto.png)"""

        origin_layer = 'raw'
        dest_layer = 'pre_processed'
        env = Variable.get('env', default_var='dev')
        control_var = f"{int(Variable.get(f'{dag_name}_control_var', default_var='000000000000000')):015d}"
        last_control_var = Variable.get(f'{dag_name}_last_control_var', default_var='000000000000000')
        current_dag_name = dag_name + '_' + control_var
        list_all_dates = eval(Variable.get(f'{dag_name}_list_all_dates', default_var='[]'))
        list_partitions = eval(Variable.get(f'{dag_name}_list_partitions', default_var='[]'))
        list_inconsistency = eval(Variable.get(f'{dag_name}_{dest_layer}_list_inconsistency', default_var='[]'))

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
        
        with TaskGroup(group_id='group_pre_process_clean_load') as group_pre_process_clean_load:
            list_all_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                                      agg_by=agg_by)
            for date in list_all_dates:
                PythonOperator(
                    task_id=f'pre_process_clean_load-{date}',
                    python_callable=pre_processAndCleanData().execute,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    op_kwargs={
                        'dag_id': dag.dag_id,
                        'step': step,
                        'dag_name': dag_name,
                        'date': date,
                        'env': env,
                        'origin_layer': origin_layer,
                        'dest_layer': dest_layer,
                        'table_blob_col_blob': table_blob_col_blob,
                        'path_spark_xml': path_spark_xml,
                        'path_spark_avro': path_spark_avro,
                        'path_native_lib': path_native_lib,
                        'path_ojdbc': path_ojdbc,
                        'executor_cores': executor_cores,
                        'executor_memory': executor_memory,
                        'driver_memory': driver_memory,
                        'hdfs_conn_id': 'webhdfs',
                        'path_avro_schema': path_avro_schema,
                        'list_all_dates': list_all_dates,
                        'max_registry_by_avro': max_registry_by_avro
                    }
                )
        
        task_create_partitions = PythonOperator(
            task_id='create_partitions',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=CreatePartitions().execute,
            op_kwargs={'dag_name': dag_name,
                       'current_dag_name': current_dag_name,
                       'list_all_dates': list_all_dates,
                       'agg_by': agg_by,
                       'env': env,
                       'hive_conn_id': 'hive',
                       'impala_conn_id': 'impala',
                       'layer': dest_layer}
        )
        
        with TaskGroup(group_id='group_compare_data') as group_compare_data:
            list_all_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                                      agg_by=agg_by)
        
            for date in list_all_dates:
                PythonOperator(
                    task_id=f'compare_{origin_layer}_and_{dest_layer}_{date}',
                    python_callable=CompareDataImpalaImpala().execute,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    op_kwargs={'dag_name': dag_name,
                               'control_var': control_var,
                               'last_control_var': last_control_var,
                               'date': date,
                               'hive_conn_id': 'impala',
                               'origin_layer': origin_layer,
                               'dest_layer': dest_layer,
                               'redis_conn_id': cache_blob,
                               'table_id': table_id,
                               'dt_ref': table_id_col_dt_ref,
                               'agg_by': agg_by,
                               'table_id_col_fk': table_id_col_fk,
                               'table_id_col_dt_created': table_id_col_dt_created,
                               'list_partitions': list_partitions,
                               'list_all_dates': list_all_dates}
                )
        
        task_check_if_contains_inconsistency = BranchPythonOperator(
            task_id=f'check_if_contains_inconsistency',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            wait_for_downstream=True,
            python_callable=AirflowMetaStoreHelper('airflow_db').check_if_contains_inconsistency,
            op_kwargs={'dag_name': dag_name,
                       'last_control_var': last_control_var,
                       'layer': dest_layer,
                       'true_case': 'generate_list_inconsistency',
                       'false_case': 'clear_airflow_var',
                       'redis_conn_id': cache_blob,
                       'get_redis_key': f'{dag_name}_inconsistency_date'}
        )
        
        task_generate_list_inconsistency = PythonOperator(
            task_id=f'generate_list_inconsistency',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=AirflowMetaStoreHelper('airflow_db').generate_list_inconsistency,
            op_kwargs={'dag_name': dag_name,
                       'last_control_var': last_control_var,
                       'layer': dest_layer}
        )
        
        task_hdfs_clear_inconsistency_data = PythonOperator(
            task_id='hdfs_clear_inconsistency_data',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=HdfsHelper('webhdfs').clear_inconsistency_data,
            op_kwargs={'dag_name': dag_name,
                       'list_inconsistency': list_inconsistency,
                       'layer': dest_layer}
        )
        
        task_crash_dag = PythonOperator(
            task_id=f'crash_dag',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=crash_dag
        )

        task_clear_airflow_var = PythonOperator(
            task_id='clear_airflow_var',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            python_callable=clear_airflow_var,
            op_kwargs={'airflow_conn_id': 'airflow_db',
                       'list_pattern_name_var': f"["
                                                f" '{dag_name}_%_total_row_id',"
                                                f" '{dag_name}_raw_total_registry_%',"
                                                f" '{dag_name}_list_dags',"
                                                f" '{dag_name}_list_path_to_concat',"
                                                f" '{dag_name}_list_path_to_rename',"
                                                f" '{dag_name}_list_all_dates'"
                                                f"]"}
        )

        task_no_inconsistencies_were_found = DummyOperator(
            task_id='no_inconsistencies_were_found'
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
    task_start >> group_pre_process_clean_load >> task_create_partitions >> group_compare_data >> task_check_if_contains_inconsistency >> task_generate_list_inconsistency >> task_hdfs_clear_inconsistency_data >> task_crash_dag
    task_start >> group_pre_process_clean_load >> task_create_partitions >> group_compare_data >> task_check_if_contains_inconsistency >> task_clear_airflow_var >> task_no_inconsistencies_were_found >> task_end
    task_start >> task_clear_airflow_var >> task_no_inconsistencies_were_found >> task_end

    return dag


# -----------------
# DAG GENERATOR
# -----------------
for doc in get_list_docs(path=''.join(here + f'/../configs/data/')):
    config = read_data_config(data_name=doc)

    if config[doc]['doc_type'] == 'xml':
        globals()[doc] = create_dag(
            dag_name=config[doc]['name'],
            agg_by=config[doc]['agg_by'],
            path_avro_schema=dag_config['paths']['path_hdfs']['path_avro_schemas'],
            executor_cores=config[doc]['executor_cores'],
            executor_memory=config[doc]['executor_memory'],
            driver_memory=config[doc]['driver_memory'],
            cache_blob=config[doc]['cache_blob'],
            max_registry_by_avro=config[doc]['max_registry_by_avro'],
            table_id=config[doc]['source_ctrl']['table_name'],
            table_id_col_fk=config[doc]['source_ctrl']['cols']['fk'],
            table_id_col_dt_ref=config[doc]['source_ctrl']['cols']['dt_ref'],
            table_id_col_dt_created=config[doc]['source_ctrl']['cols']['dt_created'],
            table_blob_col_blob=config[doc]['source_blob']['cols']['blob']
        )
