import json
import os
from datetime import datetime
from datetime import timedelta

import airflow
import urllib3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.hdfs.hdfs_helper import HdfsHelper
from operators.hdfs_concat_files import HdfsConcatFiles
from operators.oracle_get_results import OracleGetResults
from operators.oracle_update_control_var import OracleUpdateControlVar
from operators.sync_data import SyncData
from operators.update_stats_by_table import UpdateStasByTable
from transfers.oracle_table_to_hdfs import OracleTableToHdfsTransfer
from utils.config_helper import get_list_docs
from utils.config_helper import read_data_config
from utils.dag_helper import clear_airflow_var
from utils.time_handler import end_time
from utils.time_handler import start_time

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
    max_registry_by_file: str,
    db_name: str,
    table_name: str,
    col_control_var: str,
    col_name_dt_ref: str,
    oracle_conn_id: str,
    path_avro_schema: str,
    path_config_docs: str,
    executor_cores: str,
    executor_memory: str,
    executor_instances: str,
    driver_memory: str
) -> airflow.models.dag.DAG:
    # -----------------
    #        DAG
    # -----------------
    args = {
        'owner': 'job',
        'run_as_user': 'job',
        'start_date': datetime(2021, 8, 1),
        'do_xcom_push': False,
        'depends_on_past': True,
        'retries': 10,
        'retry_delay': timedelta(seconds=90),
        'dag_name': dag_name
    }

    with DAG(dag_id=f'{step}_{dag_name}',
             description=f'Importa os dados de {dag_name}',
             # schedule_interval=None,
             schedule_interval='00 19 * * *',
             catchup=False,
             default_args=args) as dag:
        dag.doc_md = __doc__
        dag.doc_md = """![image alt <](../big_data.wiki/.attachments/xpto_company.png)"""
        layer = 'raw'
        env = Variable.get('env', default_var='prod')
        last_control_var = Variable.get(f'{dag_name}_last_control_var', default_var='000000000000000')
        control_var = f"{int(Variable.get(f'{dag_name}_control_var', default_var='000000000000000')):015d}"
        current_dag_name = dag_name + '_' + control_var
        total_rows = Variable.get('total_rows', default_var='100000')

        sql_get_data = f'''
        SELECT
            *
        FROM {db_name}.{table_name}
        WHERE
            {col_control_var} > :control_var
            AND TO_DATE(to_char({col_name_dt_ref}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')
        ORDER BY {col_control_var} ASC
        FETCH FIRST :total_rows ROWS ONLY'''
        dict_bind_sql_get_data = {
            'control_var': control_var,
            'total_rows': total_rows
        }

        sql_count_id = f'''
        SELECT COUNT({col_control_var})
        FROM {db_name}.{table_name}
        WHERE
            {col_control_var} > :control_var
            AND TO_DATE(to_char({col_name_dt_ref}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')'''
        dict_bind_sql_count_id = {
            'control_var': control_var
        }

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
                       'true_case': 'extract_transform_load',
                       'false_case': 'sync_data'}
        )

        task_extract_transform_load = OracleTableToHdfsTransfer(
            task_id=f'extract_transform_load',
            retries=20,
            dag_name=dag_name,
            current_dag_name=current_dag_name,
            sql_get_data=sql_get_data,
            dict_bind=dict_bind_sql_get_data,
            col_control_var=col_control_var,
            path_avro_schema=path_avro_schema,
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
            compress_type='snappy',
            oracle_conn_id=oracle_conn_id,
            hdfs_conn_id='webhdfs',
            oracle_driver='oracle.jdbc.driver.OracleDriver',
            max_registry_by_file=max_registry_by_file
        )

        task_sync_data = SyncData(
            task_id='sync_data',
            dag_name=dag_name,
            db_name=db_name,
            table_name=table_name,
            col_name_control_var=col_control_var,
            col_name_dt_ref=col_name_dt_ref,
            path_avro_schema=path_avro_schema,
            layer=layer,
            env=env,
            hdfs_conn_id='webhdfs',
            oracle_conn_id=oracle_conn_id,
            oracle_driver='oracle.jdbc.driver.OracleDriver',
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            executor_instances=executor_instances,
            driver_memory=driver_memory,
            path_ojdbc=path_ojdbc,
            path_spark_avro=path_spark_avro,
            path_native_lib=path_native_lib,
            compress_type='snappy',
            max_registry_by_file=max_registry_by_file
        )

        task_concat_file = HdfsConcatFiles(
            task_id=f'hdfs_concat_file',
            retries=100,
            dag_name=dag_name,
            layer=layer,
            env=env,
            col_name_control_var=col_control_var,
            path_avro_schema=path_avro_schema,
            hdfs_conn_id='webhdfs',
            executor_cores=executor_cores,
            executor_memory=executor_memory,
            driver_memory=driver_memory,
            path_ojdbc=path_ojdbc,
            path_spark_avro=path_spark_avro,
            path_native_lib=path_native_lib,
            format_data='parquet',
            compress_type='snappy',
            max_registry_by_avro=max_registry_by_file
        )

        task_save_execution_state_hdfs = PythonOperator(
            task_id='save_execution_state_hdfs',
            python_callable=HdfsHelper('webhdfs').save_execution_state_hdfs,
            op_kwargs={'dag_name': dag_name,
                       'control_var': control_var,
                       'layer': layer}
        )

        task_update_statistics = UpdateStasByTable(
            task_id=f'update_statistics',
            retries=50,
            db_name=dag_name,
            table_name=layer,
            impala_conn_id='impala'
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

        task_update_control_var = OracleUpdateControlVar(
            task_id='update_control_var',
            trigger_rule=TriggerRule.ALL_SUCCESS,
            depends_on_past=True,
            control_var=control_var,
            last_control_var=last_control_var,
            dag_name=dag_name,
            current_dag_name=current_dag_name,
            col_control_var=col_control_var,
            oracle_conn_id=oracle_conn_id,
            dict_bind=dict_bind_sql_get_data,
            sql=sql_get_data
        )

        task_trigger_import_file = TriggerDagRunOperator(
            task_id=f'trigger_import_file_{dag_name}',
            trigger_dag_id=dag.dag_id
        )

        task_end = PythonOperator(
            task_id='end',
            trigger_rule=TriggerRule.ALL_DONE,
            python_callable=end_time,
            op_kwargs={'current_dag_name': current_dag_name,
                       'dag_name': dag_name,
                       'last_control_var_name': f'{dag_name}_last_control_var',
                       'postgres_conn_id': 'airflow_db'}
        )

    # -----------------
    #      GRAPH
    # -----------------
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_extract_transform_load >> task_update_control_var >> task_trigger_import_file >> task_end
    task_start >> task_oracle_execute_count >> task_check_if_contains_data_in_oracle >> task_sync_data >> [
        task_concat_file,
        task_save_execution_state_hdfs] >> task_update_statistics >> task_clear_airflow_var >> task_end

    return dag


# -----------------
# DAG GENERATOR
# -----------------
for doc in get_list_docs(path=''.join(here + '/../configs/data/')):
    config = read_data_config(data_name=doc)

    if config[doc]['doc_type'] == 'table':
        globals()[doc] = create_dag(
            dag_name=config[doc]['name'],
            db_name=config[doc]['source']['db_name'],
            table_name=config[doc]['source']['table_name'],
            col_control_var=config[doc]['source']['cols']['control_var'],
            col_name_dt_ref=config[doc]['source']['cols']['dt_ref'],
            oracle_conn_id=config[doc]['source']['oracle_conn'],
            path_avro_schema=dag_config['paths']['path_hdfs']['path_avro_schemas'],
            max_registry_by_file=config[doc]['max_registry_by_file'],
            path_config_docs=''.join(here + f'/../configs/data/{doc}.json'),
            executor_cores=config[doc]['executor_cores'],
            executor_memory=config[doc]['executor_memory'],
            executor_instances=config[doc]['executor_instances'],
            driver_memory=config[doc]['driver_memory']
        )
