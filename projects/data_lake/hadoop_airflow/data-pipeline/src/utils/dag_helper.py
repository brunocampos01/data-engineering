from airflow.exceptions import AirflowFailException
import logging
from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.db.redis_helper import RedisHelper


def clear_environment(**context) -> None:
    RedisHelper(context['redis_conn_id']) \
        .clear_redis(dag_name=context['dag_name'])
    AirflowMetaStoreHelper(context['airflow_conn_id']) \
        .delete_airflow_var(dag_name=context['dag_name'],
                            last_control_var=context['last_control_var'])


def clear_airflow_var(**context) -> None:
    AirflowMetaStoreHelper(context['airflow_conn_id']) \
        .delete_pattern_var(context['list_pattern_name_var'])


def crash_dag() -> None:
    raise AirflowFailException()


def check_next_dag(**context) -> [str, dict]:
    if context["doc_type"] == 'xml':
        return context['true_case']

    return context['false_case']


def check_len_list_processed_dates(**context) -> [str, dict]:
    if len(context["list_all_dates"]) >= 0:
        logging.info(f"List contains {len(context['list_all_dates'])} elements")
        return context['true_case']

    self.log.info(f"List not contains elements")
    return context['false_case']
