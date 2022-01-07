import logging
import time

from humanfriendly import format_timespan


def start_time(**context) -> None:
    start = time.time()
    context['ti'].xcom_push(key=context['dag_name'] + '-start_dag', value=start)
    logging.info(start)


def end_time(**context) -> None:
    start = context['ti'].xcom_pull(key=context['dag_name'] + '-start_dag')
    end = format_timespan(time.time() - start)
    logging.info(end)
