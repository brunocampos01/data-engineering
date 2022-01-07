import logging
from pprint import pprint
from typing import Optional

import airflow
from airflow import models
from airflow import settings
from airflow.models import Connection
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from hooks.db.redis_helper import RedisHelper


class AirflowMetaStoreHelper(PostgresHook):

    def __init__(
        self,
        airflow_conn: Optional[str] = None,
        airflow_conn_type: Optional[str] = None,
        airflow_host: Optional[str] = None,
        airflow_port: Optional[str] = None,
        airflow_login: Optional[str] = None,
        airflow_passwd: Optional[str] = None,
        extra: Optional[str] = None,
        *args,
        **kwargs
    ):
        super(AirflowMetaStoreHelper, self).__init__(*args, **kwargs)
        self.airflow_conn = airflow_conn
        self.airflow_conn_type = airflow_conn_type
        self.airflow_host = airflow_host
        self.airflow_port = airflow_port
        self.airflow_login = airflow_login
        self.airflow_passwd = airflow_passwd
        self.extra = extra

    def execute_statement(self, statement: str) -> list:
        try:
            return PostgresHook(self.airflow_conn).get_records(statement)
        except Exception as err:
            self.log.error(f"Failed execute query: {err}\nquery: {statement}")
            raise

    def check_if_contains_data_in_oracle(self, **context: dict) -> [str, dict]:
        count_id = int(Variable.get(f'{context["current_dag_name"]}_total_row_id'))
        self.log.info(f'\nActual control var: {context["control_var"]}\n'
                      f'Last last_control_var: {context["last_control_var"]}')

        if count_id > 0:
            self.log.warning(f"{count_id} rows are not in HDFS.")
            return context['true_case']
        else:
            self.log.info(f"{count_id} rows. Updated HDFS.")
            return context['false_case']

    def check_if_contains_inconsistency(self, **context: dict) -> [str, dict]:
        self.log.info(f'Check if contains inconsistency between Oracle and HDFS ...')
        var_name = f'{context["dag_name"]}_{context["last_control_var"]}_{context["layer"]}_inconsistency'
        sql = f"SELECT key FROM public.variable WHERE key like '{var_name}%'"
        list_records = self.execute_statement(sql)
        self.log.info(list_records)

        if len(list_records) == 0:
            return context['false_case']

        self.log.warning(f"Inconsistency dates: {list_records[0][0]}")
        return context['true_case']

    def create_conn(self,
                    conn_type: str,
                    host: str,
                    port: int,
                    login: str,
                    passwd: str,
                    describe: str,
                    extra: Optional[str] = None) -> [airflow.models.connection.Connection, None]:
        conn_id = self.airflow_conn.lower()
        try:
            conn = Connection(conn_id=conn_id,
                              conn_type=conn_type,
                              host=host,
                              login=login,
                              password=passwd,
                              port=port,
                              description=describe,
                              extra=extra)

            session = settings.Session
            conn_name = session \
                .query(Connection) \
                .filter(Connection.conn_id == conn.conn_id) \
                .first()

            if str(conn_name) == str(conn.conn_id):
                logging.warning(f"Connection {conn.conn_id} already exists")
                return None

            session.add(conn)
            session.commit()
            logging.info(f'Connection {conn.conn_id} is created')
            return conn

        except Exception as err:
            self.log.exception(err)
            self.log.error(f'Check parameters:\n'
                           f'{conn_type}\n'
                           f'{host}\n'
                           f'{login}\n'
                           f'{passwd}\n'
                           f'{port}\n'
                           f'{extra}')
            raise

    # TODO: dag_helper
    @staticmethod
    def set_granularity(list_all_dates: list, agg_by: str) -> 'generator':
        """Change airflow variable: list_all_dates

        Parameters
        ----------
        agg_by :
            granularity, e.g: day, month
        list_all_dates :
            list contains all data processed by doc, e.g: mdfe_list_all_dates

        Examples
        --------
            input: ['01-12-2000', '02-01-2000', '02-01-2000', ...]
            output (agg_by=month): ['01-12-2000', '01-01-2000', ...]
        """
        if agg_by == 'month':
            return set((f'01-{date[3:]}' for date in list_all_dates))
        return [dt for dt in list_all_dates]

    def generate_list_inconsistency(self, dag_name: str, last_control_var: str, layer: str) -> list:
        """
        :return:
            Return a variable list generated by task compare_hdfs_and_oracle
            ex: [doc_name_111111111111117_data_inconsistency_01-01-2000,
                 doc_name_111111111111117_data_inconsistency_01-02-2000, ...]
        """
        var_name = f'{dag_name}_{last_control_var}_{layer}_inconsistency'
        tuple_var = self.execute_statement(f"SELECT key "
                                           f"FROM public.variable "
                                           f"WHERE key like '{var_name}%'")
        return [Variable.get(x[0]) for x in tuple_var]

    def delete_airflow_var(self, dag_name: str, last_control_var: str) -> None:
        tuple_var = self.execute_statement(f"SELECT id, key "
                                           f"FROM public.variable "
                                           f"WHERE key like '{dag_name + '_' + last_control_var}%'")
        list_values = (x[0] for x in tuple_var)

        for var in list_values:
            PostgresHook(self.airflow_conn) \
                .run(sql=f"DELETE FROM public.variable WHERE id='{var}'",
                     autocommit=True)
            self.log.warning(f'Delete variable: {var}')

    def delete_pattern_var(self, list_pattern_name_var: str) -> None:
        for var in eval(list_pattern_name_var):
            PostgresHook(self.airflow_conn) \
                .run(sql=f"DELETE FROM public.variable WHERE key like '{var}%'",
                     autocommit=True)
            self.log.warning(f'Delete variable pattern: {var}')

    # TODO: dag_helper
    def get_dag_name(self, **context: dict) -> None:
        self.log.info(f'Appending current dag in list_dags ...')
        context['list_dags'].append(context['current_dag_name'])
        list_dags = sorted(list(set(context['list_dags'])))

        Variable.set(key=context['name_list_dags'], value=list_dags)
        self.log.info(f'List dags = {list_dags}')

    def update_control_var(self, **context) -> None:
        self.log.info(f'Actual control_var_name: {context["control_var"]}\n'
                      f'Last last_control_var: {context["last_control_var"]}\n'
                      f'list_current_dates:{context["list_current_dates"]}\n'
                      f'list_all_dates:{context["list_all_dates"]}\n'
                      f'list_dags:{context["list_dags"]}\n'
                      f'total_pg: {context["total_pg"]}')

        list_records = RedisHelper(context['redis_conn_id']) \
            .get_list_redis(context['redis_key'])
        list_id = [x[1] for x in list_records]
        max_value = f"{max(list_id):015d}"  # 000.000.000.000.000

        Variable.set(key=f'{context["dag_name"]}_control_var', value=max_value)
        Variable.set(key=f'{context["dag_name"]}_last_control_var', value=context["control_var"])

        self.log.info(f'Updated Airflow variable:\n'
                      f'last_control_var to: {context["control_var"]}\n'
                      f'control_var to: {max_value}')

    def generate_dict_params(self, **context: dict) -> None:
        """Used next_dag"""
        list_total_partitions = [
            int(Variable.get(f'{context["current_dag_name"]}_total_pg_{date}'))
            for date in context["list_current_dates"]
        ]

        list_total_rows = [
            int(Variable.get(f'{context["current_dag_name"]}_total_rows_{date}'))
            for date in context["list_current_dates"]
        ]

        dict_params_by_date = {}
        for rows, date, part in zip(list_total_rows,
                                    context["list_all_dates"],
                                    list_total_partitions):
            d = {
                "total_rows": rows,
                "total_partitions": part,
                "processed_by_dag": context['current_dag_name']
            }
            dict_params_by_date[date] = d

        Variable.set(key=f'{context["dag_name"]}_dict_params_by_date', value=dict_params_by_date)
        self.log.info(f'dict_params_by_date:')
        pprint(dict_params_by_date)
