import logging
from pprint import pprint
from typing import optional, Dict

import airflow
from airflow import models
from airflow import settings
from airflow.models import connection
from airflow.models import variable
from airflow.providers.postgres.hooks.postgres import postgreshook
from hooks.db.redis_helper import redishelper


class airflowmetastorehelper(postgreshook):
    def __init__(
        self,
        airflow_conn: optional[str] = none,
        airflow_conn_type: optional[str] = none,
        airflow_host: optional[str] = none,
        airflow_port: optional[str] = none,
        airflow_login: optional[str] = none,
        airflow_passwd: optional[str] = none,
        extra: optional[str] = none,
        *args,
        **kwargs
    ):
        super(airflowmetastorehelper, self).__init__(*args, **kwargs)
        self.airflow_conn = airflow_conn
        self.airflow_conn_type = airflow_conn_type
        self.airflow_host = airflow_host
        self.airflow_port = airflow_port
        self.airflow_login = airflow_login
        self.airflow_passwd = airflow_passwd
        self.extra = extra

    def execute_statement(self, statement: str) -> list:
        try:
            return postgreshook(self.airflow_conn).get_records(statement)
        except Exception as err:
            self.log.error(f"failed execute query: {err}\nquery: {statement}")
            raise

    def check_if_contains_data_in_oracle(self, **context: Dict) -> Dict:
        count_id = int(variable.get(f'{context["current_dag_name"]}_total_row_id'))
        self.log.info(f'\nactual control var: {context["control_var"]}\n'
                      f'last last_control_var: {context["last_control_var"]}')

        if count_id > 0:
            self.log.warning(f"{count_id} rows are not in hdfs.")
            return context['true_case']
        else:
            self.log.info(f"{count_id} rows. updated hdfs.")
            return context['false_case']

    def check_if_contains_inconsistency(self, **context: Dict) -> [str, Dict]:
        self.log.info(f'check if contains inconsistency between oracle and hdfs ...')
        var_name = f'{context["dag_name"]}_{context["last_control_var"]}_{context["layer"]}_inconsistency'
        sql = f"select key from public.variable where key like '{var_name}%'"
        list_records = self.execute_statement(sql)
        self.log.info(list_records)

        if len(list_records) == 0:
            return context['false_case']

        self.log.warning(f"inconsistency dates: {list_records[0][0]}")
        return context['true_case']

    @staticmethod
    def set_granularity(list_all_dates: list, agg_by: str) -> 'generator':
        """change airflow variable: list_all_dates

        parameters
        ----------
        agg_by :
            granularity, e.g: day, month
        list_all_dates :
            list contains all data processed by doc, e.g: mdfe_list_all_dates

        examples
        --------
            input: ['01-12-2000', '02-01-2000', '02-01-2000', ...]
            output (agg_by=month): ['01-12-2000', '01-01-2000', ...]
        """
        if agg_by == 'month':
            return set((f'01-{date[3:]}' for date in list_all_dates))
        return [dt for dt in list_all_dates]

    # todo: dag_helper
    def create_conn(
        self,
        conn_type: str,
        host: str,
        port: int,
        login: str,
        passwd: str,
        describe: str,
        extra: optional[str] = None,
    ) -> [airflow.models.connection.connection, None]:
        conn_id = self.airflow_conn.lower()
        try:
            conn = connection(conn_id=conn_id,
                              conn_type=conn_type,
                              host=host,
                              login=login,
                              password=passwd,
                              port=port,
                              description=describe,
                              extra=extra)

            session = settings.session
            conn_name = session \
                .query(connection) \
                .filter(connection.conn_id == conn.conn_id) \
                .first()

            if str(conn_name) == str(conn.conn_id):
                logging.warning(f"connection {conn.conn_id} already exists")
                return None

            session.add(conn)
            session.commit()
            logging.info(f'connection {conn.conn_id} is created')
            return conn

        except Exception as err:
            self.log.exception(err)
            self.log.error(f'check parameters:\n'
                           f'{conn_type}\n'
                           f'{host}\n'
                           f'{login}\n'
                           f'{passwd}\n'
                           f'{port}\n'
                           f'{extra}')
            raise

    def generate_list_inconsistency(self, dag_name: str, last_control_var: str, layer: str) -> List:
        """
        :return:
            return a variable list generated by task compare_hdfs_and_oracle
            ex: [doc_name_111111111111117_data_inconsistency_01-01-2000,
                 doc_name_111111111111117_data_inconsistency_01-02-2000, ...]
        """
        var_name = f'{dag_name}_{last_control_var}_{layer}_inconsistency'
        tuple_var = self.execute_statement(f"select key "
                                           f"from public.variable "
                                           f"where key like '{var_name}%'")
        return [variable.get(x[0]) for x in tuple_var]

    def delete_airflow_var(self, dag_name: str, last_control_var: str) -> None:
        tuple_var = self.execute_statement(f"select id, key "
                                           f"from public.variable "
                                           f"where key like '{dag_name + '_' + last_control_var}%'")
        list_values = (x[0] for x in tuple_var)

        for var in list_values:
            postgreshook(self.airflow_conn) \
                .run(sql=f"delete from public.variable where id='{var}'",
                     autocommit=True)
            self.log.warning(f'delete variable: {var}')

    def delete_pattern_var(self, list_pattern_name_var: str) -> None:
        for var in eval(list_pattern_name_var):
            postgreshook(self.airflow_conn) \
                .run(sql=f"delete from public.variable where key like '{var}%'",
                     autocommit=True)
            self.log.warning(f'delete variable pattern: {var}')

    # todo: dag_helper
    def get_dag_name(self, **context: Dict) -> None:
        self.log.info(f'appending current dag in list_dags ...')
        context['list_dags'].append(context['current_dag_name'])
        list_dags = sorted(list(set(context['list_dags'])))

        variable.set(key=context['name_list_dags'], value=list_dags)
        self.log.info(f'list dags = {list_dags}')

    def update_control_var(self, **context) -> None:
        self.log.info(f'actual control_var_name: {context["control_var"]}\n'
                      f'last last_control_var: {context["last_control_var"]}\n'
                      f'list_current_dates:{context["list_current_dates"]}\n'
                      f'list_all_dates:{context["list_all_dates"]}\n'
                      f'list_dags:{context["list_dags"]}\n'
                      f'total_pg: {context["total_pg"]}')

        list_records = redishelper(context['redis_conn_id']) \
            .get_list_redis(context['redis_key'])
        list_id = [x[1] for x in list_records]
        max_value = f"{max(list_id):015d}"  # 000.000.000.000.000

        variable.set(key=f'{context["dag_name"]}_control_var', value=max_value)
        variable.set(key=f'{context["dag_name"]}_last_control_var', value=context["control_var"])

        self.log.info(f'updated airflow variable:\n'
                      f'last_control_var to: {context["control_var"]}\n'
                      f'control_var to: {max_value}')

    def generate_dict_params(self, **context: Dict) -> None:
        """used next_dag"""
        list_total_partitions = [
            int(variable.get(f'{context["current_dag_name"]}_total_pg_{date}'))
            for date in context["list_current_dates"]
        ]

        list_total_rows = [
            int(variable.get(f'{context["current_dag_name"]}_total_rows_{date}'))
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

        variable.set(key=f'{context["dag_name"]}_dict_params_by_date', value=dict_params_by_date)
        self.log.info(f'dict_params_by_date:')
        pprint(dict_params_by_date)
