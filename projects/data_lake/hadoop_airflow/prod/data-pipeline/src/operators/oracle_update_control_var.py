from airflow.models import BaseOperator
from airflow.models import Variable

from hooks.db.oracle_helper import OracleHelper


class OracleUpdateControlVar(BaseOperator):
    def __init__(
        self,
        dag_name: str,
        current_dag_name: str,
        dict_bind: dict,
        sql: str,
        col_control_var: str,
        control_var: str,
        last_control_var: str,
        oracle_conn_id='oracle_default',
        *args,
        **kwargs
    ):
        super(OracleUpdateControlVar, self).__init__(*args, **kwargs)
        self.current_dag_name = current_dag_name
        self.dict_bind = dict_bind
        self.sql = sql
        self.oracle_conn_id = oracle_conn_id
        self.dag_name = dag_name
        self.col_control_var = col_control_var
        self.control_var = control_var
        self.last_control_var = last_control_var

    def execute(self, context):
        oracle = OracleHelper(self.oracle_conn_id)
        sql = f"SELECT MAX({self.col_control_var}) FROM ({self.sql})"
        self.log.info(f"Executing SQL:\n{sql}")
        self.log.info(f"Parameters:\n{self.dict_bind}")

        max_value = f"{oracle.get_rows_with_bind(sql=sql, bind=self.dict_bind)[0][0]:015d}"  # 000.000.000.000.000

        Variable.set(key=f'{self.dag_name}_control_var', value=max_value)
        Variable.set(key=f'{self.dag_name}_last_control_var', value=self.control_var)

        self.log.info(f'Updated Airflow variable:\n'
                      f'current_dag_name: {self.current_dag_name}\n'
                      f'last_control_var to: {self.control_var}\n'
                      f'control_var to: {max_value}')
