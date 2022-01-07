from airflow.models import BaseOperator
from airflow.models import Variable

from hooks.db.oracle_helper import OracleHelper


class OracleGetResults(BaseOperator):
    """
    It was necessary to split an operator from branchOperator
    in an operator (this) and in another function (check_if_contains_data_in_oracle)
    This avoids loading in all tasks of JDBC connections and therefore generates less load.
    """

    def __init__(
        self,
        current_dag_name,
        dict_bind,
        sql_count_id,
        oracle_conn_id='oracle_default',
        *args,
        **kwargs
    ):
        super(OracleGetResults, self).__init__(*args, **kwargs)
        self.current_dag_name = current_dag_name
        self.sql_count_id = sql_count_id
        self.dict_bind = dict_bind
        self.oracle_conn_id = oracle_conn_id

    def execute(self, context):
        oracle = OracleHelper(self.oracle_conn_id)
        self.log.info(f"Executing SQL:{self.sql_count_id}\nParameters: {self.dict_bind}")
        count_id = oracle.get_rows_with_bind(sql=self.sql_count_id,
                                             bind=self.dict_bind)[0][0]

        Variable.set(key=f'{self.current_dag_name}_total_row_id',
                     value=count_id)
        self.log.info(f"{count_id} rows are not in HDFS.")
