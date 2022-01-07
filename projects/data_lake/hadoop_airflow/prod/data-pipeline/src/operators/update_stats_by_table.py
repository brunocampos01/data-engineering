from airflow.models import BaseOperator

from hooks.db.impala_helper import ImpalaHelper


class UpdateStasByTable(BaseOperator):

    def __init__(
        self,
        db_name,
        table_name,
        impala_conn_id='impala',
        *args,
        **kwargs
    ):
        super(UpdateStasByTable, self).__init__(*args, **kwargs)
        self.db_name = db_name
        self.table_name = table_name
        self.impala_conn_id = impala_conn_id

    def execute(self, **context):
        impala = ImpalaHelper(hive_conn=self.impala_conn_id)

        self.log.info("IMPALA: Updating statistics")
        impala.execute_statement(f"INVALIDATE METADATA {self.db_name}.{self.table_name}")
        impala.execute_refresh_table(db=self.db_name,
                                     table=self.table_name)
        impala.execute_statement(f"COMPUTE INCREMENTAL STATS {self.db_name}.{self.table_name}")
