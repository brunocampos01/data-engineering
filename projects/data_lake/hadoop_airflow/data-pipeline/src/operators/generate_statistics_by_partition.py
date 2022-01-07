from airflow.utils.log.logging_mixin import LoggingMixin

from hooks.db.hive_helper import HiveHelper
from hooks.db.impala_helper import ImpalaHelper


class GenerateStatistics(LoggingMixin):

    def __init__(self, *args, **kwargs):
        super(GenerateStatistics).__init__(*args, **kwargs)

    def execute(self, **context):
        hive = HiveHelper(hive_conn=context['hive_conn_id'])
        impala = ImpalaHelper(hive_conn=context['impala_conn_id'])

        self.log.info("HIVE: Generating stats")
        hive.generate_stats_by_partition(date=context['date'],
                                         db=context['dag_name'],
                                         table=context['layer'])

        self.log.info("IMPALA: Generating stats")
        impala.generate_stats_by_partition(date=context['date'],
                                           db=context['dag_name'],
                                           table=context['layer'])
        impala.execute_refresh_table(db=context['dag_name'],
                                     table=context['layer'])
