from airflow.utils.log.logging_mixin import LoggingMixin

from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.db.hive_helper import HiveHelper
from hooks.db.impala_helper import ImpalaHelper
from hooks.hdfs.hdfs_helper import HdfsHelper


class CreatePartitions(LoggingMixin):

    def __init__(self, *args, **kwargs):
        super(CreatePartitions).__init__(*args, **kwargs)

    def execute(self, **context):
        hive = HiveHelper(hive_conn=context['hive_conn_id'])
        impala = ImpalaHelper(hive_conn=context['impala_conn_id'])
        list_all_dates = AirflowMetaStoreHelper().set_granularity(
            list_all_dates=context['list_all_dates'],
            agg_by=context['agg_by']
        )

        for date in list_all_dates:
            hdfs_path = HdfsHelper().generate_hdfs_path(env=context["env"],
                                                        layer=context['layer'],
                                                        dag_id=context['dag_name'],
                                                        date=date)

            self.log.info("HIVE: Creating partition")
            hive.add_partition(date=date,
                               db=context['dag_name'],
                               table=context['layer'],
                               hdfs_path=hdfs_path)

            self.log.info("IMPALA: Creating partition")
            impala.add_partition(date=date,
                                 db=context['dag_name'],
                                 table=context['layer'],
                                 hdfs_path=hdfs_path)

            self.log.info("IMPALA: Refreshing table")
            impala.execute_refresh_table(db=context['dag_name'],
                                         table=context['layer'])
