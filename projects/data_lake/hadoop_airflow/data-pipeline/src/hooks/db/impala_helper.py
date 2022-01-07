from datetime import datetime

from hooks.db.hive_helper import HiveHelper


class ImpalaHelper(HiveHelper):
    __slots__ = ('hive_conn', 'kerberos_service_name', 'kerberos_service_name')

    def __init__(
        self,
        hive_conn: str = 'impala',
        kerberos_service_name: str = 'impala',
        *args,
        **kwargs
    ):
        super(ImpalaHelper, self).__init__(hive_conn, kerberos_service_name, *args, **kwargs)
        self.hive_conn = hive_conn
        self.kerberos_service_name = kerberos_service_name
        self._conn = HiveHelper(hive_conn=self.hive_conn,
                                kerberos_service_name=self.kerberos_service_name)
        self._cursor = self._conn.get_cursor()

    def generate_stats_by_partition(self, date: str, db: str, table: str) -> None:
        d = datetime.strptime(str(date), '%d-%m-%Y').date()
        self._cursor.execute(f"""
            COMPUTE INCREMENTAL STATS {db}.{table}
            PARTITION(year={d.year}, month={d.month}, day={d.day})""")

    def execute_refresh_table(self, db: str, table: str) -> None:
        return self._cursor.execute(f"REFRESH {db}.{table}")

    def execute_invalidate_metadata(self) -> None:
        return self._cursor.execute(f"INVALIDATE METADATA")

    def get_count_by_partition(self, db: str, table: str, process_date: str) -> str:
        year = process_date[6:]
        month = process_date[3:5]
        day = process_date[:2]
        dt = datetime.strptime(f'{day}-{month}-{year}', '%d-%m-%Y').date()

        return self.get_rows(f"""
                       SELECT COUNT(DISTINCT id)
                       FROM {db}.{table}
                       WHERE year={dt.year} and month={dt.month:02d} and day={dt.day:02d}""")[0][0]
