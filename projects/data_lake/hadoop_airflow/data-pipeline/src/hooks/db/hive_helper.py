from datetime import datetime

from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from pyhive import hive

from hooks.db.airflow_metastore_helper import AirflowMetaStoreHelper
from hooks.db.oracle_helper import OracleHelper
from hooks.hdfs.hdfs_helper import HdfsHelper


class HiveHelper(HiveServer2Hook):
    """
    Com esta extensão do HiveHelper é possível manter os serviços q foram criados pelo mammoth no kerberos
    sem precisar alterar parâmetros que já foram definidos pela Oracle no BDA.
    """
    __slots__ = ('hive_conn', 'kerberos_service_name', 'host', 'port', 'user', '_conn', '_cursor')

    def __init__(
        self,
        hive_conn,
        kerberos_service_name: str = 'hive',
        *args,
        **kwargs
    ):
        super(HiveHelper, self).__init__(*args, **kwargs)
        self.hive_conn = hive_conn
        self.kerberos_service_name = kerberos_service_name
        self.host = HiveServer2Hook.get_connection(self.hive_conn).host
        self.port = HiveServer2Hook.get_connection(self.hive_conn).port
        self.user = HiveServer2Hook.get_connection(self.hive_conn).login
        self._conn = self._connect(kerberos_service_name)
        self._cursor = self._conn.cursor()

    def _connect(self, kerberos_service_name):
        return hive.Connection(host=self.host,
                               port=self.port,
                               username=self.user,
                               auth='KERBEROS',
                               kerberos_service_name=kerberos_service_name)

    def get_cursor(self):
        return self._cursor

    def with_database(self, db: str) -> None:
        try:
            self._cursor.execute(f"USE {db}")
        except hive.DatabaseError as err:
            self.log.error(f"Failed execute query: \n{err}\ndb: {db}")
            raise

    # TODO
    def create_roles(self):
        raise NotImplementedError

    @staticmethod
    def _map_type_avro_to_hive(avro_type: str) -> str:
        type_map = {
            'null': 'void',
            'string': 'string',
            'int': 'int',
            'long': 'bigint',
            'bytes': 'binary',
            'list': 'array'
        }
        return type_map.get(avro_type)

    def format_cols(self, list_cols_avro: dict) -> str:
        columns = ''

        for col in list_cols_avro:
            if col["type"] == 'long':
                col["type"] = self._map_type_avro_to_hive(col["type"])

            # get first element in list
            if isinstance(col["type"], list):
                col["type"] = self._map_type_avro_to_hive(col["type"][0])

            hive_col = f"""
                {col["name"]}\t\t{col["type"]}\t\tCOMMENT '{col["comment"]}',\t\t"""
            columns += hive_col

        return columns

    def create_database(self, db: str, db_comment: str) -> None:
        try:
            self._cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db} COMMENT '{db_comment}'")
        except hive.Error as err:
            self.log.error(f"Failed execute query: \n{err},\ndb: {db}")
            raise

    def create_external_table_avro(self,
                                   db: str,
                                   table: str,
                                   cols: str,
                                   hdfs_path: str,
                                   hdfs_path_schema_avro: str,
                                   comment_table: str,
                                   comment_col_dt: str) -> None:
        try:
            self._cursor.execute(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
            {cols}
                PRIMARY KEY                 (id) DISABLE NOVALIDATE)
            PARTITIONED BY (
                YEAR      SMALLINT          COMMENT 'oracle: {comment_col_dt}',
                MONTH     TINYINT           COMMENT 'oracle: {comment_col_dt}',
                DAY       TINYINT           COMMENT 'oracle: {comment_col_dt}')
            ROW FORMAT SERDE
                'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS
                AVRO
            LOCATION
                'hdfs://{hdfs_path}/{db}'
            TBLPROPERTIES (
                'avro.schema.url' = 'hdfs://{hdfs_path_schema_avro}/{db}.avsc',
                'basic.stats' = 'true',
                'impala.enable.stats.extrapolation' = 'true',
                'comment'='{comment_table}')""")
        except hive.DatabaseError as err:
            self.log.error(f"Failed execute query: \n{err}\ndb: {db}")
            raise

    def create_external_table_parquet(self,
                                      db: str,
                                      table: str,
                                      col_pk: str,
                                      cols: str,
                                      hdfs_path: str,
                                      hdfs_path_schema_avro: str,
                                      comment_table: str) -> None:
        try:
            self._cursor.execute(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{table}(
            {cols}
                PRIMARY KEY                 ({col_pk}) DISABLE NOVALIDATE)
            ROW FORMAT SERDE
                'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            STORED AS
                PARQUET
            LOCATION
                'hdfs://{hdfs_path}/{db}'
            TBLPROPERTIES (
                'avro.schema.url' = 'hdfs://{hdfs_path_schema_avro}/{db}.avsc',
                'basic.stats' = 'true',
                'impala.enable.stats.extrapolation' = 'true',
                'parquet.compress' = 'SNAPPY',
                'comment'='{comment_table}')""")
        except hive.DatabaseError as err:
            self.log.error(f"Failed execute query: \n{err}\ndb: {db}\ntable: {table}")
            raise

    def execute_statement(self, statement: str) -> None:
        try:
            self._cursor.execute(statement)
        except hive.DatabaseError as err:
            self.log.error(f"Failed execute query: {err}, query: {statement}")
            raise

    def get_rows(self, hql: str) -> str:
        try:
            self._cursor.execute(hql)
            return self._cursor.fetchall()
        except Exception as err:
            self.log.error(f"Failed execute query: {err}, query: {hql}")
            raise

    def get_partitions(self, dag_name: str, origin_layer: str) -> list:
        return [x for x in self.get_rows(f'SELECT DISTINCT month, year '
                                         f'FROM {dag_name}.{origin_layer}')]

    def generate_stats_by_partition(self,
                                    date: str,
                                    db: str,
                                    table: str) -> None:
        d = datetime.strptime(str(date), '%d-%m-%Y').date()
        self._cursor.execute(f"""
            ANALYZE TABLE {db}.{table}
                PARTITION(year='{d.year}', month='{d.month:02d}', day='{d.day:02d}')
            COMPUTE STATISTICS""")

    def add_partition(self,
                      date: str,
                      db: str,
                      table: str,
                      hdfs_path: str) -> None:
        d = datetime.strptime(str(date), '%d-%m-%Y').date()
        self._cursor.execute(f"""
            ALTER TABLE {db}.{table}
                ADD IF NOT EXISTS PARTITION
                    (year={d.year}, month={d.month:02d}, day={d.day:02d})
                LOCATION
                    'hdfs://{hdfs_path}'""")

    def generate_all_partitions(self,
                                oracle_conn: str,
                                table_ctrl: str,
                                table_ctrl_col_dt_ref: str,
                                agg_by: str,
                                env: str,
                                layer: str,
                                data: str) -> None:
        """
        Generates all partitions in hive and impala.
        Useful when it is necessary to recreate/change dbs, tables or data directories in HDFS
        """
        list_all_dates = OracleHelper(oracle_conn).get_all_dates(table_ctrl=table_ctrl,
                                                                 table_ctrl_col_dt_ref=table_ctrl_col_dt_ref)
        list_all_dates = [dt for dt in list_all_dates if dt is not None]
        list_dates = AirflowMetaStoreHelper().set_granularity(list_all_dates=list_all_dates,
                                                              agg_by=agg_by)

        for date in list_dates:
            hdfs_path = HdfsHelper().generate_hdfs_path(env=env,
                                                        layer=layer,
                                                        dag_id=data,
                                                        date=date)

            self.log.info(f"Creating partition:")
            self.add_partition(date=date,
                               db=data,
                               table=layer,
                               hdfs_path=hdfs_path)

    def hive_prepare_env(self,
                         env: str,
                         layer: str,
                         data_name: str,
                         cfg: dict,
                         cfg_hdfs: dict,
                         template: str) -> None:
        # TODO: create roles

        if layer == 'raw':
            self.log.info(f'\n{template}\nCreating database: {data_name}\n{template}')
            self.create_database(db=data_name,
                                 db_comment=cfg["hdfs_data_schema"]["db_comment"])

        self.log.info(f'\n{template}\nCreating external table: {data_name}\n{template}')
        cols = self.format_cols(cfg["hdfs_data_schema"][layer]["cols"])

        if cfg['doc_type'] in ('xml', 'pipe'):
            self.create_external_table_avro(
                db=data_name,
                table=layer,
                hdfs_path=f'/data/{env}/{layer}',
                hdfs_path_schema_avro=f'{cfg_hdfs["path_avro_schemas"]}/{layer}',
                comment_table=f'{cfg["hdfs_data_schema"][layer]["table_comment"]}',
                cols=cols,
                comment_col_dt=cfg["source_ctrl"]["cols"]["dt_ref"].lower()
            )

            self.generate_all_partitions(
                env='prod',
                data=data_name,
                layer=layer,
                agg_by=cfg['agg_by'],
                oracle_conn=cfg['source_ctrl']['oracle_conn'],
                table_ctrl=cfg['source_ctrl']['table_name'],
                table_ctrl_col_dt_ref=cfg['source_ctrl']['cols']['dt_ref']
            )

        else:
            self.create_external_table_parquet(
                db=data_name,
                table=layer,
                hdfs_path=f'/data/{env}/{layer}',
                hdfs_path_schema_avro=f'{cfg_hdfs["path_avro_schemas"]}/{layer}',
                comment_table=f'{cfg["hdfs_data_schema"][layer]["table_comment"]}',
                cols=cols,
                col_pk=cfg["source"]["cols"]["pk"]
            )
