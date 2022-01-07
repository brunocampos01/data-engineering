from datetime import datetime

import cx_Oracle
import pandas as pd
from airflow.providers.oracle.hooks.oracle import OracleHook
from typing import Optional


class OracleHelper(OracleHook):
    __slots__ = ('oracle_conn_id', '_cursor')  # space savings in memory + faster attribute access

    def __init__(
        self,
        oracle_conn_id,
        *args,
        **kwargs
    ):
        super(OracleHelper, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self._cursor = self.get_cursor()

        # interact with data LOB in Oracle
        self.get_conn().outputtypehandler = self \
            ._cursor \
            .var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=self._cursor.arraysize)

        # tuning: https://cx-oracle.readthedocs.io/en/latest/user_guide/tuning.html
        self._cursor.prefetchrows = 1000
        self._cursor.arraysize = 1000

        # TODO
        # SessionPool

    def __del__(self):
        self._cursor.close()

    def _get_oracle_url(self) -> str:
        return f'jdbc:oracle:thin:@{OracleHook.get_connection(self.oracle_conn_id).host}'

    def _get_oracle_login(self) -> str:
        return OracleHook.get_connection(self.oracle_conn_id).login

    def _get_oracle_password(self) -> str:
        return OracleHook.get_connection(self.oracle_conn_id).password

    def get_rows(self, statement: str) -> str:
        return OracleHook(self.oracle_conn_id).get_records(statement)[0][0]

    def get_rows_with_bind(self, sql: str, bind: dict) -> list:
        return self._cursor.execute(sql, bind).fetchall()

    def get_pandas_df(self,
                      sql: str,
                      parameters: Optional[str] = None,
                      **kwargs
                      ) -> pd.DataFrame:
        return pd.DataFrame(data=self._cursor.execute(sql).fetchall(),
                            columns=[n[0] for n in self._cursor.description])

    def get_pyspark_df_from_query(self,
                                  spark: 'pyspark.sql.session.SparkSession',
                                  sql: str,
                                  oracle_driver: str) -> 'pyspark.sql.dataframe.DataFrame':
        return spark.read.format('jdbc') \
            .option('driver', oracle_driver) \
            .option('url', self._get_oracle_url()) \
            .option('query', sql) \
            .option('user', self._get_oracle_login()) \
            .option('password', self._get_oracle_password()) \
            .option('fetchsize', 50000) \
            .load()

    def get_pyspark_df_from_table(self,
                                  oracle_driver: str,
                                  spark: 'pyspark.sql.session.SparkSession',
                                  table: str,
                                  n_partitions: int,
                                  partition_col: str) -> 'pyspark.sql.dataframe.DataFrame':
        return spark.read.format('jdbc') \
            .option('driver', oracle_driver) \
            .option('url', self._get_oracle_url()) \
            .option('user', self._get_oracle_login()) \
            .option('password', self._get_oracle_password()) \
            .option('dbtable', table) \
            .option('partitionColumn', partition_col) \
            .option('numPartitions', n_partitions) \
            .option('lowerBound', 0) \
            .option('upperBound', 1000) \
            .option('fetchsize', 50000) \
            .load()

    @staticmethod
    def _format_sql_blob(tuple_id_by_date: tuple,
                         table_blob: str,
                         table_blob_col_pk: str,
                         table_blob_col_blob: str) -> str:
        """
        Generate query and handle the ERROR - ORA-00936: missing expression

        Example:
             input: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0',)
             output: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0')
        """
        if len(tuple_id_by_date) == 1:
            return f"""
            SELECT
                ROW_NUMBER() OVER(PARTITION BY 1000 ORDER BY {table_blob_col_pk}) AS COL_PARTITION,
                {table_blob_col_pk},
                {table_blob_col_blob}
            FROM {table_blob}
            WHERE {table_blob_col_pk} IN ('{tuple_id_by_date[0]}')"""
        else:
            return f"""
            SELECT
                 ROW_NUMBER() OVER(PARTITION BY 1000 ORDER BY {table_blob_col_pk}) AS COL_PARTITION,
                 {table_blob_col_pk},
                 {table_blob_col_blob}
            FROM {table_blob}
            WHERE {table_blob_col_pk} IN {tuple_id_by_date}"""

    @staticmethod
    def _format_sql_id_extra_cols(tuple_id_by_date: tuple,
                                  table_ctrl: str,
                                  table_ctrl_col_fk: str,
                                  extra_cols: str) -> str:
        """
        Generate query and handle the ERROR - ORA-00936: missing expression

        Example:
             input: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0',)
             output: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0')
        """
        if len(tuple_id_by_date) == 1:
            return f"""
            SELECT
                {extra_cols}
            FROM {table_ctrl}
            WHERE {table_ctrl_col_fk} IN ('{tuple_id_by_date[0]}')"""
        else:
            return f"""
            SELECT
                {extra_cols}
            FROM {table_ctrl}
            WHERE {table_ctrl_col_fk} IN {tuple_id_by_date}"""

    def generate_sql_get_data(self,
                              total_pg_date: int,
                              list_id_by_date: list,
                              date: str,
                              items_by_query: int,
                              table_blob: Optional[str] = None,
                              table_blob_col_pk: Optional[str] = None,
                              table_blob_col_blob: Optional[str] = None,
                              has_extra_cols: bool = False,
                              table_ctrl: Optional[str] = None,
                              table_ctrl_col_fk: Optional[str] = None,
                              extra_cols: Optional[str] = None) -> str:
        list_sql = []

        for page in range(total_pg_date):
            page = page + 1

            if has_extra_cols:
                sql = self \
                    ._format_sql_id_extra_cols(tuple_id_by_date=tuple(list_id_by_date[:items_by_query]),
                                               table_ctrl=table_ctrl,
                                               table_ctrl_col_fk=table_ctrl_col_fk,
                                               extra_cols=extra_cols)
            else:
                sql = self \
                    ._format_sql_blob(tuple_id_by_date=tuple(list_id_by_date[:items_by_query]),
                                      table_blob=table_blob,
                                      table_blob_col_pk=table_blob_col_pk,
                                      table_blob_col_blob=table_blob_col_blob)

            list_sql.append(sql)
            self.log.info(f'\nDate: {date}'
                          f'\nPage: {page}'
                          f'\nSize: {len(list_id_by_date[0:items_by_query])}'
                          f'\nSQL: {sql}')

            del list_id_by_date[0:items_by_query]

        return '\nUNION ALL\n'.join(list_sql)

    @staticmethod
    def generate_sql_count(process_date: str,
                           table_ctrl: str,
                           table_ctrl_col_fk: str,
                           dt_ref: str,
                           agg_by: str,
                           table_ctrl_col_dt_created: str) -> str:
        """
        Example:
            process_date: 01-01-2020
            agg_by: month
        """
        year = process_date[6:]
        month = process_date[3:5]
        day = process_date[:2]
        dt = datetime.strptime(f'{day}-{month}-{year}', '%d-%m-%Y').date()

        if agg_by == 'month':
            return f"""
            SELECT COUNT({table_ctrl_col_fk})
            FROM {table_ctrl}
            WHERE
                {table_ctrl_col_fk} IS NOT NULL
                AND TO_DATE(to_char({dt_ref}, 'MM-YYYY') , 'MM-YYYY')
                = TO_DATE('{dt.month:02d}-{dt.year}', 'MM-YYYY')
                AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')"""

        return f"""
        SELECT COUNT({table_ctrl_col_fk})
        FROM {table_ctrl}
        WHERE
            {table_ctrl_col_fk} IS NOT NULL
            AND TO_DATE(to_char({dt_ref}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                = TO_DATE('{dt.day:02d}-{dt.month:02d}-{dt.year}', 'DD-MM-YYYY')
            AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')"""

    @staticmethod
    def generate_sql_recovery(table_ctrl: str,
                              table_ctrl_col_fk: str,
                              table_ctrl_col_control_var: str,
                              table_ctrl_col_dt_ref: str,
                              table_ctrl_col_dt_created: str,
                              list_inconsistency: list) -> str:
        """
        Query generated from inconsistent date list.
        This function serves to facilitate data recovery
        Copy the return from this query and insert it into the query fields of the import.py file
        """
        rows = ''
        for date in list_inconsistency:
            month_year = date[3:]
            row = f"\tOR TO_DATE(to_char({table_ctrl_col_dt_ref}, 'MM-YYYY') , 'MM-YYYY') " \
                  f"= TO_DATE('{month_year}', 'MM-YYYY')\n"
            rows += row

        return f'''
        SELECT
            {table_ctrl_col_fk} id,
            {table_ctrl_col_control_var} control_var,
            to_char({table_ctrl_col_dt_ref}, 'DD-MM-YYYY') dt_ref
        FROM {table_ctrl}
        WHERE
            {table_ctrl_col_fk} IS NOT NULL
            AND {table_ctrl_col_control_var} > 000000000000000
            AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')
            AND {rows[3:]}'''

    def get_all_dates(self, table_ctrl: str, table_ctrl_col_dt_ref: str) -> 'generator':
        list_tuple_dates = self.get_records(f'''
        SELECT DISTINCT to_char({table_ctrl_col_dt_ref}, 'DD-MM-YYYY') dt_ref
        FROM {table_ctrl}''')

        return (item[0] for item in list_tuple_dates)
from datetime import datetime

import cx_Oracle
import pandas as pd
from airflow.providers.oracle.hooks.oracle import OracleHook
from typing import Optional


class OracleHelper(OracleHook):
    __slots__ = ('oracle_conn_id', '_cursor')  # space savings in memory + faster attribute access

    def __init__(
        self,
        oracle_conn_id,
        *args,
        **kwargs
    ):
        super(OracleHelper, self).__init__(*args, **kwargs)
        self.oracle_conn_id = oracle_conn_id
        self._cursor = self.get_cursor()

        # interact with data LOB in Oracle
        self.get_conn().outputtypehandler = self \
            ._cursor \
            .var(cx_Oracle.DB_TYPE_LONG_RAW, arraysize=self._cursor.arraysize)

        # tuning: https://cx-oracle.readthedocs.io/en/latest/user_guide/tuning.html
        self._cursor.prefetchrows = 1000
        self._cursor.arraysize = 1000

        # TODO
        # SessionPool

    def __del__(self):
        self._cursor.close()

    def _get_oracle_url(self) -> str:
        return f'jdbc:oracle:thin:@{OracleHook.get_connection(self.oracle_conn_id).host}'

    def _get_oracle_login(self) -> str:
        return OracleHook.get_connection(self.oracle_conn_id).login

    def _get_oracle_password(self) -> str:
        return OracleHook.get_connection(self.oracle_conn_id).password

    def get_rows(self, statement: str) -> str:
        return OracleHook(self.oracle_conn_id).get_records(statement)[0][0]

    def get_rows_with_bind(self, sql: str, bind: dict) -> list:
        return self._cursor.execute(sql, bind).fetchall()

    def get_pandas_df(self,
                      sql: str,
                      parameters: Optional[str] = None,
                      **kwargs
                      ) -> pd.DataFrame:
        return pd.DataFrame(data=self._cursor.execute(sql).fetchall(),
                            columns=[n[0] for n in self._cursor.description])

    def get_pyspark_df_from_query(self,
                                  spark: 'pyspark.sql.session.SparkSession',
                                  sql: str,
                                  oracle_driver: str) -> 'pyspark.sql.dataframe.DataFrame':
        return spark.read.format('jdbc') \
            .option('driver', oracle_driver) \
            .option('url', self._get_oracle_url()) \
            .option('query', sql) \
            .option('user', self._get_oracle_login()) \
            .option('password', self._get_oracle_password()) \
            .option('fetchsize', 50000) \
            .load()

    def get_pyspark_df_from_table(self,
                                  oracle_driver: str,
                                  spark: 'pyspark.sql.session.SparkSession',
                                  table: str,
                                  n_partitions: int,
                                  partition_col: str) -> 'pyspark.sql.dataframe.DataFrame':
        return spark.read.format('jdbc') \
            .option('driver', oracle_driver) \
            .option('url', self._get_oracle_url()) \
            .option('user', self._get_oracle_login()) \
            .option('password', self._get_oracle_password()) \
            .option('dbtable', table) \
            .option('partitionColumn', partition_col) \
            .option('numPartitions', n_partitions) \
            .option('lowerBound', 0) \
            .option('upperBound', 1000) \
            .option('fetchsize', 50000) \
            .load()

    @staticmethod
    def _format_sql_blob(tuple_id_by_date: tuple,
                         table_blob: str,
                         table_blob_col_pk: str,
                         table_blob_col_blob: str) -> str:
        """
        Generate query and handle the ERROR - ORA-00936: missing expression

        Example:
             input: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0',)
             output: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0')
        """
        if len(tuple_id_by_date) == 1:
            return f"""
            SELECT
                ROW_NUMBER() OVER(PARTITION BY 1000 ORDER BY {table_blob_col_pk}) AS COL_PARTITION,
                {table_blob_col_pk},
                {table_blob_col_blob}
            FROM {table_blob}
            WHERE {table_blob_col_pk} IN ('{tuple_id_by_date[0]}')"""
        else:
            return f"""
            SELECT
                 ROW_NUMBER() OVER(PARTITION BY 1000 ORDER BY {table_blob_col_pk}) AS COL_PARTITION,
                 {table_blob_col_pk},
                 {table_blob_col_blob}
            FROM {table_blob}
            WHERE {table_blob_col_pk} IN {tuple_id_by_date}"""

    @staticmethod
    def _format_sql_id_extra_cols(tuple_id_by_date: tuple,
                                  table_ctrl: str,
                                  table_ctrl_col_fk: str,
                                  extra_cols: str) -> str:
        """
        Generate query and handle the ERROR - ORA-00936: missing expression

        Example:
             input: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0',)
             output: SELECT ... IN ('14e02dc3-34ff-415e-baac-81960b1c25e0')
        """
        if len(tuple_id_by_date) == 1:
            return f"""
            SELECT
                {extra_cols}
            FROM {table_ctrl}
            WHERE {table_ctrl_col_fk} IN ('{tuple_id_by_date[0]}')"""
        else:
            return f"""
            SELECT
                {extra_cols}
            FROM {table_ctrl}
            WHERE {table_ctrl_col_fk} IN {tuple_id_by_date}"""

    def generate_sql_get_data(self,
                              total_pg_date: int,
                              list_id_by_date: list,
                              date: str,
                              items_by_query: int,
                              table_blob: Optional[str] = None,
                              table_blob_col_pk: Optional[str] = None,
                              table_blob_col_blob: Optional[str] = None,
                              has_extra_cols: bool = False,
                              table_ctrl: Optional[str] = None,
                              table_ctrl_col_fk: Optional[str] = None,
                              extra_cols: Optional[str] = None) -> str:
        list_sql = []

        for page in range(total_pg_date):
            page = page + 1

            if has_extra_cols:
                sql = self \
                    ._format_sql_id_extra_cols(tuple_id_by_date=tuple(list_id_by_date[:items_by_query]),
                                               table_ctrl=table_ctrl,
                                               table_ctrl_col_fk=table_ctrl_col_fk,
                                               extra_cols=extra_cols)
            else:
                sql = self \
                    ._format_sql_blob(tuple_id_by_date=tuple(list_id_by_date[:items_by_query]),
                                      table_blob=table_blob,
                                      table_blob_col_pk=table_blob_col_pk,
                                      table_blob_col_blob=table_blob_col_blob)

            list_sql.append(sql)
            self.log.info(f'\nDate: {date}'
                          f'\nPage: {page}'
                          f'\nSize: {len(list_id_by_date[0:items_by_query])}'
                          f'\nSQL: {sql}')

            del list_id_by_date[0:items_by_query]

        return '\nUNION ALL\n'.join(list_sql)

    @staticmethod
    def generate_sql_count(process_date: str,
                           table_ctrl: str,
                           table_ctrl_col_fk: str,
                           dt_ref: str,
                           agg_by: str,
                           table_ctrl_col_dt_created: str) -> str:
        """
        Example:
            process_date: 01-01-2020
            agg_by: month
        """
        year = process_date[6:]
        month = process_date[3:5]
        day = process_date[:2]
        dt = datetime.strptime(f'{day}-{month}-{year}', '%d-%m-%Y').date()

        if agg_by == 'month':
            return f"""
            SELECT COUNT({table_ctrl_col_fk})
            FROM {table_ctrl}
            WHERE
                {table_ctrl_col_fk} IS NOT NULL
                AND TO_DATE(to_char({dt_ref}, 'MM-YYYY') , 'MM-YYYY')
                = TO_DATE('{dt.month:02d}-{dt.year}', 'MM-YYYY')
                AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')"""

        return f"""
        SELECT COUNT({table_ctrl_col_fk})
        FROM {table_ctrl}
        WHERE
            {table_ctrl_col_fk} IS NOT NULL
            AND TO_DATE(to_char({dt_ref}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                = TO_DATE('{dt.day:02d}-{dt.month:02d}-{dt.year}', 'DD-MM-YYYY')
            AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')"""

    @staticmethod
    def generate_sql_recovery(table_ctrl: str,
                              table_ctrl_col_fk: str,
                              table_ctrl_col_control_var: str,
                              table_ctrl_col_dt_ref: str,
                              table_ctrl_col_dt_created: str,
                              list_inconsistency: list) -> str:
        """
        Query generated from inconsistent date list.
        This function serves to facilitate data recovery
        Copy the return from this query and insert it into the query fields of the import.py file
        """
        rows = ''
        for date in list_inconsistency:
            month_year = date[3:]
            row = f"\tOR TO_DATE(to_char({table_ctrl_col_dt_ref}, 'MM-YYYY') , 'MM-YYYY') " \
                  f"= TO_DATE('{month_year}', 'MM-YYYY')\n"
            rows += row

        return f'''
        SELECT
            {table_ctrl_col_fk} id,
            {table_ctrl_col_control_var} control_var,
            to_char({table_ctrl_col_dt_ref}, 'DD-MM-YYYY') dt_ref
        FROM {table_ctrl}
        WHERE
            {table_ctrl_col_fk} IS NOT NULL
            AND {table_ctrl_col_control_var} > 000000000000000
            AND TO_DATE(to_char({table_ctrl_col_dt_created}, 'DD-MM-YYYY'), 'DD-MM-YYYY')
                < TO_DATE(to_char(trunc(sysdate), 'DD-MM-YYYY'), 'DD-MM-YYYY')
            AND {rows[3:]}'''

    def get_all_dates(self, table_ctrl: str, table_ctrl_col_dt_ref: str) -> 'generator':
        list_tuple_dates = self.get_records(f'''
        SELECT DISTINCT to_char({table_ctrl_col_dt_ref}, 'DD-MM-YYYY') dt_ref
        FROM {table_ctrl}''')

        return (item[0] for item in list_tuple_dates)
