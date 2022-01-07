import logging
import math

import pandas as pd
from airflow.models import Variable
from airflow.providers.redis.hooks.redis import RedisHook
from hooks.db.oracle_helper import OracleHelper


class RedisHelper(RedisHook):
    __slots__ = 'redis_conn_id'  # space savings in memory + faster attribute access

    def __init__(self, redis_conn_id, *args, **kwargs):
        super(RedisHook, self).__init__(*args, **kwargs)
        self.redis_conn_id = redis_conn_id

    def get_list_redis(self, redis_key: str) -> list:
        """Get all objects from a Redis list of type key

        Args:
        ----------
        redis_conn_id : str
            connection name set in: airflow > admin > connections
        redis_key : str
            key name in redis.

        Returns:
        ----------
            A list containing string objects without repetition.
            Example: ['01-01-2021', '02-01-2021', '03-01-2021']
        """
        set_records = set(RedisHook(self.redis_conn_id)
                          .get_conn() \
                          .lrange(redis_key, 0, -1))

        return [eval(x.decode("utf-8")) for x in set_records]

    def fill_data_gaps(self, **context) -> None:
        """Fill data without dt_ref
        Example:
            input:  ('ef57132e-0d25-4e75-bee6-158c01c5b360', 50000, None)
            output: ('e85e10b5-43af-4721-b832-9b9b4bb366fe', 10708, '01-01-1900')
        """
        list_records = self.get_list_redis(context['redis_key'])
        pipe = RedisHook(self.redis_conn_id).get_conn().pipeline()
        df = pd.DataFrame(data=list_records)
        col_date_ref = list(df.columns)[-1]
        df[col_date_ref].replace(to_replace=[None], value='01-01-1900', inplace=True)
        records = [tuple(x) for x in df.to_numpy()]

        [pipe.lpush(context['current_dag_name'], str(row)) for row in records]
        pipe.execute()
        self.log.info(f"\nSample rows:\n{df.head(5)}")

    def get_date(self, **context) -> None:
        """Get current dates and append in list_all_dates"""
        list_records = self.get_list_redis(context['redis_key'])
        list_current_dt = list(set(x[-1] for x in list_records))

        # current execution
        Variable.set(key=f'{context["current_dag_name"]}_current_dates',
                     value=sorted(list_current_dt))
        self.log.info(f'List current dates = {list_current_dt}')

        # global execution
        list_all_dt = eval(Variable.get(f'{context["dag_name"]}_list_all_dates',
                                        default_var='[]'))
        list_all_dt_update = list(set(list_all_dt + list_current_dt))
        Variable.set(key=f'{context["dag_name"]}_list_all_dates',
                     value=sorted([date for date in list_all_dt_update]))
        self.log.info(f'List all dates = {list_all_dt_update}')

    def split_id_by_date(self, **context) -> None:
        """Create redis key by date

        example (redis keys):
            input: file_name_000000000000000
            output: file_name_000000000000000_01-01-1900, file_name_000000000000000_24-12-2020
        """
        logging.info(f'Spliting IDs by date ...')
        pipe = RedisHook(self.redis_conn_id).get_conn().pipeline()
        list_records = self.get_list_redis(context['redis_key'])

        for date in context['list_current_dates']:
            list_item = [item for item in list_records if date in item[-1]]

            name_redis_key = context['current_dag_name'] + '_' + date
            [pipe.lpush(name_redis_key, str(row)) for row in list_item]
            pipe.execute()
            self.log.info(f'{date} - Storaged at redis {name_redis_key} = {len(list_item)}')

    def generate_pagination(self, **context) -> None:
        """:return
            file_name_000000000000000_total_pg_01-01-1900 = 50
        """
        self.log.info(f'Getting pagination total ...')
        ns_keys = context['current_dag_name'] + '_' + '*'
        name_var_total_pg = context['current_dag_name'] + '_' + 'total_pg'
        list_dates_redis_bin = RedisHook(self.redis_conn_id).get_conn().scan_iter(ns_keys)
        list_keys_redis = sorted((x.decode("utf-8") for x in list_dates_redis_bin))
        total_pg = 0

        for date, key in zip(context['list_current_dates'], list_keys_redis):
            list_records = self.get_list_redis(redis_key=key)

            # pg by date
            total_pg_by_date = math.ceil(
                len(list_records) / int(context['items_by_query']))  # e.g: 3.5 to 4
            name_key_pg = name_var_total_pg + '_' + date
            Variable.set(key=name_key_pg, value=total_pg_by_date)
            self.log.info(f'{name_key_pg} = {total_pg_by_date}')

            total_pg += total_pg_by_date

        # total pg
        Variable.set(key=name_var_total_pg, value=total_pg)

    # TODO: operator
    def generate_sql_by_date(self, **context) -> None:
        self.log.info(f'Generating SQL ...')
        items_by_query = int(context['items_by_query'])

        for date in context['list_current_dates']:
            list_records_unsorted = self.get_list_redis(context['redis_key'] + '_' + date)
            list_records = sorted(list_records_unsorted, key=lambda tup: tup[1])
            total_pg_date = eval(
                Variable.get(context['current_dag_name'] + '_' + 'total_pg' + '_' + date))

            self.log.info(f'Getting {context["redis_key"] + "_" + date} in Redis')

            sql_blob = OracleHelper(context['oracle_conn']) \
                .generate_sql_get_data(total_pg_date=total_pg_date,
                                       list_id_by_date=[x[-3] for x in list_records],
                                       items_by_query=items_by_query,
                                       date=date,
                                       table_blob=context['table_blob'],
                                       table_blob_col_pk=context['table_blob_col_pk'],
                                       table_blob_col_blob=context['table_blob_col_blob'])
            Variable.set(key=context['current_dag_name'] + '_' + 'sql_blob' + '_' + date,
                         value=sql_blob)

            if len(context['extra_cols']) > 0:
                self.log.info(f'Generating extra cols ...')
                sql_id_extra_cols = OracleHelper(context['oracle_conn']) \
                    .generate_sql_get_data(total_pg_date=total_pg_date,
                                           list_id_by_date=[x[-3] for x in list_records],
                                           date=date,
                                           items_by_query=items_by_query,
                                           table_ctrl=context['table_ctrl'],
                                           table_ctrl_col_fk=context['table_ctrl_col_fk'],
                                           has_extra_cols=True,
                                           extra_cols=context['extra_cols'])
                Variable.set(key=context['current_dag_name'] + '_' + 'sql_extra_cols' + '_' + date,
                             value=sql_id_extra_cols)

    def clear_redis(self, dag_name: str) -> None:
        redis_conn = RedisHook(self.redis_conn_id).get_conn()
        ns_keys = dag_name + '*'

        for key in redis_conn.scan_iter(ns_keys):
            redis_conn.delete(key)
            self.log.warning(f'Delete key: {key} in Redis')

    def get_pyspark_df(self,
                       spark: 'pyspark.sql.session.SparkSession',
                       schema: 'StructType',
                       redis_key: str
                       ) -> 'pyspark.sql.dataframe.DataFrame':
        return spark \
            .createDataFrame(self.get_list_redis(redis_key), schema=schema)
