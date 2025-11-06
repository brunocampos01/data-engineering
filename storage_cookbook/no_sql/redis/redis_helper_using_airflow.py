import logging

import pandas as pd
from airflow.models import Variable
from airflow.providers.redis.hooks.redis import RedisHook


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
