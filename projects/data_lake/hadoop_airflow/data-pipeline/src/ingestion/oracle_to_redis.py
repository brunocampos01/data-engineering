from airflow.models import BaseOperator
from airflow.providers.redis.hooks.redis import RedisHook

from hooks.db.oracle_helper import OracleHelper


class OracleToRedisTransfer(BaseOperator):

    def __init__(
        self,
        sql,
        dict_bind,
        name_redis_key,
        oracle_conn_id='oracle_default',
        redis_conn_id='redis_default',
        *args,
        **kwargs
    ):
        super(OracleToRedisTransfer, self).__init__(*args, **kwargs)
        self.name_redis_key = name_redis_key
        self.sql = sql
        self.dict_bind = dict_bind
        self.oracle_conn_id = oracle_conn_id
        self.redis_conn_id = redis_conn_id

    def execute(self, context):
        oracle = OracleHelper(self.oracle_conn_id)
        redis = RedisHook(self.redis_conn_id)
        self.log.info(f"Executing SQL:{self.sql}")

        self.log.info("Extracting data from Oracle")
        conn_redis = redis.get_conn()
        records = oracle.get_rows_with_bind(sql=self.sql,
                                            bind=self.dict_bind)

        self.log.info("Inserting rows into Redis")
        pipe = conn_redis.pipeline()
        [pipe.lpush(self.name_redis_key, str(row)) for row in records]
        pipe.execute()
        self.log.info(f"Inserted {len(records)} rows.")
