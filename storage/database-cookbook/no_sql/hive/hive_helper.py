import configparser
import logging
import os

from pyhive import hive


class HiveHelper(object):

    def __init__(self, host: str, port: int, user: str):
        self.__conn = self.__connect(host=host,
                                     port=port,
                                     user=user)
        self.__cursor = self.__conn.cursor()

    def __connect(self, host: str, port: int, user: str):
        return hive.Connection(host=host,
                               port=port,
                               username=user,
                               auth='KERBEROS',
                               kerberos_service_name='hive')

    def __del__(self):
        self.__conn.close()

    def with_database(self, db: str):
        try:
            self.__cursor.execute(f"USE {db}")
            return self
        except hive.DatabaseError as err:
            logging.error(f"Failed execute query: \n{err},\db: {db}")
            raise

    def grant_privileges(self, user: str, db: str):
        """
        :param user: name new user
        :param database: db which new user can access
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute(
                f"GRANT ALL PRIVILEGES ON {db}.* TO '{user}'@'%';")
            self.__cursor.execute("FLUSH PRIVILEGES;")
            return self
        except hive.DatabaseError as err:
            logging.error(f"Failed execute query: \n{err},\db: {db}")

            raise

    def create_hive_roles(self):
        # TODO
        pass

    def create_database(self, db: str, comment_db: str):
        try:
            self.__cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db} COMMENT 'oracle: {comment_db}'")
            return self
        except hive.Error as err:
            logging.error(f"Failed execute query: \n{err},\db: {db}")
            raise

    def create_external_table(self, db: str, hdfs_path: str, hdfs_schema_path: str,
                              comment_table: str, comment_pk: str, comment_blob: str, comment_dt_ref: str):
        try:
            self.__cursor.execute(f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {db}.raw(
                ID_FILE        STRING       COMMENT 'oracle: {comment_pk}',
                BL_CONTENT     STRING       COMMENT 'oracle: {comment_blob}',
                PRIMARY KEY    (ID_FILE) DISABLE NOVALIDATE)
            PARTITIONED BY (
                YEAR      SMALLINT          COMMENT 'oracle: {comment_dt_ref}',
                MONTH     TINYINT           COMMENT 'oracle: {comment_dt_ref}',
                DAY       TINYINT           COMMENT 'oracle: {comment_dt_ref}')
            ROW FORMAT SERDE
                'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
            STORED AS
                AVRO
            LOCATION
                'hdfs://{hdfs_path}/{db}'
            TBLPROPERTIES (
                'avro.schema.url'='hdfs://{hdfs_schema_path}/{db}.avsc',
                'comment' = 'oracle: {comment_table}')""")
            return self
        except hive.DatabaseError as err:
            logging.error(f"Failed execute query: \n{err},\db: {db}")

            raise

    def execute_statement(self, statement: str):
        """
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute(statement)
            return self
        except hive.DatabaseError as err:
            logging.error(
                f"Failed execute query: {err}, query: {statement}")
            raise
