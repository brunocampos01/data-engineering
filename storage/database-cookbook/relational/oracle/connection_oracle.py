# documentation: https://cx-oracle.readthedocs.io/en/latest/index.html
import os
import sys

import cx_Oracle

here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '1.0'
__author__ = 'Bruno Campos'

sql = 'SELECT * FROM v$version;'


def conn_oracle(user: str, pw: str, host: str, port: str, database: str):
    """ Connect to the database. """
    try:
        return cx_Oracle.connect(user,
                                 pw,
                                 host + ':' + port + '/' + database,
                                 encoding="UTF-8")

    except cx_Oracle.DatabaseError:
        raise


def create_cursor(conn: str):
    try:
        return conn.cursor()

    except Exception as err:
        print(f"Cursor Error: {err}")
        raise


def disconnect_oracle(conn: str, cursor: str):
    """
    Disconnect from the database. If this fails, for instance
    if the connection instance doesn't exist, ignore the exception.
    """

    try:
        cursor.close()
        conn.close()

    except cx_Oracle.DatabaseError as err:
        print(err)
        pass


def execute_statements(self, cursor: str, sql: str):
    """Execute whatever SQL statements"""
    list_rows = []
        
    try:
        result = cursor.execute(sql)
            
        for row in result:
            list_rows.append(row)
            
        return list_rows
    except cx_Oracle.DatabaseError:
        raise


def main():
    conn = conn_oracle(user=config.ORACLE_CONFIG['user'],
                       pw=config.ORACLE_CONFIG['pw'],
                       host=config.ORACLE_CONFIG['host'],
                       port=config.ORACLE_CONFIG['port'],
                       database=config.ORACLE_CONFIG['database'])
    cursor = create_cursor(conn=conn)
    execute_statements(cursor=cursor, sql=sql)
    disconnect_oracle(conn=conn, cursor=cursor)


if __name__ == '__main__':
    main()
