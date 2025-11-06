# documentation: https://dev.mysql.com/doc/

import os
import sys

import mysql.connector

here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '1.0'
__author__ = 'Bruno Campos'

sql = 'SELECT * FROM v$version;'


def conn_mysql(host: str, port: str, user: str, pw: str, db: str):
    """ Connect to the database. """
    try:
        conn = mysql.connector.connect(host=host,
                                       port=port,
                                       user=user,
                                       password=pw,
                                       database=db)
        conn.autocommit = True
        conn.get_warnings = True

        return conn

    except mysql.connector.Error:
        raise


def create_cursor(conn: str):
    try:
        return conn.cursor(buffered=True)

    except Exception as err:
        print(f"Cursor Error: {err}")
        raise


def disconnect_mysql(conn: str, cursor: str):
    """
    Disconnect from the database. If this fails, for instance
    if the connection instance doesn't exist, ignore the exception.
    """
    try:
        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        pass


def execute_statements(cursor: str, sql: str):
    try:
        cursor.execute(sql)

    except mysql.connector.Error as err:
        print(f"Failed execute query: {err}, query: {sql}")
        raise


def main():
    conn = conn_mysql(user=config.MYSQL_CONFIG['user'],
                      pw=config.MYSQL_CONFIG['pw'],
                      host=config.MYSQL_CONFIG['host'],
                      port=config.MYSQL_CONFIG['port'],
                      db=config.MYSQL_CONFIG['database'])

    cursor = create_cursor(conn=conn)
    execute_statements(cursor=cursor, sql=sql)
    disconnect_mysql(conn=conn, cursor=cursor)


if __name__ == "__main__":
    main()
