import os
import sys

import pyodbc
import pandas as pd

here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '1.0'
__author__ = 'Bruno Campos'

sql = 'SELECT @@version;'


def create_pandas_df(*list_table, cnxn) -> list:
    list_df = []

    for table in list_table:
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, cnxn)
        logging.info(f'df name: {df}')
        logging.info(df.head(2))
        list_df.append(table)

    return list_df



def conn_sqlserver(sql_server: str, server: str, database: str,
                   user: str, pw: str):
    try:
        return pyodbc.connect(
            'DRIVER={' + sql_server + '};SERVER=' + server + ';DATABASE=' + database + ';UID=' + user + ';PWD=' + pw)

    except pyodbc.Error:
        print(f"Connection failed!\nargs:\n{locals()}")
        raise


def test_conn(cnxn):
    cursor = None

    try:
        cursor = cnxn.cursor()
        cursor.execute("SELECT @@version;")

    except Exception as err:
        logging.error(f"Error: {err}")
        raise

    finally:
        cursor.close()


def create_cursor(conn: str):
    try:
        return conn.cursor()

    except Exception:
        raise


def disconnect_sqlserver(conn: str, cursor: str):
    """
    Disconnect from the database. If this fails, for instance
    if the connection instance doesn't exist, ignore the exception.
    """

    try:
        cursor.close()
        conn.close()

    except Exception as err:
        print(err)
        pass


def execute_statements(cursor: str, sql: str):
    """
    Execute whatever SQL statements
    """
    try:
        cursor.execute(sql)

    except pyodbc.Error as ex:
        raise


def main():
    conn = conn_sqlserver(sql_server=config.SQLSERVER_CONFIG['sql_server'],
                          server=config.SQLSERVER_CONFIG['server'],
                          database=config.SQLSERVER_CONFIG['database'],
                          user=config.SQLSERVER_CONFIG['username'],
                          pw=config.SQLSERVER_CONFIG['password'])
    cursor = create_cursor(conn=conn)
    execute_statements(cursor=cursor, sql=sql)
    disconnect_sqlserver(conn=conn, cursor=cursor)


if __name__ == '__main__':
    main()
