# documentation: https://www.psycopg.org/docs/usage.html

import os
import sys

import psycopg2

here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

query = 'SELECT version();'


def conn_postgres(username: str, password: str, server: str, database: str):
    try:
        return psycopg2.connect(user=username,
                                password=password,
                                host=server,
                                port='5432',
                                database=database)

    except psycopg2.Error as err:
        print("\nError while connecting to PostgreSQL", err)


def create_cursor(conn: str):
    try:
        return conn.cursor()

    except psycopg2.Error as err:
        print(f"\nCursor Error: {err}")
        raise


def disconnect_postgres(conn: str, cursor: str):
    """
    Disconnect from the database. If this fails, for instance
    if the connection instance doesn't exist, ignore the exception.
    """

    try:
        cursor.close()
        conn.close()
    except psycopg2.Error as err:
        print(f"\nError: {err}")
        pass


def execute_statements(cursor: str, query: str):
    list_result_query = []

    cursor.execute(query)
    print("Executing query: ", cursor.query)
    for row in cursor:
        list_result_query.append(row)

    return list_result_query


def main():
    conn = conn_postgres(server=config.POSTGRES_CONFIG['server'],
                         database=config.POSTGRES_CONFIG['database'],
                         username=config.POSTGRES_CONFIG['username'],
                         password=config.POSTGRES_CONFIG['password'])

    cursor = create_cursor(conn=conn)
    execute_statements(cursor=cursor, query=query)
    disconnect_postgres(conn=conn, cursor=cursor)


if __name__ == '__main__':
    main()
