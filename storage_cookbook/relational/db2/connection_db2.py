# documentation: https://www.ibm.com/support/knowledgecenter/SSEPGG_11.5.0/com.ibm.swg.im.dbclient.python.doc/doc/c0054699.html

import datetime
import os
import sys

import ibm_db
import ibm_db_dbi

here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '1.0'
__author__ = 'Bruno Campos'

sql = 'SELECT * FROM SYSIBMADM.ENV_INST_INFO;'


def conn_db2(user, pw, host, port, database):
    """ Connect to the database. """
    conn_str = 'DATABASE=' + {database} + '; HOSTNAME=' + {host} + '; PORT=' + {port} + '; PROTOCOL=TCPIP; UID=' + {
        user} + '; PWD=' + {pw} + ';'

    try:
        return ibm_db_dbi.Connection(ibm_db.connect(conn_str, '', ''))

    except Exception:
        print(f"Failed connect: {ibm_db.conn_errormsg()}")
        raise


def disconnect_db2(conn: str):
    """
    Disconnect from the database. If this fails, for instance
    if the connection instance doesn't exist, ignore the exception.
    """

    try:
        conn.close()
    except Exception:
        print(f"Failed disconnect: {ibm_db.conn_errormsg()}")
        pass


def execute_statements(conn: str, sql: str):
    """
    Execute whatever SQL statements
    """
    try:
        stmt = ibm_db.exec_immediate(conn, sql)
        print("Number of affected rows: ", ibm_db.num_rows(stmt))

    except Exception:
        print(f"Failed execute query: {ibm_db.stmt_errormsg}")
        raise


def main():
    conn = conn_db2(user=config.ORACLE_CONFIG['user'],
                    pw=config.ORACLE_CONFIG['pw'],
                    host=config.ORACLE_CONFIG['host'],
                    port=config.ORACLE_CONFIG['port'],
                    database=config.ORACLE_CONFIG['database'])

    execute_statements(conn=conn, sql=sql)
    disconnect_db2(conn=conn)


if __name__ == '__main__':
    main()


def save_csv(df: 'dataframe' = None, path: str = '/') -> None:
    df.to_csv(path_or_buf=path,
              sep=',',
              index=False,
              encoding='utf8')

    return print("Data recorded!")


def main():
    query = prepare_query('2019-08-21 00:00:00', '2020-02-21 23:59:59')
    df_cognos = execute_query(conn_str, query)
    save_csv(df=df_cognos, path='../data/cognos.csv')


if __name__ == '__main__':
    start = datetime.datetime.now()
    main()
    print(f'total time: {datetime.datetime.now() - start}')
