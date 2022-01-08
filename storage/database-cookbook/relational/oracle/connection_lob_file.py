import cx_Oracle

# https://cx-oracle.readthedocs.io/en/latest/user_guide/sql_execution.html#outputtypehandlers
def output_type_handler(self, cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.CLOB:
        return cursor.var(cx_Oracle.LONG_STRING, arraysize=cursor.arraysize)
    if defaultType == cx_Oracle.BLOB:
        return cursor.var(cx_Oracle.LONG_BINARY, arraysize=cursor.arraysize)

def conn_lob_oracle(self, user: str, pw: str, host: str, port: str, service_name: str):
    """ Connect to the database. """
    try:
        dsn_tns = cx_Oracle.makedsn(host, port, service_name=service_name)
        conn = cx_Oracle.connect(user=user, password=pw, dsn=dsn_tns)
        conn.outputtypehandler = self.output_type_handler
        
        return conn
    except cx_Oracle.DatabaseError:
        raise
