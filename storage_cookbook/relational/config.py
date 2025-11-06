DB2_CONFIG = {
    'server':               '<IP>',
    'database':             '<DB_NAME>',
    'port':                 '50005',
    'user':                 '<USER_NAME>',
    'pw':                   '<PASSWD>'
}

ORACLE_CONFIG = {
    'host':                 '<IP>',
    'database':             '<DB_NAME>',
    'port':                 '1521',
    'user':                 '<USER_NAME>',
    'pw':                   '<PASSWD>'
}

SQLSERVER_CONFIG = {
    'sql_server':           'ODBC Driver 17 for SQL Server',
    'server':               '<IP>',
    'port':                 '1433',
    'database':             '<DB_NAME>',
    'user':                 '<USER_NAME>',
    'pw':                   '<PASSWD>'
}

POSTGRES_CONFIG = {
    'server':               '<IP>',
    'port':                 '5432',
    'database':             '<DB_NAME>',
    'user':                 '<USER_NAME>',
    'pw':                   '<PASSWD>'
}

MYSQL_CONFIG = {
    'server':               '<IP>',
    'port':                 '3306',
    'database':             '<DB_NAME>',
    'user':                 '<USER_NAME>',
    'pw':                   '<PASSWD>'
}

SHAREPOINT_CONFIG = {
    'user':                 '<USER_NAME>',
    'pw':                   '<password>',
    'tenant_name':          '<DB_NAME>',
    'site':                 '<USER_NAME>',
    'client_id':            '<client_id>',
    'client_secret':        '<client_secret>',
    'resource':             '<resource>'
}

PATHS = {
    'dw':                   '/data_base/',
    'ssis':                 '/etl/ssis/',
    'ssas':                 '/olap/analysis_services/',
    'stored_procedures':    'stored_procedures/',
    'tmp':                  '/tmp/',
    'name_dict_lineage':    'dict_olap_dw.pickle'
}
