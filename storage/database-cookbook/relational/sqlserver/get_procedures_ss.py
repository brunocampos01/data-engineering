import re
import os
import pyodbc
import sys


here = os.path.abspath(os.path.dirname(__file__))
path_config = os.path.join(here + '/..')
sys.path.insert(1, path_config)
import config

__version__ = '0.1'
__author__ = 'Bruno Campos'

path = os.path.join(here + '/../..')
path_dw = config.PATHS['dw']
path_procedures = config.PATHS['stored_procedures']
absolute_path_dw = os.path.join(path + path_dw)
absolute_path_procedures = os.path.join(path + path_dw + path_procedures)
print(absolute_path_procedures)


def conn(sql_server: str, server: str, database: str, username: str,
         password: str):
    try:
        return pyodbc.connect('DRIVER={' + sql_server + '};'
                              'SERVER=' + server + ';'
                              'DATABASE=' + database + ';'
                              'UID=' + username + ';'
                              'PWD=' + password)
    except pyodbc.Error:
        print(f"Connection failed!\nargs:\n{locals()}")
        raise


def execute_query(cnxn: str, query: str):
    cursor = cnxn.cursor()
    cursor.execute(query)
    return cursor


def get_sp(cur: 'pyodbc.Cursor'):
    sp = cur.fetchone()
    dict_sp = {}

    while sp:
        dict_sp[sp[1]] = [sp[2]]
        sp = cur.fetchone()

    return dict_sp


def create_sp_file(dict_sp: dict, path_procedure: str=absolute_path_procedures):
    for item in dict_sp.items():
        sp_name = item[0]
        path = os.path.join(path_procedure + sp_name + '.sql')

        with open(path, mode='w+', encoding='UTF8') as file_w:
            file = str(item[1]).replace('  ', ' ') \
                .replace('\\t', ' \t') \
                .replace('\\r', '\r') \
                .replace('\\n', '') \
                .replace('"]', '') \
                .replace('["', '') \
                .replace('\']', '') \
                .replace('[\'', '') \
                .replace('select', 'SELECT\n') \
                .replace('from', '\nFROM') \
                .replace('where', '\nWHERE') \
                .replace('order by', '\nORDER BY') \
                .replace(' on', ' ON')\
                .replace('and ', ' AND ') \
                .replace('as ', ' AS ') \
                .replace('cast ( ', 'CAST(') \
                .replace('convert ( ', 'CONVERT(') \
                .replace('then', 'THEN') \
                .replace('case', 'CASE') \
                .replace('when', 'WHEN') \
                .replace('else', 'ELSE') \
                .replace('convert ( ', 'CONVERT(') \
                .replace('join', 'JOIN') \
                .replace('left outer', 'LEFT OUTER') \
                .replace('right outer', 'RIGHT OUTER') \
                .replace('nolock', 'NOLOCK') \
                .replace('inner', 'INNER') \
                .replace('left outer', 'LEFT OUTER') \
                .replace('current', 'CURRENT') \
                .replace('procedure', 'PROCEDURE') \
                .replace('inner', 'INNER') \
                .replace('left outer', 'LEFT OUTER') \
                .replace('current', 'CURRENT') \
                .replace('timestamp', 'TIMESTAMP') \
                .replace('date', 'DATE') \
                .replace('\\', '')
            file = re.sub(' +', ' ', file) # remove 2 or more spaces

            file_w.write(file)
            print(f'\n\nProcedure: {sp_name}\nQuery: {str(file)}')


def main():
    sp_data_warehouse = \
        """
    SELECT schema_name(obj.schema_id) as schema_name,
           obj.name as procedure_name,
            mod.definition
    FROM sys.objects obj
    JOIN sys.sql_modules mod
         on mod.object_id = obj.object_id;
        """
    # source: https://dataedo.com/kb/query/sql-server/list-stored-procedures

    cnxn = conn(sql_server=config.SQLSERVER_CONFIG['sql_server'],
                server=config.SQLSERVER_CONFIG['server'],
                database=config.SQLSERVER_CONFIG['database'],
                username=config.SQLSERVER_CONFIG['username'],
                password=config.SQLSERVER_CONFIG['password'])

    cur = execute_query(cnxn=cnxn, query=sp_data_warehouse)

    dict_sp = get_sp(cur=cur)
    create_sp_file(dict_sp=dict_sp, path_procedure=absolute_path_procedures)


if __name__ == '__main__':
    main()
