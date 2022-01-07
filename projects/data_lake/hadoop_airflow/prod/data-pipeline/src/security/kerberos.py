import logging
import subprocess

import requests
from airflow.hooks.base import BaseHook
from hdfs.ext.kerberos import KerberosClient


def kerberos_auth(conn_id: str) -> 'hdfs.ext.kerberos.KerberosClient':
    logging.info(f'Getting kerberos ticket ...')
    login = BaseHook.get_connection(conn_id).login
    password = BaseHook.get_connection(conn_id).password
    host = BaseHook.get_connection(conn_id).host
    port = BaseHook.get_connection(conn_id).port

    passwd = subprocess.Popen(('echo', password), stdout=subprocess.PIPE)
    subprocess.call(('kinit', login), stdin=passwd.stdout)

    session = requests.Session()
    session.verify = False

    return KerberosClient(f'https://{host}:{port}',
                          mutual_auth="REQUIRED",
                          max_concurrency=256,
                          session=session)
