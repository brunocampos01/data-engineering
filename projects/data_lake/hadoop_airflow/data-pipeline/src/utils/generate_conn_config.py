import json
import os


def generate_conn_config(
    path_file: str,
    hostname: str,
    hive_conn: str,
    impala_conn: str,
    hdfs_conn: str,
    oracle_login: str,
    oracle_password: str,
    big_data_login: str,
    big_data_password: str
) -> None:
    data = {
        "oracle": {
            "DFE": {
                "conn": "name_conn",
                "conn_type": "Oracle",
                "host": "localhost",
                "login": oracle_login,
                "password": oracle_password,
                "port": "1521",
                "extra": "",
                "describe": "ctrl: file_name"
            }
        },
        "redis": {
            "broker": {
                "conn": "broker",
                "conn_type": "Redis",
                "host": hostname,
                "login": "None",
                "password": "None",
                "port": "6379",
                "extra": "{\"db\":0}",
                "describe": "Airflow broker"
            },
            "cache_file_name": {
                "conn": "cache_file_name",
                "conn_type": "Redis",
                "host": hostname,
                "login": "None",
                "password": "None",
                "port": "7001",
                "extra": "{\"db\":0}",
                "describe": "file_name"
            },
            "cache_doc_name": {
                "conn": "cache_doc_name",
                "conn_type": "Redis",
                "host": hostname,
                "login": "None",
                "password": "None",
                "port": "7001",
                "extra": "{\"db\":1}",
                "describe": "doc_name"
            },
            "cache_validation": {
                "conn": "cache_validation",
                "conn_type": "Redis",
                "host": hostname,
                "login": "None",
                "password": "None",
                "port": "7001",
                "extra": "{\"db\":15}",
                "describe": "cache validation"
            }
        },
        "big_data": {
            "webhdfs": {
                "conn": "webhdfs",
                "conn_type": "HDFS",
                "host": hdfs_conn,
                "login": big_data_login,
                "password": big_data_password,
                "port": "14000",
                "extra": "",
                "describe": "/data"
            },
            "hive": {
                "conn": "hive",
                "conn_type": "Hive Server 2 Thrift",
                "host": hive_conn,
                "login": big_data_login,
                "password": big_data_password,
                "port": "10000",
                "extra": "{\"authMechanism\":\"GSSAPI\", \"kerberos_service_name\":\"hive\"}",
                "describe": "db, tables, partitions"
            },
            "impala": {
                "conn": "impala",
                "conn_type": "Hive Server 2 Thrift",
                "host": impala_conn,
                "login": big_data_login,
                "password": big_data_password,
                "port": "21050",
                "extra": "{\"authMechanism\":\"KERBEROS\", \"kerberos_service_name\":\"impala\", \"run_set_variable_statements\":\"false\"}",
                "describe": "db, tables, partitions"
            },
            "airflow_db": {
                "conn": "airflow_db",
                "conn_type": "Postgres",
                "host": hostname,
                "login": "airflow",
                "password": "airflow",
                "port": "5432",
                "extra": "",
                "describe": "Airflow database"
            }
        }
    }

    with open(path_file, "w") as json_file:
        json.dump(data, json_file, indent=4)


def main():
    pwd = os.environ.get("PWD")
    path_file = f'{pwd}/configs/conn_config.json'
    hostname = os.environ.get("HOSTNAME")
    oracle_login = os.environ.get("ORACLE_LOGIN")
    oracle_password = os.environ.get("ORACLE_PASSWORD")
    big_data_login = os.environ.get("big_data_LOGIN")
    big_data_password = os.environ.get("big_data_PASSWORD")

    hive_conn = 'cluster1node03.xpto.company'
    impala_conn = 'cluster1node01.xpto.company'
    hdfs_conn = 'cluster1node02.xpto.company'

    generate_conn_config(path_file=path_file,
                         hostname=hostname,
                         hive_conn=hive_conn,
                         impala_conn=impala_conn,
                         hdfs_conn=hdfs_conn,
                         oracle_login=oracle_login,
                         oracle_password=oracle_password,
                         big_data_login=big_data_login,
                         big_data_password=big_data_password)

    print(f'\nfile: {path_file} generated.')


if __name__ == '__main__':
    main()
