import os
import IPython


class SQLServerDWSql:
    """This class contains basic options to access on-premises DW."""
    format = "com.microsoft.sqlserver.jdbc.spark"

    def __init__(self):
        ipython = IPython.get_ipython()
        dbutils = ipython.user_ns["dbutils"] if ipython else None
        scope = os.environ["KVScope"] if "KVScope" in os.environ else ""
        sql_server_name = dbutils.secrets.get(scope=scope, key="dw-sql-server-name")
        self.driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        self.user = dbutils.secrets.get(scope, "dw-sql-server-user-name")
        self.password = dbutils.secrets.get(scope, 'dw-sql-server-user-password')
        self.port = dbutils.secrets.get(scope, 'dw-sql-server-port')
        self.jdbc_url = f'jdbc:sqlserver://{sql_server_name}:{self.port};'

    def options(self, db_name) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            'url': self.jdbc_url,
            "driver": self.driver,
            'databaseName': db_name,
            "TrustServerCertificate": "true"
        }
