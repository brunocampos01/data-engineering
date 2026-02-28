# spark-submit --remote "sc://<host>.azuredatabricks.net:443/;token=<token>;use_ssl=true;x-databricks-cluster-id=<cluster-id>" test_conn.py
# ----------------------------------------------------------

import os

from databricks.connect import DatabricksSession
from databricks.sdk.runtime.dbutils_stub import dbutils
import pyspark


def set_up():
    venv_python = os.getenv('PYSPARK_VENV_PYTHON', '/path/to/venv/bin/python3')
    os.environ['PYSPARK_PYTHON'] = venv_python
    os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python
    os.environ['USER'] = os.getenv('DATABRICKS_USER', 'user@example.com')

    print(os.getenv('PYSPARK_PYTHON'))
    print(os.getenv('PYSPARK_DRIVER_PYTHON'))


print(pyspark.__version__)
set_up()

spark = DatabricksSession.builder.getOrCreate()
df = spark.read.table('dev_silver.bloomberg.ld_currency')
df.show(5)
