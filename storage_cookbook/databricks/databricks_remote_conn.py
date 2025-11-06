# spark-submit --remote "sc://adb-4658342328695968.8.azuredatabricks.net:443/;token=99999999999999999;use_ssl=true;x-databricks-cluster-id=0000-204921-f3eoaw9a" test_conn.py
# ----------------------------------------------------------

from databricks.connect import DatabricksSession
from databricks.sdk.runtime.dbutils_stub import dbutils
import pyspark

def set_up():
    # export SPARK_HOME=/Users/bruno.campos/projects/venv_code/bin/python3
    os.environ["PYSPARK_PYTHON"] = "/Users/bruno.campos/projects//venv_code/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/bruno.campos/projects/venv_code/bin/python3"
    os.environ["USER"] = "bruno.campos@google.com"

    print(os.getenv('PYSPARK_PYTHON'))
    print(os.getenv('PYSPARK_DRIVER_PYTHON'))

print(pyspark.__version__)
set_up()

spark = DatabricksSession.builder.getOrCreate()
df = spark.read.table("dev_silver.bloomberg.ld_currency")
df.show(5)
