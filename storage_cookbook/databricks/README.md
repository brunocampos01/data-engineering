# Databricks — Connection & Utilities Cookbook

Recipes for connecting to Databricks on AWS from a local environment, running remote Spark sessions, and safely handling secrets.

---

## Files

| File | Purpose |
|------|---------|
| `databricks_remote_conn.py` | Remote `DatabricksSession` via Personal Access Token (PAT) |
| `databricks_oauth_conn.py` | Remote `DatabricksSession` without PAT — OAuth or AWS IAM |
| `get_remote_spark_encapsulate_dbutils.py` | `get_remote_spark()` helper + `DbUtils` wrapper for local/remote parity |
| `show_secrets.py` | Reveal a Databricks secret in plaintext for debugging |

---

## Requirements

```bash
pip install databricks-connect databricks-sdk pyspark
```

---

## Environment Variables

| Variable | Used in | Description |
|----------|---------|-------------|
| `HOST_DATABRICKS` | `get_remote_spark_encapsulate_dbutils.py` | Workspace host (e.g. `<workspace-id>.cloud.databricks.com`) |
| `TOKEN_DATABRICKS` | `get_remote_spark_encapsulate_dbutils.py` | Personal access token |
| `CLUSTER_ID_DATABRICKS` | `get_remote_spark_encapsulate_dbutils.py` | Target cluster ID |
| `PYSPARK_VENV_PYTHON` | `databricks_remote_conn.py` | Path to venv Python binary |
| `DATABRICKS_USER` | `databricks_remote_conn.py` | User identity |
| `DATABRICKS_HOST` | `databricks_oauth_conn.py` | Workspace URL — `https://<workspace-id>.cloud.databricks.com` |
| `DATABRICKS_CLIENT_ID` | `databricks_oauth_conn.py` | Service principal client ID — OAuth M2M only |
| `DATABRICKS_CLIENT_SECRET` | `databricks_oauth_conn.py` | Service principal client secret — OAuth M2M only |
| `AWS_ACCESS_KEY_ID` | `databricks_oauth_conn.py` | AWS access key — `connect_aws_iam()` only |
| `AWS_SECRET_ACCESS_KEY` | `databricks_oauth_conn.py` | AWS secret key — `connect_aws_iam()` only |
| `AWS_SESSION_TOKEN` | `databricks_oauth_conn.py` | AWS session token — assumed roles only |
| `ENVIRONMENT` | `show_secrets.py` | Databricks secret scope name |

---

## Usage

### Remote Spark connection via PAT (`databricks_remote_conn.py`)

```bash
# Option A — DatabricksSession (recommended)
python databricks_remote_conn.py

# Option B — spark-submit with an explicit remote URL
spark-submit \
  --remote "sc://<workspace-id>.cloud.databricks.com:443/;token=<token>;use_ssl=true;x-databricks-cluster-id=<cluster-id>" \
  databricks_remote_conn.py
```

---

### OAuth / AWS IAM connection — no PAT required (`databricks_oauth_conn.py`)

Use this when **Settings → Developer → Access tokens** is disabled by your org admin.

| Method | Function | Best for |
|--------|----------|---------|
| OAuth M2M | `connect_oauth_m2m()` | CI/CD pipelines, automated jobs |
| OAuth U2M | `connect_oauth_u2m()` | Local / interactive development |
| AWS IAM | `connect_aws_iam()` | Reuse existing AWS credentials |
| Config file | `connect_config_file()` | Shared config across scripts |

**OAuth U2M or config file — one-time setup:**
```bash
pip install databricks-cli
databricks configure --host https://<workspace-id>.cloud.databricks.com
# Follow the prompts — choose OAuth when asked for the auth type
```

**OAuth M2M — service principal setup:**
```bash
export DATABRICKS_HOST=https://<workspace-id>.cloud.databricks.com
export DATABRICKS_CLIENT_ID=<service-principal-client-id>
export DATABRICKS_CLIENT_SECRET=<service-principal-client-secret>
```
Create the service principal in the Databricks account console, assign it to the workspace, then generate its OAuth M2M credentials.

**AWS IAM — credential resolution order:**
1. `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` env vars (+ `AWS_SESSION_TOKEN` for assumed roles)
2. Shared credentials file — `~/.aws/credentials` (`aws configure`)
3. EC2 instance profile or ECS task role

```bash
export DATABRICKS_HOST=https://<workspace-id>.cloud.databricks.com
# AWS credentials are picked up automatically from env / ~/.aws / instance profile
```

**Python — pick the method that matches your setup:**
```python
from databricks_oauth_conn import (
    connect_oauth_m2m,
    connect_oauth_u2m,
    connect_aws_iam,
    connect_config_file,
)

spark = connect_aws_iam()     # or connect_oauth_m2m() / connect_oauth_u2m()
df = spark.read.table('dev_silver.bloomberg.ld_currency')
df.show(5)
```

---

### Remote Spark + DbUtils wrapper (`get_remote_spark_encapsulate_dbutils.py`)

`get_remote_spark()` returns a `SparkSession` connected to the remote cluster. Falls back to a local session if the connection fails.

`DbUtils` wraps `dbutils.widgets` so that the same code works both inside a Databricks notebook and from a local IDE:

| Scenario | Behaviour |
|----------|-----------|
| Running inside Databricks | Delegates to `dbutils.widgets` directly |
| Running remotely / locally | Widget values are read from / written to `/tmp/<key_name>.txt` |

```python
from get_remote_spark_encapsulate_dbutils import get_remote_spark, DbUtils

spark = get_remote_spark()
db = DbUtils(spark, dbutils)

db.text('output_path', 's3://my-bucket/bronze/landing/')
path = db.get('output_path')
```

---

### Reveal a Databricks secret (`show_secrets.py`)

Databricks automatically redacts secret values in notebook output. Inserting an invisible Unicode separator (U+2063) between every character breaks the redaction pattern, printing the raw value.

> Use only in non-production environments for debugging. Remove before committing.

```python
# Inside a Databricks notebook — set ENVIRONMENT to your secret scope name
scope = os.getenv('ENVIRONMENT')
secret = dbutils.secrets.get(scope, '<secret_name>')
```
