# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Purpose

A comprehensive data engineering reference and project repository covering database connectivity patterns, a production-grade Hadoop/Airflow data lake, data quality pipelines (medallion architecture), and a Databricks data catalog.

## Code Style

- **Charset:** UTF-8, **Line endings:** LF, **Indent:** 4 spaces, **Max line length:** 100 characters (enforced by `.editorconfig`)

## Key Projects & Commands

### Data Lake (Hadoop + Airflow) — `projects/data_lake/hadoop_airflow/data-pipeline/`

**Setup:**
```bash
virtualenv -p python3 venv && source venv/bin/activate
pip3 install --require-hashes -r requirements.txt
set -a && source configs/.env
python3 src/utils/generate_conn_config.py
python3 src/utils/generate_dag_config.py
```

**Start infrastructure (run from project root):**
```bash
# Cache/broker stack (Redis + PostgreSQL)
cd cache/ && docker-compose up -d

# Main Airflow services
cd data-pipeline/ && docker-compose up -d

# Initialize Airflow metadata DB (first time only)
airflow db init
airflow users create --username $USER --firstname $USER --lastname $USER --role Admin --email $USER@xpto.company
python3 src/utils/prepare_env.py
```

**Start Airflow services:**
```bash
airflow webserver --daemon --port 8080
airflow celery flower --daemon
airflow scheduler --daemon
airflow celery worker --daemon
```

**Run tests:**
```bash
cd projects/data_lake/hadoop_airflow/data-pipeline
pytest tests -vv
```

### Reverse Index (Spark) — `projects/reverse_index/`

```bash
spark-submit jobs/job_generate_dict.py
spark-submit jobs/job_map_wordid_documentid.py
```
Requires: Python 3.8.10, Spark 3.2.0, Java 1.8.0_282.

## Architecture

### Data Lake (Hadoop + Airflow)

**Data flow:** Oracle DB → HDFS (Avro/Parquet) → Hive/Impala → Spark analysis

**Stack:** Oracle, HDFS, Hive, Impala, Spark, Redis (Celery broker), PostgreSQL 13 (Airflow metadata DB, replicated), Airflow 2.1.1 with CeleryExecutor, Kerberos/Sentry security.

**Plugin layout under `src/`:**
- `hooks/db/` and `hooks/hdfs/` — database and HDFS connection helpers
- `operators/` — custom Airflow operators (concat files, Oracle get/update, sync data, stats update)
- `transfers/oracle_table_to_hdfs.py` — core Oracle→HDFS transfer logic
- `utils/` — config helpers, DAG helpers, time handlers, environment preparation
- `dags/` — `import_table.py`, `import_file.py`, `pre_process.py`

**Pre-built JARs in `libs/`:** `ojdbc8.jar`, `spark-avro`, `spark-redis`, `avro-tools`, `parquet-tools`, `spark-xml`

**Key Airflow settings (`airflow.cfg`):** CeleryExecutor, dag_concurrency=64, parallelism=16, max_active_runs_per_dag=2, pool_size=1024.

### Data QA — `projects/data_qa/`

Medallion architecture (Bronze → Silver → Gold) on Databricks with Great Expectations for validation, PySpark for transforms, Azure Data Lake Storage. JSON schemas define expected data types per layer (`bronze/resources/schemas/`). Tests live in `tests/qa/`.

### Data Catalog — `projects/data_catalog_databricks/`

Populates Databricks Unity Catalog metadata from data documentation, generates Power BI dashboards, uses star schema (facts + dimensions). Requires Python 3.10.12 and Databricks Runtime 13.3 LTS (Spark 3.4.1).

### Storage Cookbook — `storage_cookbook/`

Standalone Python connection examples for 20+ databases (MySQL, PostgreSQL, Oracle via cx_Oracle, SQL Server via pyodbc, DB2, SQLite, DynamoDB, Elasticsearch, Redis, Hive). Each module follows the same pattern: `conn_*()` → `create_cursor()` → `execute_statements()` → `disconnect_*()`.

### Data Processing Utilities — `data_processing/`

- `nested_json_to_csv.py` — flattens nested JSON to CSV using Pandas
- `nested_xml_to_json.py` — recursive XML→JSON conversion
