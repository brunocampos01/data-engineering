# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ### A report to list the parameters defined for all the task on a workflow/job
# MAGIC
# MAGIC

# COMMAND ----------

# %sh
# pip install databricks-sdk

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql import Row
from pyspark.sql.types import * 
from pyspark.sql import functions as F

# COMMAND ----------

def parse_jobs_tasks_list(job):
    job_definition = w.jobs.get(job.job_id)
    job_tasks = job_definition.settings.tasks if job_definition else []
    return job_tasks        

def parse_job_tasks_definition(task_list):
    print(job_name)
    rows = [
        Row(
            job_name=job_name,
            task=task.task_key,
            notebook=task.notebook_task.notebook_path,
            parameter_key=k,
            parameter_value=v
        )
        for task in task_list
        if task.notebook_task and task.notebook_task.base_parameters
        for k, v in task.notebook_task.base_parameters.items()
    ]
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### job_name using dimesions
# MAGIC - bi-commercial-dim-silver-to-gold
# MAGIC - bi-conformed-silver-to-gold
# MAGIC - bi-finance-dim-silver-to-gold
# MAGIC - bi-hr-dim-silver-to-gold
# MAGIC - bi-market-data-silver-to-gold
# MAGIC - bi-procurement-dim-silver-to-gold

# COMMAND ----------

dbutils.widgets.text("job_name", "bi-procurement-dim-silver-to-gold")
job_name = dbutils.widgets.get("job_name")
host = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}'
token =dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
w = WorkspaceClient(host=host, token=token)
job_definition = next(filter(lambda job: job.settings.name == job_name, w.jobs.list()), None)

job_task_definitions = []
if job_definition:
    job_task_list = parse_jobs_tasks_list(job_definition)
    job_task_definitions = parse_job_tasks_definition(job_task_list)
    print(f"Number of tasks in the workflow: {len(job_task_list)}")
    print(f"Total Number of rows: {len(job_task_definitions)}")

if job_task_definitions:
    df = spark.createDataFrame(job_task_definitions)\
        .groupBy("job_name", "task", "notebook") \
        .pivot("parameter_key") \
        .agg(F.first("parameter_value"))
    df.display()
else:
    print(f"Workflow {job_name} not found")

# COMMAND ----------

df.filter(F.col('keep_removed_records').isin("True")).select('task').distinct().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct table_catalog, table_schema, table_name, f.constraint_name 
# MAGIC from test_gold.information_schema.constraint_column_usage f 
# MAGIC where 
# MAGIC     table_schema = 'dw_commercial'
# MAGIC     and table_name like "dim_%" and column_name like '%_sk'
# MAGIC     and exists ( 
# MAGIC     select * from test_gold.information_schema.columns c where f.table_name = c.table_name and f.column_name = c.column_name and c.ordinal_position != 1 
# MAGIC     )
# MAGIC    order by all
