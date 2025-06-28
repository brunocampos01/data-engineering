# Databricks notebook source
from pyspark.sql.functions import (
    col,
    regexp_extract,
    lit,
    split,
    to_timestamp,
    concat_ws,
)

from library.catalog_manager import CatalogManager

# COMMAND ----------

uc_gold_catalog = CatalogManager.gold_catalog_name(spark)
uc_schema_name = 'dw_report'
onpremises_catalog = 'XPTOdw'
onpremises_schema_name = 'report'

# COMMAND ----------

df = spark.sql(f'''
select
    concat_ws('.', table_catalog, table_schema, table_name) as table_id,
    table_schema,
    table_name AS view_name,
    replace(table_schema, 'dw_', '') as table_schema_no_prefix,
    replace(table_name, 'v_', '') as view_name_no_prefix
from system.information_schema.views
where
  table_catalog = '{uc_gold_catalog}'
  and table_schema = '{uc_schema_name}'
''')
list_views_name = sorted([v[0] for v in df.select('view_name').collect()])
print(f'all views in {uc_gold_catalog}.{uc_schema_name}:\n{list_views_name}')
df.display()
df.createOrReplaceTempView('uc_views')

# COMMAND ----------

from library.database.sqlserver_dw_sql import SQLServerDWSql
loader = SQLServerDWSql()

query = f'''
SELECT
    LOWER(CONCAT(
        CASE
            WHEN TABLE_CATALOG = 'XPTOdw' THEN '{onpremises_catalog}'
            ELSE TABLE_CATALOG
        END,
        '.',
        CASE
            WHEN TABLE_SCHEMA = 'commercial' THEN 'dw_commercial'
            ELSE TABLE_SCHEMA
        END,
        '.',
        TABLE_NAME
    )) AS table_id,
    TABLE_CATALOG,
    CASE
        WHEN TABLE_SCHEMA = 'commercial' THEN 'dw_commercial'
        ELSE TABLE_SCHEMA
    END AS table_schema,
    LOWER(TABLE_NAME) AS view_name,
    VIEW_DEFINITION
FROM INFORMATION_SCHEMA.VIEWS
WHERE
    TABLE_CATALOG = '{onpremises_catalog}'
    AND TABLE_SCHEMA = '{onpremises_schema_name}'
'''

df = spark.read.format('jdbc')\
    .options(**loader.options(onpremises_catalog))\
    .option("query", query)\
    .load()
df.display()
df.createOrReplaceTempView('on_premises_views')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.*
# MAGIC FROM on_premises_views a LEFT JOIN uc_views b ON a.table_schema = b.table_schema AND a.view_name = b.view_name
# MAGIC -- FROM on_premises_views a LEFT JOIN uc_views b ON a.table_schema = b.table_schema_no_prefix AND a.view_name = b.view_name_no_prefix -- dw_report
# MAGIC WHERE
# MAGIC   b.table_id IS NULL
# MAGIC   AND a.view_name not like "%xref%"
# MAGIC   AND a.view_name not like "%bak%"
# MAGIC ORDER BY a.table_id

# COMMAND ----------

# MAGIC %md
# MAGIC - [] dw_commercial
# MAGIC - [] dw_report
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test

# COMMAND ----------

core_notebook = "view-silver_to_gold"

for view_name in list_views_name:
    try:
        execution_parameters = {
            "sql_database": "onpremise",
            # "view_db_name": f"{view_name}", # commercial
            "view_db_name": f"{view_name.replace('v_', '')}",
            "schema_gold_name": f"{uc_schema_name}",
            "view_gold_name": f"{view_name}",
            "catalog_db_name": f"{onpremises_catalog}",
            "schema_db_name": f"{onpremises_schema_name}",
        }
        print(dbutils.notebook.run(core_notebook, 60*60, execution_parameters))
    except Exception as e:
        print(f'Not working on {view_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
