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

gold_catalog = CatalogManager.gold_catalog_name(spark)
schema_name = 'dw_commercial'
type_table = 'fact'

# COMMAND ----------

df_sk = spark.sql(f'''
select 
    concat_ws('.', table_catalog, table_schema, table_name) as table_id, 
    column_name,
    table_name
from system.information_schema.columns
where 
  table_catalog = '{gold_catalog}'
  and table_schema = '{schema_name}'
  and table_name like '{type_table}%'
  and column_name like '%_sk'
''')
list_sk_cols = list({row.table_name for row in df_sk.collect()})
print(f'Tables with SK cols: {list_sk_cols} ')
print(f'Total SK in UC: {df_sk.count()}')
df_sk.display()
df_sk.createOrReplaceTempView('sk_uc')

# COMMAND ----------

df_fk = None

for table_name in list_sk_cols:
    path_uc = f'{gold_catalog}.{schema_name}.{table_name}'
    df = spark.sql(f'DESCRIBE EXTENDED {path_uc}')
    pattern = r"REFERENCES\s*`([^`]+)`.`([^`]+)`.`([^`]+)`"
    df = df \
        .filter("data_type like '%FOREIGN KEY%'") \
        .withColumn("column_name", split("data_type", "`")[1]) \
        .withColumn("layer", regexp_extract(col("data_type"), pattern, 1)) \
        .withColumn("schema", regexp_extract(col("data_type"), pattern, 2)) \
        .withColumn("table", regexp_extract(col("data_type"), pattern, 3)) \
        .withColumn("constraint_origin", concat_ws(".", col("layer"), col("schema"), col("table"))) \
        .withColumn('table_id', lit(path_uc)) \
        .filter(col('table_id').contains(type_table)) \
        .select('table_id', 'column_name', 'constraint_origin')

    if df_fk is None:
        df_fk = df
    else:
        df_fk = df_fk.union(df)

print(f'Total FK in UC: {df_fk.count()}')
df_fk.display()
df_fk.createOrReplaceTempView('fk_uc')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT a.table_id, a.column_name, b.table_id, b.column_name
# MAGIC FROM sk_uc a LEFT JOIN fk_uc b ON a.table_id = b.table_id and a.column_name = b.column_name
# MAGIC WHERE b.column_name is null
# MAGIC ORDER BY a.table_id
