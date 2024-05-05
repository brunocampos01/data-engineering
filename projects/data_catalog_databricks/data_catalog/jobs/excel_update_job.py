# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache
# MAGIC import os
# MAGIC
# MAGIC from data_catalog.src.transformer.excel_transformer import ExcelTransformer
# MAGIC from data_catalog.src.writer.delta_table_writer import DeltaTableWriter
# MAGIC from data_catalog.src.loader.delta_table_loader import DeltaTableLoader
# MAGIC
# MAGIC from library.logger_provider import LoggerProvider

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("file_name_catalog", "DataCatalog.xlsx")

# COMMAND ----------

file_name_catalog = dbutils.widgets.get("file_name_catalog")

env = os.getenv('Environment').lower()
logger = LoggerProvider.get_logger()

# COMMAND ----------

layer_name = 'landing'

excel_transformer = ExcelTransformer(
    spark=spark,
    layer_name='landing',
)
writer_delta_table = DeltaTableWriter(
    spark=spark,
    layer_name=layer_name,
    container_name=layer_name,
    folder='data_catalog',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_data_stewards = excel_transformer.execute('data_stewards')
df_data_stewards.display()

# COMMAND ----------

df_layers = excel_transformer.execute('layers')
df_layers.display()

# COMMAND ----------

df_sources = excel_transformer.execute('sources')
df_sources.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables: get from UC and landing

# COMMAND ----------

df_tables = excel_transformer.execute('tables', 'uc')
df_delta_tables = excel_transformer.execute('tables', 'landing')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables: join UC + landing

# COMMAND ----------

df_tables = excel_transformer.execute(
    table_name='tables',
    df_uc=df_tables,
    df_landing=df_delta_tables,
)
logger.info(f'Total df_tables = {df_tables.count()}')
df_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fields: get from UC and landing

# COMMAND ----------

df_fields = excel_transformer.execute('fields', 'uc')
df_delta_fields = excel_transformer.execute('fields', 'landing')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fields: join UC + landing

# COMMAND ----------

df_fields = excel_transformer.execute(
    table_name='fields',
    df_uc=df_fields,
    df_landing=df_delta_fields,
)
logger.info(f'Total df_fields = {df_fields.count()}')
df_fields.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save new file

# COMMAND ----------

writer_delta_table.execute(
    list_df=[df_data_stewards, df_layers, df_sources, df_tables, df_fields],
    data_format='excel',
    table_name=f'fact_sources',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleansing the `data_catalog` folder

# COMMAND ----------

writer_delta_table.execute_cleaning('data_catalog', 'DataCatalog.xlsx')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
