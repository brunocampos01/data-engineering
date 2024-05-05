# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache
# MAGIC import os
# MAGIC
# MAGIC from library.logger_provider import LoggerProvider
# MAGIC from data_catalog.src.loader.delta_table_loader import DeltaTableLoader
# MAGIC from data_catalog.src.writer.delta_table_writer import DeltaTableWriter
# MAGIC from data_catalog.src.transformer.silver_transformer import SilverTransformer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

layer_name = 'silver'
logger = LoggerProvider.get_logger()
loader_delta_bronze = DeltaTableLoader(
    spark=spark,
    layer_name='bronze',
    container_name='bronze',
    folder_name='data_catalog',
)
writer_delta_table = DeltaTableWriter(
    spark=spark,
    layer_name=layer_name,
    container_name=layer_name,
    folder='data_catalog',
)
silver_transformer = SilverTransformer(
    spark=spark,
    layer_name=layer_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Data Stewards

# COMMAND ----------

# data steward is simple, because of this the reader get data from excel
df_delta_data_stewards = loader_delta_bronze.execute('data_stewards')
df_delta_data_stewards = silver_transformer.execute(df_delta_data_stewards, 'data_stewards')
writer_delta_table.execute(
    df=df_delta_data_stewards,
    data_format='delta',
    table_name='data_stewards',
)

df_delta_data_stewards.show()
logger.info(f'Total data_stewards = {df_delta_data_stewards.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Layers

# COMMAND ----------

df_delta_layers = loader_delta_bronze.execute('layers')
df_delta_layers = silver_transformer.execute(df_delta_layers, 'layers')
writer_delta_table.execute(
    df=df_delta_layers,
    data_format='delta',
    table_name='layers',
)

df_delta_layers.show()
logger.info(f'Total layers = {df_delta_layers.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Sources

# COMMAND ----------

df_delta_sources = loader_delta_bronze.execute('sources')
df_delta_sources = silver_transformer.execute(df_delta_sources, 'sources')
writer_delta_table.execute(
    df=df_delta_sources,
    data_format='delta',
    table_name='sources',
)

df_delta_sources.display()
logger.info(f'Total sources = {df_delta_sources.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Tables

# COMMAND ----------

df_delta_tables = loader_delta_bronze.execute('tables')
df_delta_tables = silver_transformer.execute(df_delta_tables, 'tables')
writer_delta_table.execute(
    df=df_delta_tables,
    data_format='delta',
    table_name='tables',
)

df_delta_tables.display()
logger.info(f'Total tables = {df_delta_tables.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Sources-Tables

# COMMAND ----------

df_delta_sources_tables = silver_transformer.execute(df_delta_tables, 'sources_tables')
writer_delta_table.execute(
    df=df_delta_sources_tables,
    data_format='delta',
    table_name='sources_tables',
)

df_delta_sources_tables.display()
logger.info(f'Total source_tables = {df_delta_sources_tables.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Fields

# COMMAND ----------

df_delta_fields = loader_delta_bronze.execute('fields')
df_delta_fields = silver_transformer.execute(df_delta_fields, 'fields')
writer_delta_table.execute(
    df=df_delta_fields,
    data_format='delta',
    table_name='fields',
)

df_delta_fields.limit(10).display()
logger.info(f'Total fields = {df_delta_fields.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Data Usage

# COMMAND ----------

df_delta_fields_data_usage = silver_transformer.execute(df_delta_fields, 'fields_datausage')
writer_delta_table.execute(
    df=df_delta_fields_data_usage,
    data_format='delta',
    table_name='fields_data_usage',
)

df_delta_fields_data_usage.limit(10).display()
logger.info(f'Total fields_data_usage = {df_delta_fields_data_usage.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
