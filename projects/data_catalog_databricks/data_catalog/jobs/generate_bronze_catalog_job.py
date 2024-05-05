# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache
# MAGIC import os
# MAGIC
# MAGIC from data_catalog.src.cleaner.data_cleaner import DataCleaner
# MAGIC from data_catalog.src.loader.excel_loader import ExcelLoader
# MAGIC from data_catalog.src.loader.map_sources_tables_loader import MapSourcesTablesLoader
# MAGIC from data_catalog.src.loader.uc_formatted_loader import UCFormattedLoader
# MAGIC from data_catalog.src.transformer.bronze_transformer import BronzeTransformer
# MAGIC from data_catalog.src.writer.delta_table_writer import DeltaTableWriter
# MAGIC from library.logger_provider import LoggerProvider

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("file_name_catalog", "DataCatalog.xlsx") # used to get sources names
dbutils.widgets.text("layer_name", "bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

file_name_catalog = dbutils.widgets.get("file_name_catalog")
layer_name = dbutils.widgets.get("layer_name")

logger = LoggerProvider.get_logger()
loader_excel = ExcelLoader(
    spark=spark,
    layer_name=layer_name,
    container_name='landing',
    folder='data_catalog',
)
loader_uc = UCFormattedLoader(
    spark=spark,
    layer_name=layer_name,
)
writer_delta_table = DeltaTableWriter(
    spark=spark,
    layer_name=layer_name,
    container_name=layer_name,
    folder='data_catalog',
)
map_sources_loader = MapSourcesTablesLoader(
    spark=spark,
    layer_name=layer_name,
    container_name='landing',
    folder='data_catalog',
)
bronze_transformer = BronzeTransformer(
    spark=spark,
    layer_name=layer_name,
)
data_cleaner = DataCleaner(
    spark=spark,
    layer_name=layer_name,
)

base_path_queries = ''.join(os.getcwd() + '/../' + 'src/loader/queries/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Data Stewards

# COMMAND ----------

# data steward is simple, because of this the reader get data from excel
df_excel_data_stewards = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Data Stewards')
df_origin_data_stewards = data_cleaner.execute(df=df_excel_data_stewards, table_name='data_stewards')

writer_delta_table.execute(
    df=df_origin_data_stewards,
    data_format='delta',
    table_name='data_stewards',
)

df_origin_data_stewards.show()
logger.info(f'Total data stewards = {df_origin_data_stewards.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Layers

# COMMAND ----------

df_excel_layers = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Layers')
df_origin_layers = data_cleaner.execute(df=df_excel_layers, table_name='layers')

writer_delta_table.execute(
    df=df_origin_layers,
    data_format='delta',
    table_name='layers',
)

logger.info(f'Total layers = {df_origin_layers.count()}')
df_origin_layers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Sources

# COMMAND ----------

file_path_sql_info_schema = ''.join(base_path_queries + 'get_databases_info_schema.sql')
file_path_sql_tags = ''.join(base_path_queries + 'get_databases_tags.sql')

df_uc_sources = loader_uc.execute(    
    file_path_sql_info_schema=file_path_sql_info_schema,
    file_path_sql_tags=file_path_sql_tags,
    table_name='sources',
)
df_uc_sources = data_cleaner.execute(
    df=df_uc_sources,
    table_name='sources',
    is_uc=True,
)
writer_delta_table.execute(
    df=df_uc_sources,
    data_format='delta',
    table_name='sources',
)

logger.info(f'Total sources with historical = {df_uc_sources.count()}')
df_uc_sources.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Tables

# COMMAND ----------

file_path_sql_info_schema = ''.join(base_path_queries + 'get_tables_info_schema.sql')
file_path_sql_tags = ''.join(base_path_queries + 'get_tables_tags.sql')

df_uc_tables = loader_uc.execute(
    file_path_sql_info_schema=file_path_sql_info_schema,
    file_path_sql_tags=file_path_sql_tags,
    table_name='tables',
)
df_uc_tables = data_cleaner.execute(
    df=df_uc_tables,
    table_name='tables',
    is_uc=True,
)
df_tables = bronze_transformer.execute(
    df=df_uc_tables,
    table_name='tables',
)
writer_delta_table.execute(
    df=df_tables,
    data_format='delta',
    table_name='tables',
)

logger.info(f'total of rows after merge = {df_tables.count()}')
df_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Table: Fields

# COMMAND ----------

file_path_sql_info_schema = ''.join(base_path_queries + 'get_fields_info_schema.sql')
file_path_sql_tags = ''.join(base_path_queries + 'get_fields_tags.sql')

df_uc_fields = loader_uc.execute(
    file_path_sql_info_schema=file_path_sql_info_schema,
    file_path_sql_tags=file_path_sql_tags,
    table_name='fields',
)
df_uc_fields = data_cleaner.execute(
    df=df_uc_fields,
    table_name='fields',
)
df_fields = bronze_transformer.execute(
    df=df_uc_fields,
    table_name='fields',
)
writer_delta_table.execute(
    df=df_fields,
    data_format='delta',
    table_name='fields',
)

logger.info(f'total of rows after merge = {df_fields.count()}')
df_fields.limit(100).display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
