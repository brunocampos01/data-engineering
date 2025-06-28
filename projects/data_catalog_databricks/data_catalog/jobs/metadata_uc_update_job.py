# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache
# MAGIC import os
# MAGIC
# MAGIC from data_catalog.src.base_data_catalog import BaseDataCatalog
# MAGIC from data_catalog.src.cleaner.data_cleaner import DataCleaner
# MAGIC from data_catalog.src.loader.map_sources_tables_loader import MapSourcesTablesLoader
# MAGIC from data_catalog.src.loader.excel_loader import ExcelLoader
# MAGIC from data_catalog.src.loader.uc_formatted_loader import UCFormattedLoader
# MAGIC from data_catalog.src.transformer.landing_transformer import LandingTransformer
# MAGIC from data_catalog.src.writer.delta_table_writer import DeltaTableWriter
# MAGIC from data_catalog.src.updater.updater import Updater
# MAGIC from library.logger_provider import LoggerProvider

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("layer_name", "silver")
dbutils.widgets.text("execute_only_fields", "False")
dbutils.widgets.text("file_name_catalog", "DataCatalog.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

file_name_catalog = dbutils.widgets.get("file_name_catalog")
layer_name = dbutils.widgets.get("layer_name")
execute_only_fields = eval(dbutils.widgets.get("execute_only_fields"))

env = os.getenv('Environment').lower()
logger = LoggerProvider.get_logger()
spark.catalog.setCurrentCatalog(f'{env}_{layer_name}')

# COMMAND ----------

execute_dbs_tbls = False
if not execute_only_fields:
    execute_dbs_tbls = True

base = BaseDataCatalog(
    spark=spark,
    layer_name=layer_name,
)
loader_excel = ExcelLoader(
    spark=spark,
    layer_name=layer_name,
    container_name='landing',
    folder='data_catalog',
)
loader_data_catalog = UCFormattedLoader(
    spark=spark,
    layer_name=layer_name,
)
map_sources_loader = MapSourcesTablesLoader(
    spark=spark,
    layer_name=layer_name,
    container_name='landing',
    folder='data_catalog',
)
landing_transformer = LandingTransformer(
    spark=spark,
    layer_name=layer_name,
)
data_cleaner = DataCleaner(
    spark=spark,
    layer_name=layer_name,
)
writer_delta_table = DeltaTableWriter(
    spark=spark,
    layer_name='landing',
    container_name='landing',
    folder='data_catalog',
)
writer_delta_table_views = DeltaTableWriter(
    spark=spark,
    layer_name='landing',
    container_name='landing',
    folder='data_catalog_processed',
)

dict_map_sources_tables = map_sources_loader.execute(file_name_catalog)
updater = Updater(
    spark=spark,
    layer_name=layer_name,
    dict_map_sources_tables=dict_map_sources_tables,
)

list_cols_default = base.get_list_cols_default()
base_path_queries = ''.join(os.getcwd() + '/../' + 'src/loader/queries/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Layers

# COMMAND ----------

df_excel_layers = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Layers')
df_origin_layers = data_cleaner.execute(df=df_excel_layers, table_name='layers')

logger.info(f'Total layers = {df_origin_layers.count()}')
df_origin_layers.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Sources

# COMMAND ----------

df_excel_sources = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Sources')
df_origin_sources = data_cleaner.execute(df=df_excel_sources, table_name='sources')

list_tags_names_sources = [c for c in df_origin_sources.columns if c not in list_cols_default]
df_sources = landing_transformer.execute(
    df_origin=df_origin_sources,
    dict_map_sources_tables=dict_map_sources_tables,
    table_name='sources',
    list_origin_tags_names=list_tags_names_sources,
)

logger.info(f'Tags in sources = {list_tags_names_sources}')
logger.info(f'Total sources = {df_sources.count()}')
df_sources.display(truncate=False, n=1000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### From Origin

# COMMAND ----------

df_excel_tables = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Tables')
df_origin_tables = data_cleaner.execute(df=df_excel_tables, table_name='tables')

list_origin_tables_tags_names = [c for c in df_origin_tables.columns if c not in list_cols_default]

logger.info(f'Tags in Excel tables = {list_origin_tables_tags_names}')
logger.info(f'Total in Excel tables = {df_origin_tables.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### From UC

# COMMAND ----------

file_path_sql_info_schema = ''.join(base_path_queries + '/get_tables_info_schema.sql')
file_path_sql_tags = ''.join(base_path_queries + '/get_tables_tags.sql')

df_uc_tables = loader_data_catalog.execute(
    file_path_sql_info_schema=file_path_sql_info_schema,
    file_path_sql_tags=file_path_sql_tags,
    table_name='tables',
)
list_uc_tables_tags_names = [c for c in df_uc_tables.columns if c not in list_cols_default]
logger.info(f'Total in UC tables = {df_uc_tables.count()}')

# COMMAND ----------

# NOTES:
# In views is not possible to add comments, because of this, we save the df to keep all view rows to make sure that all data will keep.
if layer_name == 'bronze':
    df_tables_only_views = df_origin_tables.filter(df_origin_tables['obj_type'] == 'VIEW')
    writer_delta_table_views.execute(
        df=df_tables_only_views,
        data_format='delta',
        table_name='tables_only_views',
        folder_name='data_catalog_processed',
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Catalog to Insert in UC: Tables

# COMMAND ----------

df_tables = landing_transformer.execute(
    df_origin=df_origin_tables,
    df_uc=df_uc_tables,
    dict_map_sources_tables=dict_map_sources_tables,
    table_name='tables',
    list_origin_tags_names=list_origin_tables_tags_names,
)

df_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Catalog: Fields

# COMMAND ----------

# MAGIC %md
# MAGIC ### From Origin

# COMMAND ----------

df_excel_fields = loader_excel.execute(file_name=file_name_catalog, sheet_tab='Fields')
df_origin_fields = data_cleaner.execute(df=df_excel_fields, table_name='fields')

list_origin_fields_tags_names = [c for c in df_origin_fields.columns if c not in list_cols_default]

# tmp
df_origin_fields = df_origin_fields.drop('sa_checked')

logger.info(f'Tags in Excel = {list_origin_fields_tags_names}')
logger.info(f'Total fields = {df_origin_fields.count()}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### From UC

# COMMAND ----------

file_path_sql_info_schema = ''.join(base_path_queries + '/get_fields_info_schema.sql')
file_path_sql_tags = ''.join(base_path_queries + '/get_fields_tags.sql')

df_uc_fields = loader_data_catalog.execute(
    file_path_sql_info_schema=file_path_sql_info_schema,
    file_path_sql_tags=file_path_sql_tags,
    table_name='fields',
)
df_uc_fields = data_cleaner.execute(df=df_uc_fields, table_name='fields')

list_uc_fields_tags_names = [c for c in df_uc_fields.columns if c not in list_cols_default]

logger.info(f'Total fields = {df_uc_fields.count()}')

# COMMAND ----------

# NOTES:
# In views is not possible to add comments, because of this, we save the a df to keep all view rows to make sure that all data will keep.
if layer_name == 'bronze':
    df_fields_only_views = df_origin_fields.filter(df_origin_fields['obj_type'] == 'VIEW')
    writer_delta_table_views.execute(
        df=df_fields_only_views,
        data_format='delta',
        table_name='fields_only_views',
        folder_name='data_catalog_processed',
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Catalog to Insert in UC: Fields

# COMMAND ----------

df_fields = landing_transformer.execute(
    df_origin=df_origin_fields,
    df_uc=df_uc_fields,
    table_name='fields',
    dict_map_sources_tables=dict_map_sources_tables,
    list_origin_tags_names=list_origin_fields_tags_names,
)
logger.info(f'Total fields = {df_fields.count()}')
df_fields.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Unity Catalog Metadata

# COMMAND ----------

updater.execute(
    delta_table_name='catalogs',
    df=df_origin_layers,
)

# COMMAND ----------

if execute_dbs_tbls:
    updater.execute(
        delta_table_name='sources',
        df=df_sources,
        list_tags=list_tags_names_sources,
    )

# COMMAND ----------

if execute_dbs_tbls:
    updater.execute(
        delta_table_name='tables',
        df=df_tables,
        list_tags=list_origin_tables_tags_names,
    )

# COMMAND ----------

if execute_only_fields:
    updater.execute(
        delta_table_name='fields',
        df=df_fields,
        list_tags=list_origin_fields_tags_names,
        type_metadata='descriptions',
    )

# COMMAND ----------

if execute_only_fields:
    updater.execute(
        delta_table_name='fields',
        df=df_fields,
        list_tags=list_origin_fields_tags_names,
        type_metadata='tags',
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
