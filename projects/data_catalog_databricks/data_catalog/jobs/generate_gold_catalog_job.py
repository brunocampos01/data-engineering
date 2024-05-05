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
# MAGIC from data_catalog.src.transformer.gold_dim_transformer import GoldDimTransformer
# MAGIC from data_catalog.src.transformer.gold_fact_transformer import GoldFactTransformer
# MAGIC from data_catalog.src.transformer.gold_fact_measures_transformer import GoldFactMeasuresTransformer
# MAGIC from data_catalog.src.transformer.base_dw_transformer import DWTransformer
# MAGIC from data_catalog.src.writer.delta_table_writer import DeltaTableWriter
# MAGIC from data_catalog.src.utils import get_tag_names

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

layer_name = 'gold'

logger = LoggerProvider.get_logger()
loader_delta_silver = DeltaTableLoader(
    spark=spark,
    layer_name='silver',
    container_name='silver',
    folder_name='data_catalog',
)
loader_delta_gold = DeltaTableLoader(
    spark=spark,
    layer_name='gold',
    container_name='gold',
    folder_name='data_catalog',
)
writer_delta_table = DeltaTableWriter(
    spark=spark,
    layer_name=layer_name,
    container_name=layer_name,
    folder='data_catalog',
)
gold_dim_transformer = GoldDimTransformer(
    spark=spark,
    layer_name=layer_name,
)
gold_fact_transformer = GoldFactTransformer(
    spark=spark,
    layer_name=layer_name,
)
gold_fact_measures_transformer = GoldFactMeasuresTransformer(
    spark=spark,
    layer_name=layer_name,
)
dw_transformer = DWTransformer(
    spark=spark,
    layer_name=layer_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Layers Dimension

# COMMAND ----------

df_silver_layers = loader_delta_silver.execute('layers')
df_dim_layers = gold_dim_transformer.execute(df_silver_layers, 'layer')
writer_delta_table.execute(
    df=df_dim_layers,
    data_format='delta',
    table_name='dim_layers',
)

logger.info(f'Total layers = {df_dim_layers.count()}')
df_dim_layers.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sources Dimension

# COMMAND ----------

df_silver_sources = loader_delta_silver.execute('sources')
df_dim_sources = gold_dim_transformer.execute(df_silver_sources, 'source')
writer_delta_table.execute(
    df=df_dim_sources,
    data_format='delta',
    table_name='dim_sources',
)

logger.info(f'Total = {df_dim_sources.count()}')
df_dim_sources.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tables Dimension

# COMMAND ----------

df_silver_tables = loader_delta_silver.execute('tables')
df_dim_only_tables = gold_dim_transformer.execute(df_silver_tables, 'table')

logger.info(f'Total = {df_dim_only_tables.count()}')
df_dim_only_tables.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sources-Tables Dimension

# COMMAND ----------

df_silver_sources_tables = loader_delta_silver.execute('sources_tables')
df_dim_sources_tables = gold_dim_transformer.execute(df_silver_sources_tables, 'sources_tables')
writer_delta_table.execute(
    df=df_dim_sources_tables,
    data_format='delta',
    table_name='dim_sources_tables',
)

logger.info(f'Total = {df_dim_sources_tables.count()}')
df_dim_sources_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Dimensions: `dim_tables` + `dim_sources_tables`

# COMMAND ----------

df_dim_tables = gold_dim_transformer.execute_creation_relationalship(
    df_left=df_dim_only_tables,
    id_left='table_id',
    df_right=df_dim_sources_tables,
    id_right='sources_table_id',
)
writer_delta_table.execute(
    df=df_dim_tables,
    data_format='delta',
    table_name='dim_tables',
)

logger.info(f'Total = {df_dim_tables.count()}')
df_dim_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fields Dimension

# COMMAND ----------

df_silver_fields = loader_delta_silver.execute('fields')
df_dim_fields = gold_dim_transformer.execute(df_silver_fields, 'field')
writer_delta_table.execute(
    df=df_dim_fields,
    data_format='delta',
    table_name='dim_fields',
)

logger.info(f'Total fields = {df_dim_fields.count()}')
df_dim_fields.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Tags: Tags in Tables and Fields

# COMMAND ----------

list_sources_tags_not_dim = dw_transformer.get_list_sources_tags_not_dim()
list_tables_tags_not_dim = dw_transformer.get_list_tables_tags_not_dim()
list_fields_tags_not_dim = dw_transformer.get_list_fields_tags_not_dim()

list_tags_names_sources = get_tag_names(df_silver_sources)
dict_df_dim_tags_sources = gold_dim_transformer.execute_by_tags(
    df=df_silver_sources,
    list_tags_names=list_tags_names_sources,
    list_tags_not_dim=list_sources_tags_not_dim,
    writer_delta_table=writer_delta_table,
)

list_tags_names_tables = get_tag_names(df_silver_tables)
dict_df_dim_tags_tables = gold_dim_transformer.execute_by_tags(
    df=df_silver_tables,
    list_tags_names=list_tags_names_tables,
    list_tags_not_dim=list_tables_tags_not_dim,
    writer_delta_table=writer_delta_table,
)

list_tags_names_fields = get_tag_names(df_silver_fields)
dict_df_dim_tags_fields = gold_dim_transformer.execute_by_tags(
    df=df_silver_fields,
    list_tags_names=list_tags_names_fields,
    list_tags_not_dim=list_fields_tags_not_dim,
    writer_delta_table=writer_delta_table,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC # Facts

# COMMAND ----------

df_dim_layers.createOrReplaceTempView("layers")
df_dim_sources.createOrReplaceTempView("sources")
df_dim_tables.createOrReplaceTempView("tables")
df_dim_fields.createOrReplaceTempView("fields")

df_silver_sources.createOrReplaceTempView("silver_sources")
df_silver_tables.createOrReplaceTempView("silver_tables")
df_silver_fields.createOrReplaceTempView("silver_fields")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fact: Fields

# COMMAND ----------

df_base = spark.sql("""
    SELECT 
        fact.*,
        l.`sk_layer`,
        s.`sk_source`,
        t.`sk_table`,
        f.`sk_field`
    FROM 
        silver_fields fact LEFT JOIN layers l  ON fact.`layer` = l.`layer`
                           LEFT JOIN sources s ON fact.`source` = s.`source`
                           LEFT JOIN tables t  ON fact.`table_id` = t.`table_id`
                           LEFT JOIN fields f  ON fact.`field_id` = f.`field_id`
""")
df_base_with_tags = gold_fact_transformer.execute(
    df_base=df_base,
    fact_name='field',
    dict_df_dim_tags=dict_df_dim_tags_fields,
)

dict_list_tags = { # It is used to get all tags.
    'field': list(dict_df_dim_tags_fields.keys()),
}
dict_df = {'dim_fields': df_dim_fields}
df_fact_fields = gold_fact_measures_transformer.execute(
    df_fact=df_base_with_tags,
    df_base=df_silver_fields,
    fact_name='field',
    dict_list_tags=dict_list_tags,
    dict_df=dict_df,
)
# TODO: resolve magic numbers
total_cols_to_doc_fields = None

writer_delta_table.execute(
    df=df_fact_fields,
    data_format='delta',
    table_name=f'fact_fields',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fact: Tables

# COMMAND ----------

df_base = spark.sql("""
    SELECT 
        fact.*,
        l.`sk_layer`,
        s.`sk_source`,
        t.`sk_table`,
        t.`table_id`
    FROM 
        silver_tables fact LEFT JOIN layers l  ON fact.`layer` = l.`layer`
                           LEFT JOIN sources s ON fact.`source` = s.`source`
                           LEFT JOIN tables t  ON fact.`table_id` = t.`table_id`
""")
df_base_with_tags = gold_fact_transformer.execute(
    df_base=df_base,
    fact_name='table',
    dict_df_dim_tags=dict_df_dim_tags_tables,
)
dict_list_tags = { # It is used to get all tags. All tags are used in doc_total measure
    'table': list(dict_df_dim_tags_tables.keys()),
    'field': list(dict_df_dim_tags_fields.keys()),
}
dict_df = { # used in total measures
    'dim_tables': df_dim_tables,
    'fact_fields': df_fact_fields,
}

df_fact_tables = gold_fact_measures_transformer.execute(
    df_fact=df_base_with_tags,
    df_base=df_silver_tables,
    fact_name='table',
    dict_list_tags=dict_list_tags,
    dict_df=dict_df,
)
writer_delta_table.execute(
    df=df_fact_tables,
    data_format='delta',
    table_name=f'fact_tables',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Fact: Sources

# COMMAND ----------

df_base = spark.sql("""
    SELECT 
        fact.*,
        s.`sk_source`
    FROM 
        silver_sources fact LEFT JOIN sources s ON fact.`source` = s.`source`
""")
df_base_with_tags = gold_fact_transformer.execute(
    df_base=df_base,
    fact_name='source',
    dict_df_dim_tags=dict_df_dim_tags_sources,
)
dict_list_tags = { # It is used to get all tags. All tags are used in doc_total measure
    'source': list_tags_names_sources,
    'table': list(dict_df_dim_tags_tables.keys()),
    'field': list(dict_df_dim_tags_fields.keys()),
}
dict_df = { # used in total measures
    'dim_sources': df_dim_sources,
    'fact_tables': df_fact_tables,
    'fact_fields': df_fact_fields,
}
df_fact_sources = gold_fact_measures_transformer.execute(
    df_fact=df_base_with_tags,
    df_base=df_silver_sources,
    fact_name='source',
    dict_list_tags=dict_list_tags,
    dict_df=dict_df,
)
writer_delta_table.execute(
    df=df_fact_sources,
    data_format='delta',
    table_name=f'fact_sources',
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
