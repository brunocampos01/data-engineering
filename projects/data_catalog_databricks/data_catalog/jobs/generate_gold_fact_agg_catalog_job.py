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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

layer_name = 'gold'
env = os.getenv("Environment").lower()

logger = LoggerProvider.get_logger()
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

df_fact_sources = loader_delta_gold.execute('fact_sources')
df_fact_sources.createOrReplaceTempView("fact_sources")

df_fact_tables = loader_delta_gold.execute('fact_tables')
df_fact_tables.createOrReplaceTempView("fact_tables")

df_fact_fields = loader_delta_gold.execute('fact_fields')
df_fact_fields.createOrReplaceTempView("fact_fields")

df_dim_sources = loader_delta_gold.execute('dim_sources')
df_dim_sources.createOrReplaceTempView("dim_sources")  

df_dim_tables = loader_delta_gold.execute('dim_tables')
df_dim_tables.createOrReplaceTempView("dim_tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregated Facts: `fact_tables_agg`

# COMMAND ----------

def table_is_empty(table_name: str) -> bool:
    is_empty = False
    try:
        loader_delta_gold.execute(table_name)
    except Exception:
        is_empty = True
    
    return is_empty

# COMMAND ----------

# DBTITLE 1,create delta table in azure
is_empty = table_is_empty('fact_tables_agg')

if spark.catalog.tableExists(f"`{env}_data_catalog`.`gold`.`fact_tables_agg`") == False or is_empty:
    df_tmp_fact_agg = spark.sql("""
        SELECT
            date_format(b.table_last_data_updated_at, 'yyyy-MM') AS `year_month`,
            a.`sk_layer`,
            a.`sk_source`,
            a.`sk_table`,
            cast(
                CASE
                    WHEN `total_cols_by_table` IS NULL THEN 0
                    ELSE `total_cols_by_table`
                END AS INT
            ) AS `total_cols_by_table`,
            cast(0 AS INT) AS `new_cols_by_table`,
            b.`table_id` AS `table_id`
        FROM  `fact_tables` a 
        INNER JOIN `dim_tables` b ON a.sk_table = b.sk_table
        INNER JOIN `dim_sources` c ON a.sk_source = c.sk_source
        WHERE `sk_layer` <> 1 -- we don't have cols to doc in bronze
        GROUP BY
            `year_month`, a.`sk_layer`, a.`sk_source`, a.`sk_table`,
            a.`total_cols_by_table`, `new_cols_by_table`,
            c.`source`, b.`table_id`
        ORDER BY `year_month`
    """)
    
    df_tmp_fact_agg.printSchema()
    print(df_tmp_fact_agg.count())
    print('Creating delta table in gold/data_catalog/')
    writer_delta_table.execute(
        df=df_tmp_fact_agg,
        data_format='delta',
        table_name=f'fact_tables_agg',
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Updates

# COMMAND ----------

df_fact_agg_new_data = spark.sql("""
    SELECT 
        date_format(b.table_last_data_updated_at, 'yyyy-MM') AS `year_month`,
        a.`sk_layer`,
        a.`sk_source`,
        a.`sk_table`,
        cast(a.`total_cols_by_table` AS INT),
        b.`table_id` AS `table_id`
    FROM `fact_tables` a 
    INNER JOIN `dim_tables` b ON a.sk_table = b.sk_table
    INNER JOIN `dim_sources` c ON a.sk_source = c.sk_source

    GROUP BY
        `year_month`, a.`sk_layer`, a.`sk_source`, a.`sk_table`,
        a.`total_cols_by_table`, c.`source`, b.`table_id`
    ORDER BY `year_month`
""")
df_fact_agg_new_data.createOrReplaceTempView("fact_tables_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Cols

# COMMAND ----------

df_fact_agg_new_data = spark.sql("""
WITH tmp AS (
    -- add lag
    SELECT
        *,
        cast(
            CASE
                WHEN LAG(total_cols_by_table) OVER(PARTITION BY sk_layer, sk_source, sk_table ORDER BY year_month) IS NULL THEN 0
                ELSE LAG(total_cols_by_table) OVER(PARTITION BY sk_layer, sk_source, sk_table ORDER BY year_month)
            END AS INT
        ) AS `lag_total_cols`
    FROM fact_tables_agg
)
SELECT 
    `year_month`,
    `sk_layer`,
    `sk_source`,
    `sk_table`,
    cast(
        CASE
            WHEN total_cols_by_table IS NULL THEN 0
            ELSE total_cols_by_table
        END AS INT
        ) AS `total_cols_by_table`,
    -- `lag_total_cols`,
    cast(
        CASE
            WHEN `lag_total_cols` = 0 THEN 0 -- table without cols
            WHEN `total_cols_by_table` - `lag_total_cols` IS NULL THEN 0 -- not changed
            ELSE `total_cols_by_table` - `lag_total_cols`
        END AS INT
    ) AS `new_cols_by_table`,
    `table_id`
FROM tmp
WHERE `sk_layer` <> 1 -- we don't have cols to doc in bronze
""")

logger.info(f'Total rows = {df_fact_agg_new_data.count()}')
df_fact_agg_new_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge New Values

# COMMAND ----------

df_target = loader_delta_gold.execute('fact_tables_agg')
logger.info(f'Total fact_tables_agg before merge = {df_target.count()}')

df_target.createOrReplaceTempView("old_data")
df_fact_agg_new_data.createOrReplaceTempView("new_data")

spark.sql(f"""
    MERGE INTO `old_data` AS target
    USING (SELECT * FROM `new_data`) AS source
    ON
        target.`year_month` = source.`year_month` AND
        target.`sk_layer` = source.`sk_layer` AND
        target.`sk_source` = source.`sk_source` AND
        target.`sk_table` = source.`sk_table`
    WHEN NOT MATCHED THEN
        INSERT *
""")

# check merge
df_fact_agg_tables = loader_delta_gold.execute('fact_tables_agg')
logger.info(f'Total fact_tables_agg after merge = {df_fact_agg_tables.count()}')
df_fact_agg_tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregated Facts: `fact_sources_agg`

# COMMAND ----------

# DBTITLE 1,create delta table in azure
is_empty = False
try:
    loader_delta_gold.execute('fact_sources_agg')
except Exception:
    is_empty = True


if spark.catalog.tableExists(f"`{env}_data_catalog`.`gold`.`fact_sources_agg`") == False or is_empty:
    df_tmp_fact_agg = spark.sql("""
        SELECT 
            date_format(dim_t.`table_last_data_updated_at`, 'yyyy-MM') AS `year_month`,
            fact_s.`sk_source`,
            dim_t.`tag_table_data_steward` AS `data_steward`,
            -- new cols
            cast(fact_s.`total_cols_by_source` AS INT),
            cast(fact_t.`total_cols_by_source_by_data_steward` AS INT),
            cast(0 AS INT) AS `lag_total_cols`,
            cast(0 AS INT) AS `new_cols_by_source`,
            -- new tables
            cast(fact_s.`total_tables_by_source` AS INT),
            cast(fact_t.`total_tables_by_source_by_data_steward` AS INT),
            cast(0 AS INT) AS `lag_total_tables`,
            cast(0 AS INT) AS `new_tables_by_source`,
            -- new kpi's document
            cast(fact_s.`qty_source_document_total` AS INT),
            cast(0 AS INT) AS `lag_document`,
            cast(0 AS INT) AS `new_document`,
            -- new kpi's documented
            cast(fact_s.`qty_source_documented_total` AS INT),
            cast(0 AS INT) AS `lag_documented`,
            cast(0 AS INT) AS `new_documented`,
            -- extra cols
            dim_s.`source` AS `source`
        FROM `fact_tables` fact_t
        INNER JOIN `dim_tables` dim_t ON fact_t.sk_table = dim_t.sk_table
        INNER JOIN `dim_sources` dim_s ON fact_t.sk_source = dim_s.sk_source
        INNER JOIN `fact_sources` fact_s ON fact_t.sk_source = fact_s.sk_source

        GROUP BY
            `year_month`, dim_s.`source`, fact_s.`sk_source`, dim_t.`tag_table_data_steward`,
            fact_s.`total_cols_by_source`, fact_s.`total_tables_by_source`,
            fact_s.`qty_source_document_total`, fact_s.`qty_source_documented_total`,
            fact_t.`total_cols_by_source_by_data_steward`, fact_t.`total_tables_by_source_by_data_steward`
        ORDER BY `year_month`
    """)
    df_tmp_fact_agg.printSchema()
    print(df_tmp_fact_agg.count())
    df_tmp_fact_agg.display()
    print('Creating delta table in gold/data_catalog/')

    writer_delta_table.execute(
        df=df_tmp_fact_agg,
        data_format='delta',
        table_name=f'fact_sources_agg',
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get updates

# COMMAND ----------

df_fact_agg_new_data = spark.sql("""
    SELECT 
        date_format(dim_t.`table_last_data_updated_at`, 'yyyy-MM') AS `year_month`,
        fact_s.`sk_source`,
        dim_t.`tag_table_data_steward` AS `data_steward`,
        -- new cols
        cast(fact_s.`total_cols_by_source` AS INT),
        fact_t.`total_cols_by_source_by_data_steward`,
        cast(0 AS INT) AS `lag_total_cols`,
        cast(0 AS INT) AS `new_cols_by_source`,
        -- new tables
        cast(fact_s.`total_tables_by_source` AS INT),
        fact_t.`total_tables_by_source_by_data_steward`,
        cast(0 AS INT) AS `lag_total_tables`,
        cast(0 AS INT) AS `new_tables_by_source`,
        -- new kpi's document
        cast(fact_s.`qty_source_document_total` AS INT),
        cast(0 AS INT) AS `lag_document`,
        cast(0 AS INT) AS `new_document`,
        -- new kpi's documented
        cast(fact_s.`qty_source_documented_total` AS INT),
        cast(0 AS INT) AS `lag_documented`,
        cast(0 AS INT) AS `new_documented`,
        -- extra cols
        dim_s.`source` AS `source`
    FROM `fact_tables` fact_t
    INNER JOIN `dim_tables` dim_t ON fact_t.sk_table = dim_t.sk_table
    INNER JOIN `dim_sources` dim_s ON fact_t.sk_source = dim_s.sk_source
    INNER JOIN `fact_sources` fact_s ON fact_t.sk_source = fact_s.sk_source

    GROUP BY
         `year_month`, dim_s.`source`, fact_s.`sk_source`, dim_t.`tag_table_data_steward`,
         fact_s.`total_cols_by_source`, fact_s.`total_tables_by_source`, 
         fact_s.`qty_source_document_total`, fact_s.`qty_source_documented_total`,
         fact_t.`total_cols_by_source_by_data_steward`, fact_t.`total_tables_by_source_by_data_steward`
    ORDER BY `year_month`
""")
df_fact_agg_new_data.createOrReplaceTempView("fact_sources_agg")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate cols

# COMMAND ----------

df_fact_agg_new_data = spark.sql("""
WITH tmp_lag_cols AS (
    -- create lag cols
    SELECT
        year_month,
        sk_source,
        data_steward,
        total_cols_by_source,
        total_tables_by_source,
        total_cols_by_source_by_data_steward,
        total_tables_by_source_by_data_steward,
        qty_source_document_total,
        qty_source_documented_total,
        source,
        cast(
            CASE
                WHEN LAG(total_cols_by_source) OVER(PARTITION BY sk_source ORDER BY year_month) IS NULL THEN 0
                ELSE LAG(total_cols_by_source) OVER(PARTITION BY sk_source ORDER BY year_month)
            END AS INT
        ) AS `lag_total_cols`,
        cast(
            CASE
                WHEN LAG(total_tables_by_source) OVER(PARTITION BY sk_source ORDER BY year_month) IS NULL THEN 0
                ELSE LAG(total_tables_by_source) OVER(PARTITION BY sk_source ORDER BY year_month)
            END AS INT
        ) AS `lag_total_tables`,
        cast(
            CASE
                WHEN LAG(qty_source_document_total) OVER(PARTITION BY sk_source ORDER BY year_month) IS NULL THEN 0
                ELSE LAG(qty_source_document_total) OVER(PARTITION BY sk_source ORDER BY year_month)
            END AS INT
        ) AS `lag_document`,
        cast(
            CASE
                WHEN LAG(qty_source_documented_total) OVER(PARTITION BY sk_source ORDER BY year_month) IS NULL THEN 0
                ELSE LAG(qty_source_documented_total) OVER(PARTITION BY sk_source ORDER BY year_month)
            END AS INT
        ) AS `lag_documented`
    FROM fact_sources_agg
),
tmp_new_values AS(
    -- create new cols
    SELECT
        *,
        cast(
            CASE
                WHEN `total_cols_by_source` - `lag_total_cols` IS NULL THEN 0 -- not changed
                ELSE `total_cols_by_source` - `lag_total_cols`
            END AS INT
        ) AS `new_cols_by_source`,
        cast(
            CASE
                WHEN `total_tables_by_source` - `lag_total_tables` IS NULL THEN 0 -- not changed
                ELSE `total_tables_by_source` - `lag_total_tables`
            END AS INT
        ) AS `new_tables_by_source`,
        cast(
            CASE
                WHEN `qty_source_document_total` - `lag_document` IS NULL THEN 0 -- not changed
                ELSE `qty_source_document_total` - `lag_document`
            END AS INT
        ) AS `new_document`,
        cast(
            CASE
                WHEN `qty_source_documented_total` - `lag_documented` IS NULL THEN 0 -- not changed
                ELSE `qty_source_documented_total` - `lag_documented`
            END AS INT
        ) AS `new_documented`
    FROM tmp_lag_cols
)
SELECT
    `year_month`,
    `sk_source`,
    `data_steward`,
    -- new cols
    `total_cols_by_source`,
    `total_cols_by_source_by_data_steward`,
    `lag_total_cols`,
    `new_cols_by_source`,
    -- new tables
    `total_tables_by_source`,
    `total_tables_by_source_by_data_steward`,
    `lag_total_tables`,
    `new_tables_by_source`,
    -- new kpi's document
    `qty_source_document_total`,
    `lag_document`,
    `new_document`,
    -- new kpi's documented
    `qty_source_documented_total`,
    `lag_documented`,
    `new_documented`,
    -- extra cols
    `source`
FROM tmp_new_values
""")
df_fact_agg_new_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge New Values

# COMMAND ----------

df_target = loader_delta_gold.execute('fact_sources_agg')
logger.info(f'Total fact_tables_agg before merge = {df_target.count()}')

df_target.createOrReplaceTempView("old_data")
df_fact_agg_new_data.createOrReplaceTempView("new_data")


spark.sql(f"""
    MERGE INTO `old_data` AS target
    USING (SELECT * FROM `new_data`) AS source
    ON
        target.`year_month` = source.`year_month` AND
        target.`sk_source` = source.`sk_source` 
    WHEN NOT MATCHED THEN
        INSERT *
""")

df_fact_agg_sources = loader_delta_gold.execute('fact_sources_agg')
logger.info(f'Total fact_tables_agg after merge = {df_fact_agg_sources.count()}')

df_fact_agg_sources.display()
writer_delta_table.execute(
    df=df_fact_agg_sources,
    data_format='delta',
    table_name=f'fact_sources_agg',
)

# COMMAND ----------

# MAGIC %md
# MAGIC  ---
