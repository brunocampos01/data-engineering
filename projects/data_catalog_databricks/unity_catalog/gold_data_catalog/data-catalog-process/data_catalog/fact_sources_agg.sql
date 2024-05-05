-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

CREATE WIDGET DROPDOWN data_owner DEFAULT "dev_uc_catalogs_owners"  CHOICES (VALUES 'dev_uc_catalogs_owners', 'test_uc_catalogs_owners', 'prod_uc_catalogs_owners');
CREATE WIDGET DROPDOWN data_catalog DEFAULT "dev_data_catalog"  CHOICES (VALUES 'dev_data_catalog', 'test_data_catalog', 'data_catalog');
CREATE WIDGET TEXT database_name DEFAULT "gold_data_catalog";
CREATE WIDGET DROPDOWN storage_account DEFAULT "magellanadlsdev"  CHOICES (VALUES 'magellanadlsdev', 'magellanadlstest', 'magellanadlsprod');

-- COMMAND ----------

SELECT "${data_owner}", "${data_catalog}",  "${database_name}", "${storage_account}"

-- COMMAND ----------

USE CATALOG ${data_catalog};
USE DATABASE ${database_name};

-- COMMAND ----------

DROP TABLE IF EXISTS fact_sources_agg;

CREATE TABLE fact_sources_agg (
  `year_month` STRING NOT NULL COMMENT 'The period to aggregate.',
  `sk_source` INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  `data_steward` STRING NOT NULL COMMENT 'The steward of the data.',
  `total_cols_by_source` INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_source.',
  `total_cols_by_source_by_data_steward` INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_source and tag_table_data_steward.',
  `lag_total_cols` INT COMMENT '`LAG(total_cols_by_table) OVER(PARTITION BY sk_layer, sk_source, sk_table ORDER BY year_month)`',
  `new_cols_by_source` INT COMMENT '`total_cols_by_source` - `lag_total_cols`',
  `total_tables_by_source` INT COMMENT 'Count occurrences of sk_table within each partition defined by sk_source.',
  `total_tables_by_source_by_data_steward` INT COMMENT 'Count occurrences of sk_table within each partition defined by sk_source and tag_table_data_steward.',
  `lag_total_tables` INT COMMENT '`LAG(total_tables_by_source) OVER(PARTITION BY sk_source ORDER BY year_month)`',
  `new_tables_by_source` INT COMMENT '`total_tables_by_source` - `lag_total_tables`',
  `qty_source_document_total` INT COMMENT 'Number of blank spaces to be fill in (fields + tables + source).',
  `lag_document` INT COMMENT '`LAG(qty_source_document_total) OVER(PARTITION BY sk_source ORDER BY year_month)`',
  `new_document` INT COMMENT '`qty_source_document_total` - `lag_document`',
  `qty_source_documented_total` INT COMMENT 'Number of blank spaces filled in (fields + tables + source).',
  `lag_documented` INT COMMENT '`LAG(qty_source_documented_total) OVER(PARTITION BY sk_source ORDER BY year_month)`',
  `new_documented` INT COMMENT '`qty_source_documented_total` - `lag_documented`',
  `source` STRING COMMENT 'A source refers to the origin or location from which data is obtained. It can be one or more database, a file, an application, or any other system that generates or stores data.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/fact_sources_agg';

-- COMMAND ----------

ALTER TABLE fact_sources_agg OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fact_sources_agg ADD CONSTRAINT `gold_fact_sources_agg_pk_composite` 
PRIMARY KEY(`year_month`, `sk_source`, `data_steward`);

-- COMMAND ----------

ALTER TABLE fact_sources_agg ADD CONSTRAINT `gold_fact_sources_agg_sk_source_fk` 
FOREIGN KEY (`sk_source`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources` (`sk_source`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
