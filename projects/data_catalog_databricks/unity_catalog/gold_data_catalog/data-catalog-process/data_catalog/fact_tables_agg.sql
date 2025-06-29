-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

CREATE WIDGET DROPDOWN data_owner DEFAULT "dev_uc_catalogs_owners"  CHOICES (VALUES 'dev_uc_catalogs_owners', 'test_uc_catalogs_owners', 'prod_uc_catalogs_owners');
CREATE WIDGET DROPDOWN data_catalog DEFAULT "dev_data_catalog"  CHOICES (VALUES 'dev_data_catalog', 'test_data_catalog', 'data_catalog');
CREATE WIDGET TEXT database_name DEFAULT "gold_data_catalog";
CREATE WIDGET DROPDOWN storage_account DEFAULT "magellanadlsdev"  CHOICES (VALUES 'magellanadlsdev', 'magellanadlstest', 'magellanadlsprod');

-- COMMAND ----------

SELECT "${data_owner}", "${data_catalog}", "${database_name}", "${storage_account}";

-- COMMAND ----------

USE CATALOG ${data_catalog};
USE DATABASE ${database_name};

-- COMMAND ----------

DROP TABLE IF EXISTS fact_tables_agg;

CREATE TABLE fact_tables_agg (
  year_month STRING NOT NULL COMMENT 'The period to aggregate.',
  sk_layer INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_source INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_table INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  total_cols_by_table INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_table.',
  new_cols_by_table INT COMMENT '`total_cols_by_table` - `lag_total_cols`',
  table_id STRING COMMENT 'Natural key. An unique representation of table in catalog.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/fact_tables_agg';

-- COMMAND ----------

ALTER TABLE fact_tables_agg OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fact_tables_agg ADD CONSTRAINT `gold_fact_tables_agg_pk_composite` 
PRIMARY KEY (`year_month`, `sk_layer`, `sk_source`, `sk_table`);

-- COMMAND ----------

ALTER TABLE fact_tables_agg ADD CONSTRAINT `gold_fact_tables_agg_sk_source_fk` 
FOREIGN KEY (`sk_source`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources` (`sk_source`)

-- COMMAND ----------

ALTER TABLE fact_tables_agg ADD CONSTRAINT `gold_fact_tables_agg_sk_table_fk` 
FOREIGN KEY (`sk_table`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources_tables` (`sk_table`)

-- COMMAND ----------

ALTER TABLE fact_tables_agg ADD CONSTRAINT `gold_fact_tables_agg_sk_layer_fk` 
FOREIGN KEY (`sk_layer`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_layers` (`sk_layer`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
