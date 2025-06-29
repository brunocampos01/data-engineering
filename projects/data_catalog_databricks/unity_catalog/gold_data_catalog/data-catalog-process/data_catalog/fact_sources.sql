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

DROP TABLE IF EXISTS fact_sources;

CREATE TABLE fact_sources (
  sk_source INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  pk_fact_source INT NOT NULL COMMENT 'Primay key.',
  qty_source INT COMMENT 'Calculate total sources by row.',
  qty_source_document INT COMMENT 'Number of blank spaces to be fill in source.',
  qty_source_documented INT COMMENT 'Number of blank spaces filled in source.',
  total_tables_by_source INT COMMENT 'Count occurrences of sk_table within each partition defined by sk_source.',
  total_cols_by_source INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_source.',
  qty_source_document_total INT COMMENT 'Number of blank spaces to be fill in (fields + tables + source).',
  qty_source_documented_total INT COMMENT 'Number of blank spaces filled in (fields + tables + source).'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/fact_sources';

-- COMMAND ----------

ALTER TABLE fact_sources OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fact_sources ADD CONSTRAINT `gold_fact_sources_pk_fact_source` 
PRIMARY KEY(`pk_fact_source`);

-- COMMAND ----------

ALTER TABLE fact_sources ADD CONSTRAINT `gold_fact_sources_sk_source_fk` 
FOREIGN KEY (`sk_source`) REFERENCES  `${data_catalog}`.`${database_name}`.`dim_sources` (`sk_source`) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
