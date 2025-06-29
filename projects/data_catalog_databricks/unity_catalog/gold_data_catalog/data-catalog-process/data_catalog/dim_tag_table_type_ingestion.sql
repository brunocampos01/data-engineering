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

DROP TABLE IF EXISTS dim_tag_table_type_ingestion;

CREATE TABLE dim_tag_table_type_ingestion (
  `tag_table_type_ingestion` STRING COMMENT 'It indicates the type of the ingestion of the data into the Data Catalog. It can be incremental and full.',
  `sk_tag_table_type_ingestion` INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/dim_tag_table_type_ingestion';

-- COMMAND ----------

ALTER TABLE dim_tag_table_type_ingestion OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE dim_tag_table_type_ingestion ADD CONSTRAINT `gold_dim_tag_table_type_ingestion_sk` PRIMARY KEY(`sk_tag_table_type_ingestion`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
