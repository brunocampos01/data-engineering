-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

CREATE WIDGET DROPDOWN data_owner DEFAULT "dev_uc_catalogs_owners"  CHOICES (VALUES 'dev_uc_catalogs_owners', 'test_uc_catalogs_owners', 'prod_uc_catalogs_owners');
CREATE WIDGET DROPDOWN data_catalog DEFAULT "dev_data_catalog"  CHOICES (VALUES 'dev_data_catalog', 'test_data_catalog', 'data_catalog');
CREATE WIDGET TEXT database_name DEFAULT "bronze_data_catalog";
CREATE WIDGET DROPDOWN storage_account DEFAULT "magellanadlsdev"  CHOICES (VALUES 'magellanadlsdev', 'magellanadlstest', 'magellanadlsprod');

-- COMMAND ----------

SELECT "${data_owner}", "${data_catalog}",  "${database_name}", "${storage_account}"

-- COMMAND ----------

USE CATALOG ${data_catalog};
USE DATABASE ${database_name};

-- COMMAND ----------

DROP TABLE IF EXISTS `tables`;

CREATE TABLE `tables` (
  layer STRING COMMENT 'A layer refers to the organization of data in the Data Lake according to the structure of the Medallion Architecture used by CSL such as: Bronze, Silver and Gold Layer.',
  source STRING COMMENT 'A source refers to the origin or location from which data is obtained. It can be one or more database, a file, an application, or any other system that generates or stores data.',
  table STRING COMMENT 'The table to which the field belongs',
  obj_type STRING COMMENT 'Identify if the object in UC is a TABLE or VIEW.',
  layer_raw STRING COMMENT 'Original name in Unity Catalog.',
  source_raw STRING COMMENT 'Original name in Unity Catalog.',
  table_description STRING COMMENT 'Provides a concise explanation of the data table, including its context, significance, and any relevant additional details.',
  table_created_in_uc_at TIMESTAMP COMMENT 'The date when the object was created in Unity Catalog.',
  table_last_updated_at TIMESTAMP NOT NULL COMMENT 'The last update data or schema of the table.',
  tag_table_data_steward STRING COMMENT 'The steward of the table.',
  tag_table_frequency_ingestion STRING COMMENT 'It indicates the frequency to which the data is ingested into the Data Catalog.',
  tag_table_source_system STRING,
  tag_table_type_ingestion STRING COMMENT 'It indicates the type of the ingestion of the data into the Data Catalog. It can be incremental and full.',
  table_id STRING NOT NULL COMMENT 'Natural key. An unique representation of table in catalog.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://bronze@${storage_account}.dfs.core.windows.net/data_catalog/tables';

-- COMMAND ----------

ALTER TABLE `tables` OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE `tables` ADD CONSTRAINT `bronze_tables_pk_composite` PRIMARY KEY(`table_id`, `table_last_updated_at`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
