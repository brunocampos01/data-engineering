-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()

-- COMMAND ----------

CREATE WIDGET DROPDOWN data_owner DEFAULT "dev_uc_catalogs_owners"  CHOICES (VALUES 'dev_uc_catalogs_owners', 'test_uc_catalogs_owners', 'prod_uc_catalogs_owners');
CREATE WIDGET DROPDOWN data_catalog DEFAULT "dev_data_catalog"  CHOICES (VALUES 'dev_data_catalog', 'test_data_catalog', 'data_catalog');
CREATE WIDGET TEXT database_name DEFAULT "silver_data_catalog";
CREATE WIDGET DROPDOWN storage_account DEFAULT "magellanadlsdev"  CHOICES (VALUES 'magellanadlsdev', 'magellanadlstest', 'magellanadlsprod');

-- COMMAND ----------

SELECT "${data_owner}", "${data_catalog}",  "${database_name}", "${storage_account}"

-- COMMAND ----------

USE CATALOG ${data_catalog};
USE DATABASE ${database_name};

-- COMMAND ----------

DROP TABLE IF EXISTS sources_tables;

CREATE TABLE sources_tables (
  `origin_table_id` STRING,
  `layer` STRING COMMENT 'A layer refers to the organization of data in the Data Lake according to the structure of the Medallion Architecture used by CSL such as: Bronze, Silver and Gold Layer.',
  `source` STRING COMMENT 'A source refers to the origin or location from which data is obtained. It can be one or more database, a file, an application, or any other system that generates or stores data.',
  `table` STRING COMMENT 'The table to which the field belongs',
  `obj_type` STRING COMMENT 'Identify if the object in UC is a TABLE or VIEW.',
  `layer_raw` STRING COMMENT 'Original name in Unity Catalog.',
  `source_raw` STRING COMMENT 'Original name in Unity Catalog.',
  `table_description` STRING COMMENT 'Provides a concise explanation of the data table, including its context, significance, and any relevant additional details.',
  `table_created_in_uc_at` TIMESTAMP COMMENT 'The date when the object was created in Unity Catalog.',
  `tag_table_data_steward` STRING COMMENT 'The steward of the table.',
  `tag_table_frequency_ingestion` STRING COMMENT 'It indicates the frequency to which the data is ingested into the Data Catalog.',
  `tag_table_source_system` STRING,
  `tag_table_type_ingestion` STRING COMMENT 'It indicates the type of the ingestion of the data into the Data Catalog. It can be incremental and full.',
  `array_tag_table_source_system` ARRAY<STRING>,
  `table_last_data_updated_at` TIMESTAMP COMMENT 'The last update data of the table.',
  `table_last_schema_updated_at` TIMESTAMP COMMENT 'The last update schema of the table.',
  `tag_table_last_data_steward` STRING COMMENT 'The previous steward of the table.',
  `source_exploded` STRING,
  `source_raw_updated` STRING,
  `sources_table_id` STRING NOT NULL COMMENT 'Natural key. An unique representation of field in catalog.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://silver@${storage_account}.dfs.core.windows.net/data_catalog/sources_tables';

-- COMMAND ----------

ALTER TABLE sources_tables OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE sources_tables ADD CONSTRAINT `silver_sources_tables_sources_table_id` PRIMARY KEY(`sources_table_id`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
