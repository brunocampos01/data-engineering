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

DROP TABLE IF EXISTS fields_data_usage;

CREATE TABLE fields_data_usage (
  `field_id` STRING NOT NULL COMMENT 'Natural key. An unique representation of field in catalog.',
  `field_data_usage` STRING NOT NULL,
  `field_created_in_uc_at` TIMESTAMP COMMENT 'The date when the object was created in Unity Catalog.',
  `field_last_updated_at` TIMESTAMP COMMENT 'The last update data or schema of the column.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://silver@${storage_account}.dfs.core.windows.net/data_catalog/fields_data_usage';

-- COMMAND ----------

ALTER TABLE fields_data_usage OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fields_data_usage ADD CONSTRAINT `silver_fields_data_usage_pk_composite` PRIMARY KEY(`field_id`, `field_data_usage`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
