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

DROP TABLE IF EXISTS data_stewards;

CREATE TABLE data_stewards (
  `data_steward` STRING NOT NULL COMMENT 'The steward of the data.',
  `department` STRING COMMENT 'The area of the data steward.',
  `email` STRING COMMENT 'The e-mail of the data steward.',
  `phone_number` STRING COMMENT 'The phone of the data steward.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://bronze@${storage_account}.dfs.core.windows.net/data_catalog/data_stewards';

-- COMMAND ----------

ALTER TABLE data_stewards OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE data_stewards ADD CONSTRAINT `bronze_data_steward_pk` PRIMARY KEY(`data_steward`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
