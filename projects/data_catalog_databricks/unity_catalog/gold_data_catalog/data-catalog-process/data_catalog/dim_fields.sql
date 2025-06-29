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

DROP TABLE IF EXISTS dim_fields;
CREATE TABLE IF NOT EXISTS dim_fields (
  `field_id` STRING COMMENT 'Natural key. An unique representation of field in catalog.',
  `layer` STRING COMMENT 'A layer refers to the organization of data in the Data Lake according to the structure of the Medallion Architecture used by CSL such as: Bronze, Silver and Gold Layer.',
  `source` STRING COMMENT 'A source refers to the origin or location from which data is obtained. It can be one or more database, a file, an application, or any other system that generates or stores data.',
  `table` STRING COMMENT 'The table to which the field belongs',
  `field` STRING COMMENT 'A field refers to a specific piece of information within a dataset. It represents a single category of data that is typically arranged in columns in a database or spreadsheet. Each field holds a specific type of data, such as a name, date, or numerical value.',
  `field_description` STRING COMMENT 'Provides a concise explanation of the data field, including its context, significance, and any relevant additional details.',
  `data_type` STRING COMMENT 'Classifies the nature of the data, such as numerical, textual, or categorical.',
  `field_created_in_uc_at` TIMESTAMP COMMENT 'The date when the object was created in Unity Catalog.',
  `field_last_updated_at` TIMESTAMP COMMENT 'The last update data or schema of the column.',
  `tag_field_data_element` STRING COMMENT 'Field friendily name.',
  `tag_field_imo_data_number` STRING COMMENT 'The corresponding field number in IMO Compendium.',
  `tag_field_source_of_truth` STRING COMMENT 'Indicates if the data is shared with IMO/ Indicates if the data is the source of truth.',
  `sk_field` INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/dim_fields';

-- COMMAND ----------

ALTER TABLE dim_fields OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE dim_fields ADD CONSTRAINT `gold_dim_fields_sk_field` PRIMARY KEY(`sk_field`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
