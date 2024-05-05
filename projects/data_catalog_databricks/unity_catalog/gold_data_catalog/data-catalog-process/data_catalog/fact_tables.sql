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

DROP TABLE IF EXISTS fact_tables;

CREATE TABLE fact_tables (
  sk_table INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_layer INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_source INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_table_frequency_ingestion INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_table_type_ingestion INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  pk_fact_table INT NOT NULL COMMENT 'Primay key.',
  qty_table INT COMMENT 'Calculate total tables by row.',
  qty_table_document INT COMMENT 'Number of blank spaces to be fill in table.',
  qty_table_documented INT COMMENT 'Number of blank spaces filled in table.',
  total_cols_by_table INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_table.',
  total_cols_by_source_by_data_steward INT COMMENT 'Count occurrences of sk_field within each partition defined by sk_source and tag_table_data_steward.',
  total_tables_by_source_by_data_steward INT COMMENT 'Count occurrences of sk_table within each partition defined by sk_source and tag_table_data_steward.',
  qty_table_document_total INT COMMENT 'Number of blank spaces to be fill in (fields + table).',
  qty_table_documented_total INT COMMENT 'Number of blank spaces filled in (fields + table).'
  )
USING delta
COMMENT 'Table populated with metadata by business users and data stewards, used for filling in descriptions and tags.'
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/fact_tables';

-- COMMAND ----------

ALTER TABLE fact_tables OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_pk_fact_table` PRIMARY KEY (`pk_fact_table`);

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_sk_layer_fk` 
FOREIGN KEY (`sk_layer`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_layers` (`sk_layer`);

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_sk_source_fk` 
FOREIGN KEY (`sk_source`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources` (`sk_source`);

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_sk_tag_table_frequency_ingestion_fk` 
FOREIGN KEY (`sk_tag_table_frequency_ingestion`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_table_frequency_ingestion` (`sk_tag_table_frequency_ingestion`);

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_sk_tag_table_type_ingestion_fk` FOREIGN KEY (`sk_tag_table_type_ingestion`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_table_type_ingestion` (`sk_tag_table_type_ingestion`)

-- COMMAND ----------

ALTER TABLE fact_tables ADD CONSTRAINT `gold_fact_tables_sk_table_fk` FOREIGN KEY (`sk_table`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources_tables` (`sk_table`)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
