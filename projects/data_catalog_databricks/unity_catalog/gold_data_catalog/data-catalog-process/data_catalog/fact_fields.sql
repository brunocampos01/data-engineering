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

DROP TABLE IF EXISTS fact_fields;

CREATE TABLE fact_fields (
  sk_field INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_layer INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_source INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_table INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_business_relevant INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_category INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_data_usage INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_field_type INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_is_certified INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_is_derived INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  sk_tag_field_sensitive_level INT NOT NULL COMMENT 'The numeric artificial key is used to identify the row.',
  pk_fact_field INT NOT NULL COMMENT 'Primay key.',
  qty_field INT COMMENT 'Calculate total fields by row.',
  qty_field_document INT COMMENT 'Number of blank spaces to be fill in. e.g.: tag_field_source_of_truth + tag_field_data_element + tag_field_imo_data_number = 3',
  qty_field_documented INT COMMENT 'Number of blank spaces filled in field.'
  )
USING delta
LOCATION 'abfss://gold@${storage_account}.dfs.core.windows.net/data_catalog/fact_fields';

-- COMMAND ----------

ALTER TABLE fact_fields OWNER TO ${data_owner};

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_pk_fact_field` 
PRIMARY KEY(`pk_fact_field`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_sensitive_level_fk` 
FOREIGN KEY (`sk_tag_field_sensitive_level`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_sensitive_level` (`sk_tag_field_sensitive_level`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_field_fk` 
FOREIGN KEY (`sk_field`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_fields` (`sk_field`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_table_fk` 
FOREIGN KEY (`sk_table`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources_tables` (`sk_table`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_source_fk` 
FOREIGN KEY (`sk_source`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_sources` (`sk_source`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_is_derived_fk` 
FOREIGN KEY (`sk_tag_field_is_derived`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_is_derived` (`sk_tag_field_is_derived`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_data_usage_fk` 
FOREIGN KEY (`sk_tag_field_data_usage`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_data_usage` (`sk_tag_field_data_usage`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_business_relevant_fk` 
FOREIGN KEY (`sk_tag_field_business_relevant`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_business_relevant` (`sk_tag_field_business_relevant`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_is_certified_fk` 
FOREIGN KEY (`sk_tag_field_is_certified`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_is_certified` (`sk_tag_field_is_certified`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_layer_fk` 
FOREIGN KEY (`sk_layer`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_layers` (`sk_layer`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_field_type_fk` 
FOREIGN KEY (`sk_tag_field_field_type`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_field_type` (`sk_tag_field_field_type`);

-- COMMAND ----------

ALTER TABLE fact_fields ADD CONSTRAINT `gold_fact_fields_sk_tag_field_category_fk` 
FOREIGN KEY (`sk_tag_field_category`) REFERENCES `${data_catalog}`.`${database_name}`.`dim_tag_field_category` (`sk_tag_field_category`);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
