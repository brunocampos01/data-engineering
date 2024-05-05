-- Result example:
-- +----------+-----------------+--------------+--------------------+-------------------------+---------+
-- | layer_raw|       source_raw|         table|              column|                 tag_name|tag_value|
-- +----------+-----------------+--------------+--------------------+-------------------------+---------+
-- |dev_silver|imos_datalake_api|        invdet|          lastuserid|   tag_field_data_element|      ...|
-- |dev_silver|  imos_report_api| cp_coadetails|Counterparty_Shor...|       tag_field_category|      ...|
-- |...       |...              |...           |...                 |...                      |      ...|

WITH tmp_fields_tags AS (
    SELECT *
    FROM system.information_schema.column_tags
    WHERE catalog_name IN (
        '<env>bronze', '<env>silver', '<env>gold'
    )
    ORDER BY schema_name ASC
)
SELECT
    catalog_name AS `layer_raw`,
    schema_name AS `source_raw`,
    table_name AS `table`,
    column_name AS `field`,
    CONCAT('tag_field_', tag_name) AS `tag_name`,
    tag_value
FROM tmp_fields_tags;
