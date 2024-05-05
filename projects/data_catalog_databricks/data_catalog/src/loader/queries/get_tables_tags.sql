-- Result example:
-- +----------+-----------------+-------------+-----------------------------+---------+
-- | layer_raw|       source_raw|        table|                     tag_name|tag_value|
-- +----------+-----------------+-------------+-----------------------------+---------+
-- |dev_silver|         shipsure|accountmaster|     tag_table_type_ingestion|      ...|
-- |dev_silver|         shipsure|accountmaster|tag_table_frequency_ingestion|      ...|
-- |...       |         ...     |...          |...                          |      ...|
(
    SELECT
        catalog_name AS `layer_raw`,
        schema_name AS `source_raw`,
        table_name AS `table`,
        CONCAT('tag_table_', tag_name) AS `tag_name`,
        tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name == '<env>bronze'
    ORDER BY schema_name ASC
)

UNION ALL

(
    SELECT
        catalog_name AS `layer_raw`,
        schema_name AS `source_raw`,
        table_name AS `table`,
        CONCAT('tag_table_', tag_name) AS `tag_name`,
        tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name == '<env>silver'
    ORDER BY schema_name ASC
)

UNION ALL

(
    SELECT
        catalog_name AS `layer_raw`,
        schema_name AS `source_raw`,
        table_name AS `table`,
        CONCAT('tag_table_', tag_name) AS `tag_name`,
        tag_value
    FROM system.information_schema.table_tags
    WHERE catalog_name == '<env>gold'
    ORDER BY schema_name ASC
)
