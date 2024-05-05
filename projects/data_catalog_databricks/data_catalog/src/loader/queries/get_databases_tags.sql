--- Result example:
-- +----------+--------------------+------------------------------+---------+
-- | layer_raw|          source_raw|                      tag_name|tag_value|
-- +----------+--------------------+------------------------------+---------+
-- |dev_silver|                xref|tag_source_csl_internal_system|        _|
-- |dev_silver|                xref|      tag_source_active_system|        _|
-- |...       |...                 |...                           |...      |

WITH tmp_dbs_tags AS (
    SELECT *
    FROM system.information_schema.schema_tags
    WHERE catalog_name IN (
        '<env>bronze', '<env>silver', '<env>gold'
    )
    ORDER BY schema_name ASC
)
SELECT
    catalog_name AS `layer_raw`,
    schema_name AS `source_raw`,
    CONCAT('tag_source_', tag_name) AS `tag_name`,
    tag_value
FROM tmp_dbs_tags;
