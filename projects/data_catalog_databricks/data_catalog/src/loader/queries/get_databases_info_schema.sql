-- databases (schemas)

-- NOTES:
-- It is necessary to get the layer_raw because we have the tags and comments associated by "layer" with "source".
-- e.g.:
-- dev_bronze -> bloomberg -> source_description: None
-- dev_silver -> bloomberg -> source_description: "This sources contains information related the ..."

WITH tmp_dbs AS (
    SELECT *
    FROM system.information_schema.schemata
    WHERE catalog_name IN (
        '<env>bronze', '<env>silver', '<env>gold'
    )
    ORDER BY schema_name ASC
)
SELECT
    catalog_name AS `layer_raw`,
    schema_name AS `source_raw`,
    comment AS `source_description`,
    created AS `source_created_in_uc_at`,
    last_altered AS `source_last_updated_at`
FROM tmp_dbs;
