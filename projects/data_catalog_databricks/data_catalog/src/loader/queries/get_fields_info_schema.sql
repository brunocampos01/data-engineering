-- columns
--  obj_type is used to filter only tables when the process try to update the descriptions
WITH tables_tmp AS (
    SELECT *
    FROM system.information_schema.tables
    WHERE table_catalog IN (
        '<env>bronze', '<env>silver', '<env>gold'
    )
    ORDER BY table_schema, table_name ASC
)

, tmp_fields AS (
    SELECT *
    FROM system.information_schema.columns
    WHERE table_catalog IN (
        '<env>silver', '<env>gold'
    )
    ORDER BY table_schema, table_name, column_name ASC
)

SELECT
    t1.table_catalog AS `layer_raw`,
    t1.table_schema AS `source_raw`,
    t1.table_name AS `table`,
    t2.column_name AS `field`,
    t2.full_data_type AS `data_type`,
    t2.comment AS `field_description`,
    CASE
        WHEN t1.table_type LIKE 'EXTERNAL' THEN 'TABLE'
        ELSE t1.table_type
    END AS `obj_type`,
    t1.created AS `field_created_in_uc_at`,
    t1.last_altered AS `field_last_updated_at`

FROM tables_tmp t1
JOIN tmp_fields t2 ON t1.table_catalog = t2.table_catalog
                   AND t1.table_schema = t2.table_schema
                   AND t1.table_name = t2.table_name;
