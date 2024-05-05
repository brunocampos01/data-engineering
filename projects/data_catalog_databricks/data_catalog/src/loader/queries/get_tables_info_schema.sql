-- tables
(
    SELECT
        CASE
            WHEN table_type LIKE 'EXTERNAL' THEN 'TABLE'
            ELSE table_type
        END AS `obj_type`,
        table_catalog AS `layer_raw`,
        table_schema AS `source_raw`,
        table_name AS `table`,
        comment AS `table_description`,
        created AS `table_created_in_uc_at`,
        last_altered AS `table_last_updated_at`
    FROM system.information_schema.tables
    WHERE table_catalog =  '<env>bronze'
    ORDER BY table_schema, table_name ASC
)

UNION ALL

(
    SELECT
        CASE
            WHEN table_type LIKE 'EXTERNAL' THEN 'TABLE'
            ELSE table_type
        END AS `obj_type`,
        table_catalog AS `layer_raw`,
        table_schema AS `source_raw`,
        table_name AS `table`,
        comment AS `table_description`,
        created AS `table_created_in_uc_at`,
        last_altered AS `table_last_updated_at`
    FROM system.information_schema.tables
    WHERE table_catalog =  '<env>silver'
    ORDER BY table_schema, table_name ASC
)

UNION ALL

(
    SELECT
        CASE
            WHEN table_type LIKE 'EXTERNAL' THEN 'TABLE'
            ELSE table_type
        END AS `obj_type`,
        table_catalog AS `layer_raw`,
        table_schema AS `source_raw`,
        table_name AS `table`,
        comment AS `table_description`,
        created AS `table_created_in_uc_at`,
        last_altered AS `table_last_updated_at`
    FROM system.information_schema.tables
    WHERE table_catalog =  '<env>gold'
    ORDER BY table_schema, table_name ASC
)
