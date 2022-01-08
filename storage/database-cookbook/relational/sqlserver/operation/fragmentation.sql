SELECT
	object_name(a.object_id) as TableName,
    a.index_id,
	name as IndedxName,
	avg_fragmentation_in_percent
FROM sys.dm_db_index_physical_stats
    (DB_ID (N''),
    OBJECT_ID(N'dbo.DWH'),
    NULL,
    NULL,
    NULL) AS a
INNER JOIN sys.indexes AS b
    ON a.object_id = b.object_id
    AND a.index_id = b.index_id;
ORDER BY 5 DESC;

GO
