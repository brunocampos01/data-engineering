CREATE VIEW staging.hyperion_handle AS
SELECT
	DISTINCT UD10 AS name
FROM raw.hyperion_rh 
