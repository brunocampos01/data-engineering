CREATE VIEW staging.hyperion_version AS
SELECT
	DISTINCT ud08 AS name
FROM raw.hyperion_financas AS hf
