CREATE VIEW staging.hyperion_position AS
SELECT
	DISTINCT UD09 AS name
FROM raw.hyperion_rh
WHERE UD09 IN ('Celetista', 'Jovem Aprendiz', 'Estagiario', 'Diretor', 'Terceiros')
