CREATE VIEW IF NOT EXISTS staging.senior_positions_grades AS
SELECT
	estrutura AS positionStructureCode,
	codigo AS idPosition,
	cargo AS positionTitle,
	grade AS positionGrade
FROM raw.senior_cargos_grades_estrutura_3
