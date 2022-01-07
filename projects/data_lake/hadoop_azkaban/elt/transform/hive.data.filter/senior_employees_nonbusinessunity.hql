CREATE VIEW IF NOT EXISTS staging.senior_employees_nonbusinessunity AS
SELECT
	id,
	name,
	cpf,
	hiringdate,
	workunity,
	idworkunity
FROM raw.senior_colaboradores_semunidade_pre052014
