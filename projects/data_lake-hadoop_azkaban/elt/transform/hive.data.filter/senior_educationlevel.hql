CREATE VIEW IF NOT EXISTS staging.senior_educationlevel AS
SELECT
  CAST(grains 	   	AS int)         AS id,
  CAST(desgra 	   	AS string)      AS educationlevel
FROM raw.senior_instrucaocolaborador AS instrucaocolaborador
ORDER BY id DESC;
