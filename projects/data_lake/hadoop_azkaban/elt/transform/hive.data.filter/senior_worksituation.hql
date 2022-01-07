CREATE VIEW IF NOT EXISTS staging.senior_worksituation AS
SELECT
  CAST(codsit 	   	AS int)         AS id,
  CAST(dessit 	   	AS string)      AS worksituation
FROM raw.senior_situacaocolaborador AS situacaocolaborador
WHERE codsit < 900
ORDER BY id DESC;
