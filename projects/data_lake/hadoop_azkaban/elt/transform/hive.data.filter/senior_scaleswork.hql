CREATE VIEW IF NOT EXISTS staging.senior_scaleswork AS
SELECT
  CAST(scaleswork.codesc 	   	AS int)         AS id,
  CAST(scaleswork.nomesc 	   	AS string)      AS description,
  (scaleswork.hordsr/60)       					AS paidtimeoff,
  (scaleswork.hormes/60)       					AS workhours
FROM raw.senior_escalatrabalho as scaleswork
ORDER BY id DESC;
