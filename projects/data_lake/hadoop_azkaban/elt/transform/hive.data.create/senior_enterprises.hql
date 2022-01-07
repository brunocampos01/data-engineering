CREATE VIEW IF NOT EXISTS staging.senior_enterprises AS
SELECT
  CAST(empresa.numemp 	   	AS int)    AS id,
  CASE
    WHEN empresa.numemp = 1 THEN "BR02"
    WHEN empresa.numemp = 2 THEN "BR01"
    WHEN empresa.numemp = 3 THEN "BR03"
    WHEN empresa.numemp = 4 THEN "BR04"
    WHEN empresa.numemp = 5 THEN "BR05"
    WHEN empresa.numemp = 6 THEN "BR06"
    WHEN empresa.numemp = 7 THEN "BR07"
    WHEN empresa.numemp = 9 THEN "BR76"
    ELSE NULL
  END                             AS controllershipCode,
  CAST(empresa.nomemp 	   	AS string)    AS name,
  CASE
    WHEN empresa.nomemp = "xpto_company PARTICIPAÇÕES LTDA" THEN "xpto_company PARTICIPACOES"
    ELSE CAST(empresa.apeemp 	   	AS string)
  END                             AS shortName
FROM raw.senior_empresas AS empresa
ORDER BY id ASC;
