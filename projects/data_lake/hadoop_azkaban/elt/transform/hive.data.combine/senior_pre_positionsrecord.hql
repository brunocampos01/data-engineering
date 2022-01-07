CREATE VIEW IF NOT EXISTS staging.senior_pre_positionsrecord AS
SELECT
  CONCAT(
    CAST(hp.numemp AS  int), '.',
    CAST(hp.tipcol AS  int), '.',
    REPLACE(
      CAST(hp.numcad AS  string),
        '.0','')
      )		            			        AS idEmployeeSenior,
  CAST(hp.datalt 	   	AS date)   AS positionUpdateDate,
  CAST(hp.estcar 	   	AS int)         AS positionStructurecode,
  CAST(hp.codcar 	   	AS string)         AS idposition,
  ps.positiontitle,
  ps.positionseniority,
  ps.fullpositiontitle,
  ps.positiontitleprofile,
  ps.positiontype,
  ps.positioncategory,
  ps.positiongrade
FROM raw.senior_historicocargos AS hp
	LEFT JOIN staging.senior_positionsstructure AS ps ON hp.estcar = ps.positionstructurecode AND hp.codcar = ps.id
ORDER BY positionUpdateDate DESC;
