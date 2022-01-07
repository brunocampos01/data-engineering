CREATE VIEW IF NOT EXISTS staging.senior_costcenter AS
SELECT
  CASE
  	WHEN ((LTRIM(ccu.ccutxt) = '') OR (SUBSTRING(ccu.ccutxt,1,1) = '1') OR (ccu.ccutxt IS NULL)) THEN CAST(ccu.codccu AS string)
  	ELSE CONCAT_WS('-',area.businessunityinitial,UPPER(ccu.ccutxt))
  END											AS id,
  ccu.numemp									AS identerprise,
  enterprise.shortname        					AS enterpriseshortname,
  CAST(ccu.codccu       AS  string)     		AS idCcuSenior,
  CAST(ccu.ccutxt       AS  string)     		AS ccunonbusinesscode,
  CAST(ccu.nomccu       AS  string)     		AS ccuname,
  CAST(ccu.datcri       AS timestamp)   		AS creationDate,
  area.businessunityinitial,
  area.areaname,
  area.controllershipcode
FROM raw.senior_centrosdecusto as ccu
LEFT JOIN staging.senior_companyareas AS area ON ccu.usu_unidade = area.id
LEFT JOIN staging.senior_enterprises AS enterprise ON ccu.numemp = enterprise.id
ORDER BY creationDate DESC;
