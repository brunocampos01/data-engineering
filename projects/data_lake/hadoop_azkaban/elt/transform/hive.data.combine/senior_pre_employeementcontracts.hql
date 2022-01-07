CREATE VIEW IF NOT EXISTS staging.senior_pre_employeementcontracts AS
SELECT
  CONCAT(
  	CAST(colaboradores.numemp AS  int), '.',
  	CAST(colaboradores.tipcol AS  int), '.',
  	REPLACE(
  		CAST(colaboradores.numcad AS  string),
  			'.0','')
  		)		            			             AS id,
  CAST(colaboradores.nomfun          AS  string)     AS name,
  CAST(colaboradores.numemp          AS  int)		 AS idEnterprise,
  CAST(colaboradores.tipcol          AS  int)        AS idEmployeeType,
  REPLACE(
  		CAST(colaboradores.numcad AS  string),
  			'.0','')		                   		 AS registerCode,
  CAST(colaboradores.cateso          AS  int)         AS idSocialCategory,
  CAST(colaboradores.tipcon          AS  int)         AS idContractType,
  CAST(colaboradores.tipsex          AS  string)      AS gender,
  CAST(colaboradores.deffis          AS  string)      AS pcd,
  CAST(colaboradores.benrea          AS  string)      AS rehabilitated,
  CAST(colaboradores.grains          AS  string)      AS idEducationLevel,
  CAST(colaboradores.datadm          AS  date)   AS lastHiringDate,
  CASE
    WHEN (colaboradores.usu_datemp IS NULL)         THEN  CAST(colaboradores.datadm AS date)
    WHEN (colaboradores.usu_datemp = "1900-12-31 00:00:00.0")  THEN  CAST(colaboradores.datadm AS date)
    ELSE CAST(colaboradores.usu_datemp  AS  date)
  END                                               AS hiringDate,
  CAST(colaboradores.datafa          AS  date)      AS workLeaveDate,
  CAST(colaboradores.sitafa          AS  int)       AS idWorkSituation,
  IF(colaboradores.sitafa = 7, CAST(colaboradores.datafa AS  date), NULL)   AS dismissaldate,
  CAST(colaboradores.datnas          AS  date)   AS birthdayDate,
  CAST(colaboradores.usu_postra      AS  int)         AS idWorkplace,
  CASE
  	WHEN (colaboradores.sitafa = 7 AND colaboradores.datafa < "2012-07-01 00:00:00.0")  THEN 1
  	WHEN (colaboradores.sitafa = 7 AND (colaboradores.datafa >= "2012-07-01 00:00:00.0" AND colaboradores.datafa <= "2019-12-31 00:00:00.0")) THEN 2
  	WHEN (colaboradores.sitafa <> 7) THEN 3
  ELSE 3
  END                                           AS positionsStructurecode,
  CAST(colaboradores.codcar          AS  string)      AS idposition,
  TRIM(LPAD(CAST(CAST(colaboradores.numcpf AS decimal(11)) AS string),11,'0')) 	AS cpf,
  ccu.codccu                                AS idCcuSenior,
  ccu.usu_unidade 							AS idCompanyArea,
  CAST(colaboradores.estpos          AS  int)         AS teamStructureCode,
  equipe.hierarchyCode								  AS hierarchyCode,
  CAST(colaboradores.postra          AS  string)      AS teamCode,
  equipe.fatherTeamCode 				 AS directLeaderTeamCode,
  CASE
  	WHEN equipe.fatherTeamCode = 'CONSELHO' THEN NULL
  	WHEN diretoriaexecutiva.teamCode = colaboradores.postra THEN NULL
  	ELSE diretoriaexecutiva.teamCode
  END 												AS executiveBoardTeamCode,
  CASE
  	WHEN equipe.fatherTeamCode = 'CONSELHO' THEN NULL
  	WHEN diretoria.teamCode = colaboradores.postra THEN NULL
  	ELSE diretoria.teamCode
  END 												AS boardTeamCode,
  CASE
  	WHEN equipe.fatherTeamCode = 'CONSELHO' THEN NULL
  	WHEN gestaonivel1.teamCode = colaboradores.postra THEN NULL
  	ELSE gestaonivel1.teamCode
  END												AS managementLevel1TeamCode
FROM raw.senior_colaboradores AS colaboradores
LEFT JOIN raw.senior_centrosdecusto AS ccu ON colaboradores.codccu = ccu.codccu AND colaboradores.numemp = ccu.numemp
LEFT JOIN staging.senior_teamshierarchy AS equipe ON colaboradores.estpos = equipe.teamStructureCode AND colaboradores.postra = equipe.teamCode
LEFT JOIN staging.senior_teamshierarchy AS diretoriaexecutiva ON colaboradores.estpos = diretoriaexecutiva.teamStructureCode AND SUBSTR(equipe.teamPositionHierarchy, 0, 4) = diretoriaexecutiva.teamPositionHierarchy
LEFT JOIN staging.senior_teamshierarchy AS diretoria ON colaboradores.estpos = diretoria.teamStructureCode AND SUBSTR(equipe.teamPositionHierarchy, 0, 6) = diretoria.teamPositionHierarchy
LEFT JOIN staging.senior_teamshierarchy AS gestaonivel1 ON colaboradores.estpos = gestaonivel1.teamStructureCode AND SUBSTR(equipe.teamPositionHierarchy, 0, 8) = gestaonivel1.teamPositionHierarchy
LEFT JOIN staging.senior_teamshierarchy AS gestaonivel2 ON colaboradores.estpos = gestaonivel2.teamStructureCode AND SUBSTR(equipe.teamPositionHierarchy, 0, 10) = gestaonivel2.teamPositionHierarchy
WHERE colaboradores.CATESO <> 701
	AND colaboradores.codcar <> 9999
	AND colaboradores.numemp IN(1,2,3,6,7)
ORDER BY hiringDate DESC;
