CREATE VIEW IF NOT EXISTS staging.senior_teamshierarchy AS
SELECT
  CAST(hierarquiaequipe.estpos 	   	AS int)           AS teamStructureCode,
  CAST(hierarquiaequipe.postra 	   	AS string)        AS teamCode,
  CAST(hierarquiaequipe.idepos 	   	AS int)           AS hierarchyCode,
  CAST(hierarquiaequipe.pospos 	   	AS string)        AS teamPositionHierarchy,
  CAST(equipetrabalho.despos 	   	AS string)          AS teamName,
  CAST(equipetrabalho.datcri 	   	AS timestamp)       AS dateCreation,
  SUBSTR(hierarquiaequipe.pospos,
         0, length(hierarquiaequipe.pospos)-2)        AS fatherTeamPositionHierarchy,
  CAST(equipePai.postra 	   	AS string)              AS fatherTeamCode
FROM raw.senior_hierarquiaequipe AS hierarquiaequipe
LEFT JOIN raw.senior_equipetrabalho AS equipetrabalho ON hierarquiaequipe.estpos = equipetrabalho.estpos AND hierarquiaequipe.postra = equipetrabalho.postra
LEFT JOIN raw.senior_hierarquiaequipe AS equipePai ON hierarquiaequipe.estpos = equipePai.estpos AND (SUBSTR(hierarquiaequipe.pospos,0, length(hierarquiaequipe.pospos)-2)) = equipePai.pospos
WHERE hierarquiaequipe.CodThp = 1
AND  hierarquiaequipe.RevHie = 1;

------------------------------------------------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS staging.senior_hierarquiaequipe AS
SELECT
  CAST(hierarquiaequipe.estpos 	   	AS int)           AS idEstruturaEquipe,
  CAST(hierarquiaequipe.postra 	   	AS string)        AS idEquipe,
  CAST(hierarquiaequipe.idepos 	   	AS int)           AS idHierarquia,
  CAST(hierarquiaequipe.pospos 	   	AS string)        AS idposicaoEquipe,
  CAST(equipetrabalho.despos 	   	AS string)          AS nomeEquipe,
  CAST(equipetrabalho.datcri 	   	AS timestamp)       AS dataCriacaoEquipe,
  SUBSTR(hierarquiaequipe.pospos,
         0, length(hierarquiaequipe.pospos)-2)        AS idPosicaoEquipePai,
  CAST(equipePai.postra 	   	AS string)              AS idEquipePai
FROM raw.senior_hierarquiaequipe AS hierarquiaequipe
LEFT JOIN raw.senior_equipetrabalho AS equipetrabalho ON hierarquiaequipe.estpos = equipetrabalho.estpos AND hierarquiaequipe.postra = equipetrabalho.postra
LEFT JOIN raw.senior_hierarquiaequipe AS equipePai ON hierarquiaequipe.estpos = equipePai.estpos AND (SUBSTR(hierarquiaequipe.pospos,0, length(hierarquiaequipe.pospos)-2)) = equipePai.pospos
WHERE hierarquiaequipe.CodThp = 1
AND  hierarquiaequipe.RevHie = 1;
