CREATE VIEW IF NOT EXISTS test.senior_xptoteams AS
SELECT
  md5(concat(row_sequence(),
  		idEstruturaEquipe,
  		idEquipe))		              		AS skxptoTeams,
  concat(idEstruturaEquipe,idEquipe)	    AS nkxptoTeams,
  idEstruturaEquipe 						AS teamStructureCode,
  idEquipe 									AS teamCode,
  idHierarquia								AS hierarchyCode,
  idposicaoEquipe							AS teamPositionHierarchy,
  nomeEquipe								AS teamName,
  idPosicaoEquipePai						AS fatherTeamPositionHierarchy,
  idEquipePai								AS fatherTeamCode
FROM staging.senior_hierarquiaequipe;
