CREATE VIEW IF NOT EXISTS staging.senior_dismissals AS
SELECT
  CONCAT(
	    CAST(desligamentos.numemp AS  int), '.',
	    CAST(desligamentos.tipcol AS  int), '.',
	    REPLACE(
	      CAST(desligamentos.numcad AS  string),
	        '.0','')
  	)		            			        		AS id,
  CAST(desligamentos.numemp 	   	AS int)         AS idCompany,
  CAST(desligamentos.tipcol 	   	AS int)         AS idEmployeeType,
  REPLACE(
    CAST(desligamentos.numcad AS  string),
      '.0','')      								AS registerCode,
  CAST(desligamentos.datdem 	   	AS timestamp)   AS dismissalDate,
  CAST(desligamentos.caudem 	   	AS int)         AS idDismissalCauses,
  caudes.cause AS dismissalcauses,
  CASE
  	WHEN desligamentos.caudem = 1 THEN "Involuntário"
 	WHEN desligamentos.caudem = 2 THEN "Involuntário"
 	WHEN desligamentos.caudem = 3 THEN "Voluntário"
 	WHEN desligamentos.caudem = 4 THEN "Voluntário"
 	WHEN desligamentos.caudem = 5 THEN "Involuntário"
 	WHEN desligamentos.caudem = 6 THEN "Involuntário"
 	WHEN desligamentos.caudem = 7 THEN "Involuntário"
 	WHEN desligamentos.caudem = 8 THEN "Voluntário"
 	WHEN desligamentos.caudem = 9 THEN "Involuntário"
 	WHEN desligamentos.caudem = 10 THEN "Involuntário"
 	WHEN desligamentos.caudem = 11 THEN "Voluntário"
 	WHEN (desligamentos.caudem = 12 AND desligamentos.usu_fimctr = "E") THEN "Involuntário"
  	WHEN (desligamentos.caudem = 12 AND desligamentos.usu_fimctr = "C") THEN "Voluntário"
 	WHEN desligamentos.caudem = 13 THEN "Involuntário"
 	WHEN desligamentos.caudem = 14 THEN "Voluntário"
 	WHEN desligamentos.caudem = 15 THEN "Involuntário"
 	WHEN desligamentos.caudem = 16 THEN "Voluntário"
 	WHEN desligamentos.caudem = 17 THEN "Voluntário"
 	WHEN desligamentos.caudem = 18 THEN "Voluntário"
 	WHEN desligamentos.caudem = 19 THEN "Voluntário"
 	WHEN desligamentos.caudem = 20 THEN "Involuntário"
 	WHEN desligamentos.caudem = 21 THEN "Involuntário"
 	WHEN desligamentos.caudem = 22 THEN "Involuntário"
 	WHEN desligamentos.caudem = 23 THEN "Involuntário"
 	WHEN desligamentos.caudem = 24 THEN NULL
 	WHEN desligamentos.caudem = 25 THEN "Voluntário"
 	WHEN desligamentos.caudem = 26 THEN "Involuntário"
 	WHEN desligamentos.caudem = 27 THEN "Voluntário"
 	WHEN desligamentos.caudem = 28 THEN "Involuntário"
 	WHEN desligamentos.caudem = 29 THEN "Voluntário"
 	WHEN desligamentos.caudem = 30 THEN "Voluntário"
 	ELSE NULL
  END 													AS dismissalInitiative
FROM raw.senior_desligamentos AS desligamentos
	LEFT JOIN staging.senior_dismissalcauses AS caudes ON desligamentos.caudem = caudes.id
ORDER BY dismissalDate DESC;
---------------------------------------------------------------------------------------------------------------------------


CREATE VIEW IF NOT EXISTS staging.senior_desligamentos AS
SELECT
  CONCAT(
	    CAST(desligamentos.numemp AS  int), '.',
	    CAST(desligamentos.tipcol AS  int), '.',
	    REPLACE(
	      CAST(desligamentos.numcad AS  string),
	        '.0','')
  	)		            			        		AS id,
  colaboradores.nomeColaborador,
  CAST(desligamentos.numemp 	   	AS int)         AS idEmpresa,
  CAST(desligamentos.tipcol 	   	AS int)         AS idTipoColaborador,
  REPLACE(
    CAST(desligamentos.numcad AS  string),
      '.0','')      								AS idCadastroColaborador,
  CAST(desligamentos.datdem 	   	AS timestamp)   AS dataDesligamento,
  CAST(desligamentos.caudem 	   	AS int)         AS idCausasDesligamento,
  caudes.descricao AS causasdesligamento,
  CASE
  	WHEN desligamentos.usu_fimctr = "E" THEN "Involuntário"
  	WHEN desligamentos.usu_fimctr = "C" THEN "Voluntário"
  	ELSE NULL
  END												AS iniciativaDesligamentoFimContrato,
  CASE
  	WHEN caudes.id = 1 THEN "Involuntário"
 	WHEN caudes.id = 2 THEN "Involuntário"
 	WHEN caudes.id = 3 THEN "Voluntário"
 	WHEN caudes.id = 4 THEN "Voluntário"
 	WHEN caudes.id = 5 THEN "Involuntário"
 	WHEN caudes.id = 6 THEN "Involuntário"
 	WHEN caudes.id = 7 THEN "Involuntário"
 	WHEN caudes.id = 8 THEN "Voluntário"
 	WHEN caudes.id = 9 THEN "Involuntário"
 	WHEN caudes.id = 10 THEN "Involuntário"
 	WHEN caudes.id = 11 THEN "Voluntário"
 	WHEN caudes.id = 13 THEN "Involuntário"
 	WHEN caudes.id = 14 THEN "Voluntário"
 	WHEN caudes.id = 15 THEN "Involuntário"
 	WHEN caudes.id = 16 THEN "Voluntário"
 	WHEN caudes.id = 17 THEN "Voluntário"
 	WHEN caudes.id = 18 THEN "Voluntário"
 	WHEN caudes.id = 19 THEN "Voluntário"
 	WHEN caudes.id = 20 THEN "Involuntário"
 	WHEN caudes.id = 21 THEN "Involuntário"
 	WHEN caudes.id = 22 THEN "Involuntário"
 	WHEN caudes.id = 23 THEN "Involuntário"
 	WHEN caudes.id = 24 THEN "Involuntário"
 	WHEN caudes.id = 25 THEN "Voluntário"
 	WHEN caudes.id = 26 THEN "Involuntário"
 	WHEN caudes.id = 27 THEN "Voluntário"
 	WHEN caudes.id = 28 THEN "Involuntário"
 	WHEN caudes.id = 29 THEN "Voluntário"
 	WHEN caudes.id = 30 THEN "Voluntário"
 	ELSE NULL
  END 													AS iniciativaDesligamento,
  colaboradores.idTipoContrato,
  colaboradores.generoColaborador,
  colaboradores.idPCD,
  colaboradores.idGrauInstrucao,
  colaboradores.idSituacao,
  colaboradores.dataNascimento,
  colaboradores.idLocalTrabalho,
  colaboradores.idCargo,
  colaboradores.idCPF,
  colaboradores.idinternosenior,
  colaboradores.idsetornegocio,
  colaboradores.idEstruturaEquipe,
  colaboradores.idEquipe
FROM raw.senior_desligamentos AS desligamentos
LEFT JOIN staging.senior_colaboradores AS colaboradores
	ON  desligamentos.numemp = colaboradores.idempresa
	AND desligamentos.tipcol = colaboradores.idtipocolaborador
	AND desligamentos.numcad = colaboradores.idcadastrocolaborador
LEFT JOIN staging.senior_causasdesligamento AS caudes ON desligamentos.caudem = caudes.id
SORT BY dataDesligamento DESC;
