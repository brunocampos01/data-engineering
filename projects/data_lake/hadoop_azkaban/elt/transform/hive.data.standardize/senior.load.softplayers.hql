CREATE VIEW test.senior_softplayers AS
SELECT
  md5(concat(row_sequence(),
        colaboradores.id))		    	AS skSoftplayerCode,
  colaboradores.id						AS nkSoftplayerCode,
  colaboradores.cpf						AS cpf,
  colaboradores.nomeColaborador			AS name,
  colaboradores.registro				AS employeeType,
  colaboradores.tipocontrato			AS contractType,
  colaboradores.idCadastroColaborador   AS seniorCode,
  colaboradores.generoColaborador		AS gender,
  colaboradores.pcd 					AS pcd,
  colaboradores.instrucao 				AS education,
  colaboradores.situacao 				AS workSituation,
  colaboradores.datanascimento			AS birthdayDate,
  colaboradores.localtrabalho 			AS workplace,
  colaboradores.cidade					AS city,
  colaboradores.estado					AS state
FROM staging.senior_movimentacao AS colaboradores;
