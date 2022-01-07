INSERT OVERWRITE TABLE employeesmovements.fact_employeechanges
SELECT
	dsoftplayers.id,
	dbs.id,
	denterprises.id,
	ddirectleader.id,
	dexecutiveboard.id,
	dboard.id,
	dmanagementlevel1.id,
	dteams.id,
	djobs.id,
	main.id,
	main.cpf,
	main.dataadmissaotempodeempresa,
	main.datadesligamento,
	djobs.updatedate,
	main.diasempresa,
	main.anosempresa,
	CASE
	  	WHEN main.anosempresa < 1 THEN "abaixo de 1 ano"
	  	WHEN main.anosempresa >= 1 AND main.anosempresa < 2 THEN "de 1 a ~2 anos"
	  	WHEN main.anosempresa >= 2 AND main.anosempresa < 5 THEN "de 2 a ~5 anos"
	  	WHEN main.anosempresa >= 5 AND main.anosempresa < 10 THEN "de 5 a ~10 anos"
	  	WHEN main.anosempresa > 10 THEN "mais que 10 anos"
	  	ELSE NULL
  	END,
	IF(DATEDIFF(CURRENT_DATE,main.dataadmissaotempodeempresa) < 90,
		"Incompleto",
		IF(main.diasempresa <= 90,
			"Não retido",
			"Retido")
		),
	main.idadeanos,
	CASE
	  	WHEN main.idadeanos < 18 THEN "menos de 18 anos"
	  	WHEN main.idadeanos >= 18 AND main.idadeanos < 26 THEN "de 18 a ~26 anos"
	  	WHEN main.idadeanos >= 26 AND main.idadeanos < 36 THEN "de 26 a ~36 anos"
	  	WHEN main.idadeanos >= 36 AND main.idadeanos < 45 THEN "de 36 a ~45 anos"
	  	WHEN main.idadeanos > 45 THEN "menos de 18 anos"
	  	ELSE NULL
  	END,
	main.nomesetor,
	main.siglaunidade
FROM staging.senior_movimentacao AS main
LEFT JOIN employeesmovements.dim_softplayers AS dsoftplayers ON main.id = dsoftplayers.nksoftplayercode
LEFT JOIN employeesmovements.dim_controllershipbusinessstructure AS dbs ON main.centrodecusto = dbs.nkbusinessstructure
LEFT JOIN employeesmovements.dim_enterprises AS denterprises ON main.empresa = denterprises.nkenterprisecode
LEFT JOIN employeesmovements.dim_xptodirectleaders AS ddirectleader ON main.idliderdireto = ddirectleader.nkleadercode
LEFT JOIN employeesmovements.dim_executiveboard AS dexecutiveboard ON main.iddiretoriaexecutiva = dexecutiveboard.nkexecutiveboardcode
LEFT JOIN employeesmovements.dim_board AS dboard ON main.iddiretoria = dboard.nkboardcode
LEFT JOIN employeesmovements.dim_managementlevel1 AS dmanagementlevel1 ON main.idgestaonivel1 = dmanagementlevel1.nkmanagementcode
LEFT JOIN employeesmovements.dim_xptoteams AS dteams ON main.estruturaequipe = dteams.teamstructurecode AND main.equipenome = dteams.teamcode
LEFT JOIN employeesmovements.dim_jobstitlesrecord AS djobs ON main.id = djobs.employeecode
LEFT JOIN staging.senior_setoresnegocio AS dun ON main.nomesetor = dun.nomesetor;
