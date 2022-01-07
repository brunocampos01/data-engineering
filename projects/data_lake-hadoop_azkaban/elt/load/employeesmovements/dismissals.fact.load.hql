INSERT OVERWRITE TABLE employeesmovements.fact_dismissals
SELECT
	dsoftplayers.id,
	dbs.id,
	denterprises.id,
	main.id,
	main.cpf,
	main.dataadmissaotempodeempresa,
	main.datadesligamento,
	main.causasdesligamento,
	main.iniciativadesligamento,
	main.cargosemnivel,
	main.nivelcargo,
	main.perfilcargo,
	main.cargocomnivel,
	main.tipocargo,
	main.categoriacargo,
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
LEFT JOIN staging.senior_setoresnegocio AS dun ON main.nomesetor = dun.nomesetor
WHERE
main.situacao = 'Demitido' AND
main.causasdesligamento <> 'Transferencia p/ Outra Empresa' AND
main.causasdesligamento <> 'Transferencia p/ Outra Filial' AND
main.causasdesligamento <> 'Encer. Estágio - Efetivação' AND
main.causasdesligamento <> 'Transferência por Sucessão';
