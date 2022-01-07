CREATE VIEW staging.oracle_accounts AS
select
	 	replace(cast(c.r7tf as string),'.0','')															as id
		,replace(CONCAT(cast(c.r7tf as string), ' - ', c.title),'.0','')	 	as name
		,c.title																														as shortName
		,c.descric_e3_o																											as description
		,c.bspg																															as package
		,c.cifb																															as moneySource
		,c.grupo																														as groupName
		,c.ff																																as subGroupName
		,c.status																														as status
		,c.comoorc_ar																												as howToBudget
		,c.tipoderateio																											as apportionmentType
		,c.ryhb																															as dynamicClass
		,c.an2r																															as dynamicClass2
	from raw.sharepoint_plano_de_contas c
	where c.r7tf is not null
