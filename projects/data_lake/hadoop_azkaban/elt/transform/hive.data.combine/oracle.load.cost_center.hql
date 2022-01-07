CREATE VIEW staging.oracle_cost_center AS
with centrosDeCustoHyp as (
	select
		distinct t3.member as code,
		t3.alias as name
	from raw.hyperion_metadados as t1
	left join raw.hyperion_metadados t2 on t2.dimension = t1.dimension and t2.app = t1.app and t2.parent = t1.member
	left join raw.hyperion_metadados t3 on t3.dimension = t2.dimension and t3.app = t2.app and t3.parent = t2.member
	where t1.dimension = 'NEGOCIO'
	and t1.member = 'Total_Negocio'
),
centrosDeCustoSharepoint as (
select DISTINCT
	u.codigo 													                     as idBusinessUnit
	,cr.verticalnegocioid										               as idVertical
	,cc.diretoriaid												                 as idBoard
	,cr.id														                     as idResultsCenter
	,cc.gerenciaid												                 as idManagement
	,cc.id														                     as id
	,CONCAT(u.sigla, '-', cc.codigo)							         as code
	,CONCAT(cc.title, ' (', u.sigla, '-', cc.codigo, ')')	 as name
	,cc.title													                     as shortName
	,cc.codigo													                   as internalCode
	,cc.tipocc													                   as type
	,cc.status													                   as status
	,cc.b6ss													                     as apportionmentGroup
	,cc.mmlr													                     as classification
	,cc.OData_62_m22																as wagebillclassification
	,ug.title													                     as manager
	,ug.email													                     as managerEmail
	,ur.title													                     as responsible
	,ur.email													                     as responsibleEmail
from raw.sharepoint_centros_de_custo as cc
left join raw.sharepoint_unidades as u on u.id = cc.unid
left join raw.sharepoint_centros_de_resultado as cr on cr.id = cc.cr_centroid
left join raw.sharepoint_users ug on ug.id = cc.gestorid and ug.email like '%xpto_company%'
left join raw.sharepoint_users ur on ur.id = cc.responsavelorcid and ur.email like '%xpto%'
)
select DISTINCT
	 s.idBusinessUnit 				as idBusinessUnit
	,s.idVertical							as idVertical
	,s.idBoard								as idBoard
	,s.idResultsCenter				as idResultsCenter
	,s.idManagement						as idManagement
	,s.id											as id
	,s.code										as code
	,COALESCE(h.name,s.name) 	as name
	,s.shortName							as shortName
	,s.internalCode						as internalCode
	,s.type										as type
	,s.status									as status
	,s.apportionmentGroup			as apportionmentGroup
	,s.classification					as classification
	,s.wagebillclassification        as wagebillclassification
	,UCASE(s.manager)								as manager
	,LCASE(s.managerEmail)						as managerEmail
	,UCASE(s.responsible)						as responsible
	,LCASE(s.responsibleEmail)			as responsibleEmail
from centrosDeCustoSharepoint s
left join centrosDeCustoHyp h on s.code = h.code;
