CREATE VIEW staging.oracle_products AS
select
	 u.codigo 								as idBusinessUnit
	,cr.verticalnegocioid			as idVertical
	,cr.diretoriaid						as idBoard
	,cr.id										as idresultcenter
	,p.u7r1										as code
	,p.id											as id
	,replace(p.u7r1, 'P','')	as name
	,p.title									as description
	,p.stwf										as classification
	,p.status									as status
	,p.historico							as history
from raw.sharepoint_produtos as p
left join raw.sharepoint_unidades u on u.id = p.unidadeid
left join raw.sharepoint_centros_de_resultado cr on cr.id = p.centroresultadoid
