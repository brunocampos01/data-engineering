CREATE VIEW staging.hyperion_management AS
select
	 g.id		  							as id
	,g.title 								as name
	,u.codigo 							as businessUnitId
	,cr.verticalnegocioid		as verticalId
	,cr.diretoriaid	  			as boardId
from raw.sharepoint_gerencias as g
left join raw.sharepoint_centros_de_resultado cr on cr.id = g.centroderesultadoid
left join raw.sharepoint_unidades u on u.id = cr.unidadeid
