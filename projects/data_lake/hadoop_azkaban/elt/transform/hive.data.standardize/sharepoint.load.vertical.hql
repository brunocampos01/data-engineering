CREATE VIEW staging.hyperion_vertical AS
select
	 v.id	 										as id
	,v.odata_67_wh6						as initials
	,v.title									as name
	,v.vertical2f_transversal	as type
	,u.codigo									as businessUnit
from raw.sharepoint_verticais as v
JOIN raw.sharepoint_unidades as u ON v.unidadeid = u.id
