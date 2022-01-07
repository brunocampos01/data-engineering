CREATE VIEW staging.hyperion_board AS
select
	 d.id									as id
	,d.title							as name
	,u.codigo							as businessUnit
from raw.sharepoint_diretoria as d
JOIN raw.sharepoint_unidades as u ON d.unidadedenegf3id = u.id
order by d.id
