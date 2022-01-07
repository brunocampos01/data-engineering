CREATE VIEW staging.hyperion_company AS
select
	 e.odata_6e_l99	as cdEmpresa
	,regexp_replace(e.odata_6e_l99, "BR", "E")	as cdEmpresaHyp
	,e.title		as nmEmpresa
	,e.cnpj			as deCNPJ
	,e.mjfo			as deGrupo
from raw.sharepoint_empresas as e
where e.odata_6e_l99 is not null
order by 1
