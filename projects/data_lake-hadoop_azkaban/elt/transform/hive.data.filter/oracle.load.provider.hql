CREATE VIEW staging.oracle_provider AS
select
	distinct d.parameter_value1	as id
	,d.parameter_value1			as name
from raw.oracle_gl_drilldown d
where d.parameter_name1 = 'Fornecedor'
