CREATE VIEW staging.oracle_purchase_order AS
select
	distinct d.key			as id
	,d.parameter_value2		as invoiceId
	,d.parameter_value8		as itemName
	,d.parameter_value6		as info
from raw.oracle_gl_drilldown d
where d.key is not null
