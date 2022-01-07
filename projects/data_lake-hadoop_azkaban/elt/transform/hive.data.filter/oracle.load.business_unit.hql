CREATE VIEW staging.oracle_business_unit AS
select
	codigo	as id,
	sigla		as initials,
	title		as fullName
from raw.sharepoint_unidades
where codigo like 'BR%'
order by codigo
