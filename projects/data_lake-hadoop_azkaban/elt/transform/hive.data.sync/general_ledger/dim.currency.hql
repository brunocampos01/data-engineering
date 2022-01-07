INSERT OVERWRITE TABLE general_ledger.dim_currency
select
	md5(hc.id),
	hc.id,
	hc.name,
	hc.category
from staging.hyperion_currency hc
