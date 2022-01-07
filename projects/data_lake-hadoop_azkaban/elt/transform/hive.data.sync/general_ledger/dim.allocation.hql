INSERT OVERWRITE TABLE general_ledger.dim_allocation
select
	md5(ha.cdalocacao),
	ha.cdalocacao,
	ha.dealocacaonivel1,
	ha.dealocacaonivel2,
	ha.dealocacaonivel3
from staging.hyperion_allocation ha
