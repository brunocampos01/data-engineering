INSERT OVERWRITE TABLE general_ledger.dim_provider
select
	md5(op.id),
	op.name
from staging.oracle_provider op
