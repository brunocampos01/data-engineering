INSERT OVERWRITE TABLE account_receivables.dim_oracle_contract
select
	md5(nrcontrato),
	nrcontrato
from staging.oracle_contract as oc
