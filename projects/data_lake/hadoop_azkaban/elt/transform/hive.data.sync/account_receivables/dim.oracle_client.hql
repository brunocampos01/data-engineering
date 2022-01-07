INSERT OVERWRITE TABLE account_receivables.dim_oracle_client
select
	md5(nomecliente),
	nomecliente
from staging.oracle_clients
