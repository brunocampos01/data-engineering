INSERT OVERWRITE TABLE account_receivables.dim_intercia
select
	md5(i.cdintercia),
	i.cdintercia,
	i.nmintercia
from staging.hyperion_intercia as i
