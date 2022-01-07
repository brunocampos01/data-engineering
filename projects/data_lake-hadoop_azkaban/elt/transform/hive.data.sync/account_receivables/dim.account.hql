INSERT OVERWRITE TABLE account_receivables.dim_account
select
	md5(a.id),
	a.id,
	a.shortname,
	a.package,
	a.groupname
from staging.oracle_accounts as a
