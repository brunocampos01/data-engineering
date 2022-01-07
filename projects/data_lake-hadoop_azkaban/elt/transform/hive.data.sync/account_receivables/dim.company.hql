INSERT OVERWRITE TABLE account_receivables.dim_company
select
	md5(c.cdempresa),
	c.cdempresa,
	c.nmempresa,
	c.decnpj,
	c.degrupo
from staging.hyperion_company c
