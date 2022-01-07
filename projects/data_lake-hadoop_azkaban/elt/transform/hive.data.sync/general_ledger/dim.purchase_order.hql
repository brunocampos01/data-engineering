INSERT OVERWRITE TABLE general_ledger.dim_purchase_order
select
	md5(po.id),
	po.id,
	po.invoiceid,
	po.itemname,
	po.info
from staging.oracle_purchase_order po
