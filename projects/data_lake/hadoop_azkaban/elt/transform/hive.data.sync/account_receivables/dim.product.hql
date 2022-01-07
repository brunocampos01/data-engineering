INSERT OVERWRITE TABLE account_receivables.dim_product
SELECT
	md5(concat(p.name,p.businessunitinitials)),
	p.code,
	p.name,
	p.description,
	p.classification,
	p.status,
	p.resultcentername,
	p.businessunitname,
	p.businessunitinitials,
	p.verticalname,
	p.boardname
FROM staging.oracle_products p
