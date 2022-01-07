INSERT OVERWRITE TABLE account_receivables.dim_business_unit
SELECT
	md5(bu.id),
	bu.id,
	bu.initials,
	bu.fullname
FROM staging.oracle_business_unit as bu
