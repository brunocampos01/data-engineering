INSERT OVERWRITE TABLE headcount.fact_headcount
SELECT
	d.id,
	dbs.id,
	dv.id,
	dh.id,
	dp.id,
	h.vlorcado,
	h.vlrealizado,
	CURRENT_TIMESTAMP(),
	0
FROM staging.hyperion_rh AS h
JOIN default.dim_date d ON h.cddata = d.id
JOIN headcount.dim_business_structure dbs ON h.cdcentrocusto = dbs.nkbusinessstructure
JOIN headcount.dim_version dv ON h.cdversao = dv.name
JOIN headcount.dim_handle dh ON h.cdmovimento = dh.name
JOIN headcount.dim_position dp ON h.cdtipoprofissional = dp.name
