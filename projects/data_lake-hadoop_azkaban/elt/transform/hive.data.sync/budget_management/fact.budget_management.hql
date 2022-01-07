INSERT OVERWRITE TABLE budget_management.fact_budget_management
SELECT
	bm.cddata,
	md5(bm.cdcentrocusto),
	md5(concat(bm.cdunidadenegocio, bm.cdcentrocusto, bm.cdprojeto, bm.cdproduto, bm.cdservico, bm.cdsegmento, bm.cdempresa, bm.cdintercia, bm.cdconta)),
	md5(bm.cdconta),
	md5(bm.dehyperionversao),
	md5(bm.dehyperionobservacao),
	bm.vlorcado,
	bm.vlrealizado,
	bm.vlforecast,
	CURRENT_TIMESTAMP(),
	0
FROM staging.budget_management AS bm
