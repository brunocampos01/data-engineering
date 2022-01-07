INSERT OVERWRITE TABLE general_ledger.fact_general_ledger
select
	CAST(date_format(m.mvto_lcto_data,'yyyyMMdd') as int)			as sk_date
	,md5(c.seg_centro_custo)
	,md5(concat(c.seg_unidade_negocio, c.seg_centro_custo, c.seg_projeto, c.seg_produto, c.seg_servico, c.seg_seg_mercado, c.seg_empresa, c.seg_intercia, c.seg_conta_contabil))
	,md5(d.key)
	,md5(m.mvto_moeda)
	,md5(d.parameter_value1)
	,null
	,cast(m.mvto_deb_ctb as double)
	,cast(m.mvto_cred_ctb as double)
	,cast(m.mvto_deb_ctb - m.mvto_cred_ctb as double)
  ,CURRENT_TIMESTAMP()
	,0
from raw.oracle_gl_movimento m
left join raw.oracle_gl_pl_contas c on c.code_combination_id = m.code_combination_id
left join raw.oracle_gl_periodo p on p.periodo_nome = m.period_name
left join raw.oracle_gl_drilldown d on d.key = m.mto_key and d.source = 'Recebimento Integrado'
left join staging.oracle_cost_center cc on cc.code = concat(SPLIT(c.seg_unidade_negocio_desc, ' - ')[0], '-', c.seg_centro_custo)
left join staging.oracle_products pr on pr.name = c.seg_produto and pr.idbusinessunit = c.seg_unidade_negocio
