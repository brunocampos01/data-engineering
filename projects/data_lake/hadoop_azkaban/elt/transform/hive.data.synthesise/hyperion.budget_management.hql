CREATE MATERIALIZED VIEW staging.budget_management as
select
	f.cddata
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idbusinessunit,cc.idbusinessunit,f.cdunidadenegocio)
		else 	coalesce(cc.idbusinessunit,p.idbusinessunit,f.cdunidadenegocio)
	 end 																		as cdUnidadeNegocio
	,cc.idboard																	as cdDiretoria
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idvertical,cc.idvertical)
		else 	coalesce(cc.idvertical,p.idvertical)
	 end																		as cdVertical
	 ,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idresultcenter,cc.idresultscenter)
		else 	coalesce(cc.idresultscenter,p.idresultcenter)
	 end																		as cdCentroResultado
	,cc.idmanagement															as cdGerencia
	,f.cdCentroCusto															as cdCentroCusto
	,f.cdProjeto																as cdProjeto
	,f.cdProduto																as cdProduto
	,f.cdServico																as cdServico
	,f.cdSegmento																as cdSegmento
	,f.cdMoeda																	as cdMoeda
	,f.cdVersao																	as deHyperionVersao
	,f.cdEmpresa																as cdEmpresa
	,f.cdInterCia								 								as cdInterCia
	,f.cdAlocacao								 								as cdAlocacao
	,f.cdConta																	as cdConta
	,cast(null as string)														as deHyperionObservacao
	,cast(sum(f.vlRealizado) as decimal(16,2))									as vlRealizado
	,cast(sum(f.vlOrcado) as decimal(16,2))										as vlOrcado
	,cast(sum(f.vlForecast) as decimal(16,2))									as vlForecast
from staging.hyperion_financas f
left join staging.oracle_cost_center cc on cc.code = f.cdCentroCusto
left join staging.oracle_products p on p.code = f.cdProduto
group by
	 f.cddata
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idbusinessunit,cc.idbusinessunit,f.cdunidadenegocio)
		else 	coalesce(cc.idbusinessunit,p.idbusinessunit,f.cdunidadenegocio)
	 end
	,cc.idboard
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idvertical,cc.idvertical)
		else 	coalesce(cc.idvertical,p.idvertical)
	 end
	 ,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idresultcenter,cc.idresultscenter)
		else 	coalesce(cc.idresultscenter,p.idresultcenter)
	 end
	,cc.idmanagement
	,f.cdCentroCusto
	,f.cdProjeto
	,f.cdProduto
	,f.cdServico
	,f.cdSegmento
	,f.cdMoeda
	,f.cdVersao
	,f.cdEmpresa
	,f.cdInterCia
	,f.cdAlocacao
	,f.cdConta
UNION ALL
select
	 CAST(date_format(f.data,'yyyyMMdd') as int) as skData
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idbusinessunit,cc.idbusinessunit,f.cdunidadenegocio)
			else coalesce(cc.idbusinessunit,p.idbusinessunit,f.cdunidadenegocio)
	 end 																		as cdUnidadeNegocio
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idboard,cc.idboard)
			else coalesce(cc.idboard,p.idboard)
	 end																		as cdDiretoria
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idvertical,cc.idvertical)
			else coalesce(cc.idvertical,p.idvertical)
	 end																		as cdVertical
	 	 	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idresultcenter,cc.idresultscenter)
			else coalesce(cc.idresultscenter,p.idresultcenter)
	 end																		as cdCentroResultado
	,cc.idmanagement 																as cdGerencia
	,f.cdnegocio 																as cdCentroCusto
	,f.cdprojetooracle 															as cdProjeto
	,f.cdproduto
	,f.cdservico
	,f.cdsegmento
	,f.cdmoeda
	,'Trabalho' 																as deHyperionVersao
	,f.cdempresa
	,f.cdintercia
	,f.cdalocacao
	,f.cdconta
	,cast('#AjustesManuais: ' + f.deobservacao as  string)						as deHyperionObservacao
	,sum(f.vlrealizado)															as vlRealizado
	,sum(f.vlorcado)															as vlOrcado
	,0.0																		as vlForecast
from raw.sharepoint_ajustes_manuais f
left join staging.oracle_cost_center cc on cc.code = f.cdnegocio
left join staging.oracle_products p on p.code = f.cdproduto
where NULLIF(trim(f.data), '') is not null
group by
	 CAST(date_format(f.data,'yyyyMMdd') as int)
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idbusinessunit,cc.idbusinessunit,f.cdunidadenegocio)
			else coalesce(cc.idbusinessunit,p.idbusinessunit,f.cdunidadenegocio)
	 end
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idboard,cc.idboard)
			else coalesce(cc.idboard,p.idboard)
	 end
	,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idvertical,cc.idvertical)
			else coalesce(cc.idvertical,p.idvertical)
	 end
	 ,case
		when (f.cdConta like '3%' or f.cdConta like '7%' or f.cdConta like '6%' or f.cdConta like '41%')
			then coalesce(p.idresultcenter,cc.idresultscenter)
			else coalesce(cc.idresultscenter,p.idresultcenter)
	 end
	,cc.idmanagement
	,f.cdnegocio
	,f.cdprojetooracle
	,f.cdproduto
	,f.cdservico
	,f.cdsegmento
	,f.cdmoeda
	,f.cdempresa
	,f.cdintercia
	,f.cdalocacao
	,f.cdconta
	,f.deobservacao
UNION ALL
select
	 h.cddate
	,cc.idbusinessunit								as cdUnidadeNegocio
	,cc.idboard 									as cdDiretoria
	,cc.idvertical 									as cdVertical
	,cc.idresultscenter								as cdCentroResultado
	,cc.idmanagement 								as cdGerencia
	,h.cdcostcenter									as cdCentroCusto
	,cast(null as string)							as cdProjeto
	,cast(null as string)							as cdProduto
	,cast(null as string)							as cdServico
	,cast(null as string)							as cdSegmento
	,cast(null as string)					as cdMoeda
	,h.cdversion									as deHyperionVersao
	,cast(null as string)							as cdEmpresa
	,cast(null as string)				 			as cdInterCia
	,'NA_Alocacao'								 	as cdAlocacao
	,h.cdaccount									as cdConta
	,h.deobs										as deHyperionObservacao
	,cast(sum(h.vlaccomplished) as double)			as vlRealizado
	,cast(sum(h.vlbudgeted) as double)				as vlOrcado
	,0.0											as vlForecast
from staging.hyperion_apportionment h
left join staging.oracle_cost_center cc on cc.code = h.cdcostcenter
group by
	 h.cddate
	,cc.idbusinessunit
	,cc.idboard
	,cc.idvertical
	,cc.idresultscenter
	,cc.idmanagement
	,h.cdcostcenter
	,h.cdaccount
	,h.deobs
	,h.cdversion
