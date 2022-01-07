CREATE VIEW staging.hyperion_service AS
with
servicosHyp as (
	SELECT
	COALESCE(T7.MEMBER, T6.MEMBER, T5.MEMBER, T4.MEMBER, T3.MEMBER, T2.MEMBER, T1.MEMBER) 	AS cdServico
	,LTRIM(RTRIM(REPLACE(T8.ALIAS,CONCAT('(', COALESCE(T7.MEMBER, T6.MEMBER, T5.MEMBER, T4.MEMBER, T3.MEMBER, T2.MEMBER, T1.MEMBER), ')'),''))) AS nmServico
	,LTRIM(RTRIM(T8.ALIAS))																						AS NMSERVICOCOMPLETO
	,CAST(null as string)																										AS NMTIPO
	,T8.dimension as dimension
	FROM raw.hyperion_metadados T1
	LEFT JOIN raw.hyperion_metadados T2 ON T2.DIMENSION = T1.DIMENSION AND T2.APP = T1.APP AND T2.PARENT = T1.MEMBER
	LEFT JOIN raw.hyperion_metadados T3 ON T3.DIMENSION = T2.DIMENSION AND T3.APP = T2.APP AND T3.PARENT = T2.MEMBER
	LEFT JOIN raw.hyperion_metadados T4 ON T4.DIMENSION = T3.DIMENSION AND T4.APP = T3.APP AND T4.PARENT = T3.MEMBER
	LEFT JOIN raw.hyperion_metadados T5 ON T5.DIMENSION = T4.DIMENSION AND T5.APP = T4.APP AND T5.PARENT = T4.MEMBER
	LEFT JOIN raw.hyperion_metadados T6 ON T6.DIMENSION = T5.DIMENSION AND T6.APP = T5.APP AND T6.PARENT = T5.MEMBER
	LEFT JOIN raw.hyperion_metadados T7 ON T7.DIMENSION = T6.DIMENSION AND T7.APP = T6.APP AND T7.PARENT = T6.MEMBER
	LEFT JOIN raw.hyperion_metadados T8 ON T8.MEMBER = COALESCE(T7.MEMBER, T6.MEMBER, T5.MEMBER, T4.MEMBER, T3.MEMBER, T2.MEMBER, T1.MEMBER)
	WHERE
		T1.DIMENSION = 'SERVICO'
	AND T1.MEMBER	 = 'Total_Servico'
	AND T1.APP		 = 'FINANCAS'
	AND T1.PARENT	 = 'SERVICO'
	AND	T8.PARENT 	 = 'Total_Servico'
),
servicosSharepoint as (
	select CONCAT('S', SUBSTR(CONCAT('0000', cast(s.odata_68_149 as int)), -3, 3)) 	as cdServico
 	,s.title 																	as nmServico
 	,s.Descric_e3_odo_x												as deServico
	,s.classe																	as deClasse
	,s.tipofaturamento												as deTipoFaturamento
	,s.recorrencia														as deRecorrencia
	,s.status																	as deStatus
	,s.CNAE																		as nuCNAE
	,u.sigla																	as unidadeNegocio
from raw.sharepoint_servicos as s
join raw.sharepoint_unidades as u on s.unidadeid = u.id
)
select
	 h.cdServico
	,h.nmServico
	,h.dimension
	,s.deServico
	,s.deClasse
	,s.deTipoFaturamento
	,s.deRecorrencia
	,s.deStatus
	,s.nuCNAE
	,s.unidadeNegocio
from servicosHyp h
left join servicosSharepoint s on s.cdServico = h.cdServico;
