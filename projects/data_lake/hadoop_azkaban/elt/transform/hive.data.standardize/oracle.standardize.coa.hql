INSERT OVERWRITE TABLE default.dim_coa
select
	DISTINCT md5(concat(hf.cdunidadenegocio, hf.cdcentrocusto, hf.cdprojeto, hf.cdproduto, hf.cdservico, hf.cdsegmento, hf.cdempresa, hf.cdintercia, hf.cdconta)) as id
	,bu.id
	,cc.code
	,proj.id
	,proj.name
	,prod.name
	,prod.description
	,prod.classification
	,ser.cdservico
	,ser.nmservico
	,ser.derecorrencia
	,seg.id
	,seg.name
	,c.cdempresa
	,c.nmempresa
	,c.decnpj
	,c.degrupo
	,i.cdintercia
	,i.nmintercia
	,ct.id
	,ct.shortname
	,ct.package
	,ct.groupname
from staging.hyperion_financas hf
left join staging.oracle_business_unit bu on hf.cdunidadenegocio = bu.initials or hf.cdunidadenegocio = 'NA_Negocio'
left join staging.oracle_cost_center cc on hf.cdcentrocusto = cc.code
left join staging.oracle_project proj on hf.cdprojeto = cast(concat('PR', lpad(proj.id, 5, "0")) as string)
left join staging.oracle_products prod on hf.cdproduto = prod.code
left join staging.hyperion_service ser on hf.cdservico = ser.cdservico and (hf.cdunidadenegocio = ser.unidadenegocio or ser.cdservico = 'NA_Servico')
left join staging.hyperion_segment seg on hf.cdsegmento = seg.id and hf.cdunidadenegocio = seg.businessunit
left join staging.hyperion_company c on hf.cdempresa = c.cdempresahyp
left join staging.hyperion_intercia i on hf.cdintercia = i.cdintercia
left join staging.oracle_accounts ct on hf.cdconta = ct.id
