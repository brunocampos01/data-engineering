INSERT OVERWRITE TABLE staging.hyperion_accounts_apportionment
select coalesce(
          NULLIF(conta_nivel_6,''),
          NULLIF(conta_nivel_5,''),
          NULLIF(conta_nivel_4,''),
          NULLIF(conta_nivel_3,''),
          NULLIF(conta_nivel_2,''),
          NULLIF(conta_nivel_1,'')) as nmAccount
from raw.sharepoint_hierarquias_de_conta as shdc
where hierarquia = 'DRE Verticais Nova Mercado'
and conta_nivel_2 in ('[-] DESPESA OPERACIONAL', '[-] DEPRECIAÇÕES E AMORTIZAÇÕES', '[-] UTILIDADES E OCUPAÇÃO');

INSERT OVERWRITE TABLE staging.hyperion_apportionment_annulment
select
     h.cddata                           as cddate
    ,h.cdversao                         as cdVersion
    ,cc.code                            as cdCostCenter
    ,cc.code								            as cdCostCenterFrom
    ,''											            as cdCostCenterTo
    ,'#Rateio - Anulação' 							as cdAccount
    ,CONCAT('#Rateio - Anulação: ', cc.code) 		as deObs
    ,''											            as pctApportionment
    ,sum(h.vlorcado) * -1 							as vlBudgeted
    ,sum(h.vlrealizado) * -1 						as vlAccomplished
from staging.hyperion_financas as h
inner join staging.oracle_cost_center cc on cc.code = h.cdcentroCusto
inner join staging.oracle_accounts d on d.id = h.cdconta
inner join staging.hyperion_accounts_apportionment hcr ON hcr.nmAccount = d.name
where (h.cdversao = 'Trabalho' or h.cdVersao ='0+12')
group by h.cddata ,h.cdversao ,cc.code;


INSERT OVERWRITE TABLE
	staging.hyperion_apportionment
  SELECT
  	ra.cddate
  	,ra.cdVersion
  	,ra.cdCostCenter
  	,ra.cdCostCenterFrom
  	,ra.cdCostCenterTo
  	,ra.cdAccount
  	,ra.deObs
  	,ra.pctApportionment
  	,ra.vlBudgeted
  	,ra.vlAccomplished
  FROM staging.hyperion_apportionment_annulment AS ra
  INNER JOIN staging.oracle_cost_center cc ON cc.code = ra.cdCostCenter
  WHERE EXISTS (SELECT 1 FROM staging.sharepoint_apportionment AS ro WHERE ro.costcenterfrom = ra.cdCostCenter AND ra.cddate = regexp_replace(to_date(iddate),'-',''));

INSERT INTO TABLE
	staging.hyperion_apportionment
  SELECT
  	 ra.cddate
  	,ra.cdVersion
  	,cc.code
  	,rd.costcenterfrom
  	,rd.costcenterto
  	,'#Rateio - Cálculo de Rateio'				AS cdAccount
  	,CONCAT('#Rateio - Cálculo de Rateio: ', rd.costcenterfrom, '->', rd.costcenterto) 	AS deObs
  	,rd.percentage
  	,(vlbudgeted * -1) 	* rd.percentage 		AS vlBudgeted
  	,(vlaccomplished * -1) * rd.percentage 		AS vlAccomplished
  FROM staging.hyperion_apportionment_annulment AS ra
  INNER JOIN staging.sharepoint_apportionment rd ON rd.costcenterfrom = ra.cdCostCenterFrom AND ra.cddate = regexp_replace(to_date(rd.iddate),'-','')
  INNER JOIN staging.oracle_cost_center cc ON cc.code = rd.costcenterto;
