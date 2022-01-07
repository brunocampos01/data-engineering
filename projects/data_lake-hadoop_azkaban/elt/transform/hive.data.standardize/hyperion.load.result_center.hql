CREATE VIEW staging.hyperion_result_center AS
select
		cr.id 								as id
	 ,cr.title	 						as name
	 ,u.codigo 							as idBusiness_unit
	 ,cr.verticalnegocioid	as idVertical
	 ,cr.diretoriaid	  		as idBoard
	 ,cr.mrkb		 						as subVertical
	 ,cr.cbet 							as mode
	 ,cr.odata_75_fr4				as phase
	 ,cr.a2wx								as classification
from raw.sharepoint_centros_de_resultado as cr
left join raw.sharepoint_unidades u on u.id = cr.unidadeid
