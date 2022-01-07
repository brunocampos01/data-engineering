CREATE MATERIALIZED VIEW staging.oracle_business_structure as
select
	 md5(cc.code)									as skBusinessStructure
	,cc.code	 										as nkBusinessStructure
	,cc.internalcode 				      as costCenterCode
	,cc.shortname					        as costCenterName
	,cc.type						          as costCenterType
	,cc.status						        as costCenterStatus
	,cc.apportionmentgroup  		  as costCenterApportionmentgroup
	,cc.classification 				    as costCenterClassification
	,cc.wagebillclassification    as wageBillClassification
	,cc.manager						        as costCenterManager
	,LOWER(cc.manageremail)			  as costCenterManageremail
	,cc.responsible					      as costCenterResponsible
	,LOWER(cc.responsibleemail)		as costCenterResponsibleemail
	,bu.id							          as businessUnitId
	,bu.initials					        as businessUnitInitials
	,bu.fullname					        as businessUnitName
	,v.name							          as verticalName
	,v.type							          as verticalType
	,v.initials						        as verticalInitials
	,b.name							          as boardName
	,rc.name						          as resultCenterName
	,rc.classification				    as resultCenterClassification
	,rc.mode						          as resultCenterMode
	,rc.phase						          as resultCenterPhase
	,m.name							          as managementName
from staging.oracle_cost_center as cc
JOIN staging.oracle_business_unit as bu on cc.idbusinessunit = bu.id
JOIN staging.hyperion_vertical as v on cc.idvertical = v.id
JOIN staging.hyperion_board as b on cc.idboard = b.id
JOIN staging.hyperion_result_center as rc on cc.idresultscenter = rc.id
JOIN staging.hyperion_management as m on cc.idmanagement = m.id;
