create view staging.sccd_ungp_domain 
as select synonymdomain.`defaults`,
		  synonymdomain.domainid,
		  synonymdomain.`maxvalue`,
		  synonymdomain.description as internal,
		  synonymdomain.orgid,
		  synonymdomain.pluspcustomer,
		  synonymdomain.siteid,
		  synonymdomain.synonymdomainid,
		  synonymdomain.value, 
		  synonymdomain.valueid,
		  lsynonymdomain.description,
		  lsynonymdomain.l_synonymdomainid,
		  lsynonymdomain.langcode
from raw.sccd_ungp_synonymdomain synonymdomain 
join raw.sccd_ungp_lsynonymdomain lsynonymdomain on synonymdomain.synonymdomainid = lsynonymdomain.ownerid