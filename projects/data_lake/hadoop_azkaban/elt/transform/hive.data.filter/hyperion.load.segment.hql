CREATE VIEW staging.hyperion_segment AS
SELECT
	REPLACE(hm.parent,'Segmento_','') 									AS businessUnit
	,hm.MEMBER															AS id
	,REPLACE(REPLACE(REPLACE(hm2.ALIAS,hm.MEMBER,''),'(',''),')','')	AS name
FROM raw.hyperion_metadados as hm --T1
LEFT JOIN raw.hyperion_metadados hm2 ON hm.MEMBER = hm2.MEMBER AND hm2.PARENT LIKE 'Total%'
WHERE hm.DIMENSION = 'SEGMENTO' AND hm.PARENT LIKE 'Segmento%'
