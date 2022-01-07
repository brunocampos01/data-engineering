CREATE VIEW staging.hyperion_intercia AS
SELECT
	 T3.member 											AS CDINTERCIA
	,REPLACE(REPLACE(T3.ALIAS, T3.MEMBER, ''),'-','') 	AS NMINTERCIA
FROM raw.hyperion_metadados T1
LEFT JOIN raw.hyperion_metadados T2 ON T1.MEMBER = T2.PARENT
LEFT JOIN raw.hyperion_metadados T3 ON T2.MEMBER = T3.PARENT
WHERE T1.DIMENSION = 'INTER-CIA'
AND T1.MEMBER = 'INTER-CIA'
AND T3.MEMBER <> 'NA_Inter-CIA'
