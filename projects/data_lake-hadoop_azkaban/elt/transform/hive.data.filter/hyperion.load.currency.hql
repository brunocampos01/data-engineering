CREATE VIEW staging.hyperion_currency AS
SELECT
	 T2.MEMBER AS id
	,T2.MEMBER AS name
	,T1.MEMBER AS category
FROM raw.hyperion_metadados T1
LEFT JOIN raw.hyperion_metadados T2 ON T2.PARENT = T1.MEMBER
WHERE T1.DIMENSION = 'MOEDA' AND T1.PARENT = 'MOEDA'
