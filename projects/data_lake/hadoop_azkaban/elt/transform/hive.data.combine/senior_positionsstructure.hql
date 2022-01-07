CREATE VIEW IF NOT EXISTS staging.senior_positionsstructure AS
SELECT
  CAST(cargos.codcar 	   	AS string)		  AS id,
  CAST(cargos.estcar 		AS int)     	  AS positionStructurecode,
  REPLACE(
	  REPLACE(
	    CAST(cargos.usu_subtit 	AS string),
	    		 '""',''), '"', '')         AS positionTitle,
  CASE
  	WHEN cargos.estcar=3 AND cargos.codcar=0011 THEN 'II'
  	WHEN cargos.estcar=3 AND cargos.codcar=0040 THEN 'I'
  	WHEN cargos.estcar=3 AND cargos.codcar=0011 THEN 'II'
    WHEN cargos.usu_nivcar='NA' THEN NULL
    WHEN cargos.usu_nivcar=' ' THEN NULL
    WHEN cargos.usu_nivcar=' ' THEN NULL
    ELSE cargos.usu_nivcar
  END 										AS positionSeniority,
  CAST(cargos.titcar  		  AS string)    AS fullpositionTitle,
  CAST(cargos.caralf  		  AS string)    AS positionTitleProfile,
  jobtype.positiontype,
  jobtype.positioncategory,
  grades.positiongrade,
  CAST(cargos.datcri  		  AS timestamp) AS creationDate
FROM raw.senior_cargos AS cargos
	LEFT JOIN staging.senior_positiontype AS jobtype ON cargos.siscar = jobtype.id
	LEFT JOIN staging.senior_positions_grades AS grades ON cargos.estcar = grades.positionstructurecode AND CAST(cargos.codcar AS int) = CAST(grades.idposition AS int)
	WHERE cargos.estcar IS NOT NULL
		AND cargos.estcar <> 10
		AND cargos.codcar NOT IN('9999',
								 '8888'
								 '0900',
								 '0901',
								 '0902',
								 '0903',
								 '0904',
								 '0905',
								 '0906',
								 '0907',
								 '0908',
								 '0909',
								 '0910',
								 '0911',
								 '0912',
								 '0913')
