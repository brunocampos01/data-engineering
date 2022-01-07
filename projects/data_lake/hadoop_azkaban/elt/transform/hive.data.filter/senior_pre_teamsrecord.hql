CREATE VIEW IF NOT EXISTS staging.senior_pre_teamsrecord AS
SELECT
  CONCAT(
    CAST(ht.numemp AS  int), '.',
    CAST(ht.tipcol AS  int), '.',
    REPLACE(
      CAST(ht.numcad AS  string),
        '.0','')
      )		            			        AS idEmployeeSenior,
  CAST(ht.iniatu 	   	AS date)   				AS teamUpdateDate,
  CAST(ht.estpos 	   	AS int)         		AS teamStructureCode,
  CAST(ht.postra 	   	AS string)      		AS teamCode
FROM raw.senior_historicoequipetrabalho as ht
ORDER BY teamUpdateDate DESC;
