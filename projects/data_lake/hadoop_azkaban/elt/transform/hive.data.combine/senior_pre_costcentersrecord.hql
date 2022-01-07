CREATE VIEW IF NOT EXISTS staging.senior_pre_costcentersrecord AS
SELECT
  CONCAT(
    CAST(numemp AS  int), '.',
    CAST(tipcol AS  int), '.',
    REPLACE(
      CAST(numcad AS  string),
        '.0','')
      )		            			        AS 	idEmployeeSenior,
  CAST(datalt 	   	AS date)   				AS ccuUpdateDate,
  CAST(codccu 	   	AS string)      AS idccusenior,
  ccu.id 							AS idcc,
  ccu.businessunityinitial			AS businessunity
FROM raw.senior_historicocentrosdecusto AS hccu
  LEFT JOIN staging.senior_costcenter AS ccu ON hccu.codccu = ccu.idccusenior
  ORDER BY ccuUpdateDate DESC;
