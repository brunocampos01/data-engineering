CREATE VIEW IF NOT EXISTS staging.senior_employeeinfo AS
SELECT
  CONCAT(
    CAST(info.numemp AS  int), '.',
    CAST(info.tipcol AS  int), '.',
    REPLACE(
      CAST(info.numcad AS  string),
        '.0','')
      )		            			            AS idEmployeeSenior,
  UPPER(CAST(info.endrua   AS string))        AS adress,
  CAST(info.endnum 	   	AS int)             AS adressnumber,
  UPPER(CAST(info.endcpl   AS string))        AS complement,
  CAST(info.codest 	   	AS string)          AS state,
  LCASE(CAST(info.emacom 	   	AS string))          AS email
FROM raw.senior_infocolaboradores as info;
