CREATE TABLE raw.senior_cargos_grades_estrutura_3
(
  estrutura string,
  codigo string,
  cargo string,
  grade string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/user/hadoop/xpto_company/senior/csv_cargos_grade';
