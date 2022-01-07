CREATE TABLE raw.senior_colaboradores_semunidade_pre052014
(
  name string,
  id string,
  cpf string,
  hiringDate string,
  workUnity string,
  idWorkUnity int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/user/hadoop/xpto_company/senior/csv/';
