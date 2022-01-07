CREATE EXTERNAL TABLE raw.sharepoint_hierarquias_de_conta
(
  hierarquia string,
  ordem string,
  conta_nivel_1 string,
  conta_nivel_2 string,
  conta_nivel_3 string,
  conta_nivel_4 string,
  conta_nivel_5 string,
  conta_nivel_6 string,
  conta_nivel_7 string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = '\"'
 )
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/docs/hierarquias_de_conta'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE raw.sharepoint_ajustes_manuais
(
  data string,
  cdunidadenegocio string,
  cdnegocio string,
  cdprojetooracle string,
  cdproduto string,
  cdservico string,
  cdsegmento string,
  cdmoeda string,
  cdempresa string,
  cdintercia string,
  cdalocacao string,
  cdconta string,
  vlrealizado double,
  vlorcado double,
  deobservacao string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/docs/ajustes_manuais'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE raw.sharepoint_rateio
(
  cc string,
  data string,
  tj_unjC000066 double,
  pj_unjC000063 double,
  mp_unjC000061 double,
  pgmbox_unjC000126 double,
  inovacao_unjD000034 double,
  adv_unjC000054 double,
  inter_unjD000036 double,
  overhead_unjZ000004 double
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/docs/rateio'
TBLPROPERTIES ("skip.header.line.count"="2");
