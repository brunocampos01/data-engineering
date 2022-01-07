CREATE EXTERNAL TABLE raw.sharepoint_centros_de_custo
(
  mmlr string,
  OData_62_m22 string,
  title string,
  status string,
  cr_centroid int,
  xdyd string,
  responsavelorcid string,
  tipocc string,
  diretoriaid int,
  codigo string,
  unid int,
  gestorid int,
  id int,
  b6ss string,
  gerenciaid int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/centros_de_custo';


CREATE EXTERNAL TABLE raw.sharepoint_centros_de_resultado
(
  unidadeid int,
  title string,
  verticalnegocioid int,
  diretoriaid int,
  a2wx string,
  mrkb string,
  OData_75_fr4 string,
  cbet string,
  id int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/centros_de_resultado';

CREATE EXTERNAL TABLE raw.sharepoint_empresas
(
  title string,
  mjfo string,
  vlnb string,
  OData_6e_l99 string,
  CNPJ string,
  id int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/empresas';

CREATE EXTERNAL TABLE raw.sharepoint_produtos
(
  status string,
  unidadeid int,
  title string,
  u7r1 string,
  centroresultadoid int,
  stwf string,
  historico string,
  id int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/produtos';

CREATE EXTERNAL TABLE raw.sharepoint_servicos
(
  unidadeId string,
  status string,
  Descric_e3_odo_x string,
  title string,
  classe string,
  TC_Tax2 string,
  OData_68_149 double,
  CNAE string,
  itemservic_o string,
  tipofaturamento string,
  recorrencia string,
  id int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/servicos';

CREATE EXTERNAL TABLE raw.sharepoint_unidades
(
  title string,
  codigo string,
  id int,
  Sigla string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/unidades';

CREATE EXTERNAL TABLE raw.sharepoint_verticais
(
  OData_67_wh6 string,
  unidadeid int,
  vertical2f_transversal string,
  title string,
  id int
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/verticais';

CREATE EXTERNAL TABLE raw.sharepoint_diretoria
(
  id int,
  title string,
  UnidadedeNegf3Id string,
  ebdz string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/diretoria';

CREATE EXTERNAL TABLE raw.sharepoint_gerencias
(
  id int,
  title string,
  centrodeResultadoId string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/gerencias';

CREATE EXTERNAL TABLE raw.sharepoint_plano_de_contas
(
  id int,
  title string,
  bspg string,
  cifb string,
  descric_e3_o string,
  grupo string,
  status string,
  comoOrc_ar string,
  TipodeRateio string,
  ryhb string,
  r7tf double,
  ff string,
  an2r string
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/hadoop/xpto_company/sharepoint/controladoria/lists/plano_de_contas';
