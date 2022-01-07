set hive.support.concurrency = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.txn.manager = 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager';

drop table sla.dim_agreements;
drop table sla.dim_contract;
drop table sla.dim_customer;
drop table sla.dim_decessions;
drop table sla.dim_extensions;
drop table sla.dim_failures;
drop table sla.dim_history;
drop table sla.dim_product;
drop table sla.dim_service;
drop table sla.dim_worklog;

CREATE TABLE IF NOT EXISTS sla.dim_contract (
  id string,
  legacy string,
  contract string,
  description string,   
  status string COMMENT 'Situação na qual o contrato se encontra com relação a negociação com o cliente',
  manager string COMMENT 'Gestor do contrato',
  expiration timestamp  COMMENT 'Data de expiração do contrato',
  extension timestamp COMMENT 'Data de expiração prorrogada',
  warranty timestamp COMMENT 'Data de expiração da garantia',
  enterprise string COMMENT 'Empresa em contrato',
  site string COMMENT 'Área de negócio da organização',
  sector string COMMENT 'Setor responsável pela contratação',
  department string COMMENT 'Departamento responsável pela contratação',
  nms int COMMENT 'Nível mínimo do serviço',
  reductor int COMMENT 'Percentual redutor em caso de infração do nível mínimo de serviço',
  tolerance int COMMENT 'Tolerancia para o percentual redutor em caso de infração do nível mínimo de serviço',
  customer string COMMENT 'Nome do cliente',
  initials string COMMENT 'Sigla do cliente',
  year_month string,
  hash string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de contratos das unidades de negócio'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_service (
  id string,
  service string COMMENT 'Código da solicitação de serviço',
  site string COMMENT 'Área de negócio da organização',
  state string COMMENT 'Situação',
  status string COMMENT 'Situação na qual a solicitação de serviço se encontra perante seu fluxo de negócio',
  summary string COMMENT 'Resumo da solicitação de serviço',
  modal string COMMENT 'Veículo pelo qual a solicitação foi realizada', 
  class string COMMENT 'Classe da solicitação',
  contract string COMMENT 'Contrato ao qual a solicitação está vinculada',
  customer string COMMENT 'Orgão, empresa, ou instituição cliente solicitante definida em contrato',
  team string COMMENT 'Equipe responsável pela realização do serviço',
  version string COMMENT 'Versão do produto para o qual a solicitação exite, ou versão de lançamento da feature',
  severity string COMMENT 'Severidade imposta ao serviço',
  description string COMMENT 'Descrição da solicitação de serviço',
  details string COMMENT 'Detalhamento da descrição da solicitação de serviço',
  affectedperson string COMMENT 'Pessoa afetada pela execução da solicitação de serviço ou necessitada da mesma',
  creationdate timestamp COMMENT 'Data de criação da solicitação de serviço',
  startdate timestamp COMMENT 'Data de inicio efetivo de trabalho na solicitação de serviço',
  finishdate timestamp COMMENT 'Data de encerramento efetivo de trabalho na solicitação de serviço',
  targetstart timestamp COMMENT 'Data de inicio previsto de trabalho na solicitação de serviço',
  targetfinish timestamp COMMENT 'Data de encerramento previsto de trabalho na solicitação de serviço',
  adjustedtargetresolutiontime timestamp COMMENT 'Prazo para conclusão do trabalho ajustado pelas adiversidades do processo de atendimento',
  servicelevel string COMMENT 'Grupo de serviços da solicitação que remete ao nível do suporte',
  product string COMMENT 'Produto a qual a solicitação é referente',
  module string COMMENT 'Módulo do produto a qual a solicitação é referente',
  feature string COMMENT 'Funcionalidade do produto a qual a solicitação é referente',
  sysorigin string COMMENT 'Sistema de origem que provisionou os dados da solicitação', 
  hash string,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de solicitações de serviço expedidas'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.bloom.filter.columns"="class,status,severity,modal,servicelevel", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_product (
  id string NOT NULL,
  site string NOT NULL COMMENT 'Área de negócio da organização',
  name string NOT NULL COMMENT 'Nome do produto',
  description string COMMENT 'Descrição do produto',
  module string COMMENT 'Módulo do produto',
  feature string COMMENT 'Funcionalidade',
  owner string COMMENT 'Proprietário da funcionalidade',
  hash string NOT NULL,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de produtos da unidade de negócio'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_history (
  id string NOT NULL,
  br_history string NOT NULL COMMENT 'Bridge para acesso unitário do grão de serviço a partir dos fatos',
  service string CONSTRAINT c_history_service NOT NULL DISABLE NORELY,
  site string NOT NULL COMMENT 'Área de negócio da organização',
  status string NOT NULL COMMENT 'Situação da solicitação',
  description string NOT NULL COMMENT 'Descrição da situação da solicitação',
  changedate timestamp NOT NULL COMMENT 'Data de alteração para a situação',
  timetracking decimal(16, 6) COMMENT 'Duração de permanência na situação',
  hash string NOT NULL,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE,
  UNIQUE (service) DISABLE NOVALIDATE
) COMMENT 'Dimensão de histórico das solicitações de serviço'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_extensions (
  id string,
  br_extension string COMMENT 'Bridge para acesso unitário do grão de serviço a partir dos fatos',
  service string,
  worklog string,
  site string COMMENT 'Área de negócio da organização',
  description string COMMENT 'Justificativa do pedido de prorrogação ou extensão do prazo',
  acceptdate timestamp COMMENT 'Data em que a prorrogação foi formalizada com o aceite',
  concededate timestamp COMMENT 'Data concedida pelo cliente na prorrogação do prazo',
  approval string COMMENT 'Usuário que formalizou o aceite da prorrogação',
  submitdate timestamp COMMENT 'Data solicitada pela empresa na prorrogação do prazo',
  submitter string COMMENT 'Usuário que efetuou a solicitação de prorrogação do prazo',
  submiteddate timestamp COMMENT 'Data proposta para prorrogação',
  currentdeadline timestamp COMMENT 'Data da conclusão ajustada anterior a formalização da prorrogação de prazo',
  hash string,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE,
  UNIQUE (service) DISABLE NOVALIDATE
) COMMENT 'Dimensão de prorrogações das solicitações de serviço'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_agreements (
  id string NOT NULL,
  br_agreement string NOT NULL COMMENT 'Bridge para acesso unitário do grão de serviço a partir dos fatos',
  service string CONSTRAINT c_dim_agreements_service NOT NULL,
  worklog string NOT NULL,
  site string NOT NULL COMMENT 'Área de negócio da organização',
  description string COMMENT 'Justificativa para o acordo de prazo estabelecido na solicitação',
  acceptdate timestamp COMMENT 'Data em que o acordo foi formalizado com o aceite',
  accepteddate timestamp COMMENT 'Data aceita para o acordo do prazo',
  approval string COMMENT 'Usuário que formalizou o aceite no acordo de prazo',
  submitter string COMMENT 'Usuário que efetuou o pedido do acordo de prazo',
  submitdate timestamp COMMENT 'Data solicitada para o acordo do prazo',
  currentdeadline timestamp COMMENT 'Data da conclusão ajustada anterior a formalização do acordo',
  hash string NOT NULL,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE,
  UNIQUE (service) DISABLE NOVALIDATE
) COMMENT 'Dimensão de acordos de prazo estabelecidos em solicitações de serviço'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_decessions (
  id string,
  br_decession string COMMENT 'Bridge para acesso unitário do grão de serviço a partir dos fatos',
  service string,
  worklog string,
  site string COMMENT 'Área de negócio da organização',
  description string COMMENT 'Justificativa para alteração da severidade',
  approvaldescription string COMMENT 'Conteúdo da aprovação da alteração de serveridade',
  submitdate timestamp COMMENT 'Data em que a solicitação para alteração de severidade foi realizada',
  submitter string COMMENT 'Usuário que solicitou a alteração de severidade',
  acceptdate timestamp COMMENT 'Data em que a formalização da alteração de severidade ocorreu',
  approval string COMMENT 'Usuário que autorizou a alteração de severidade',
  original string COMMENT 'Severidade anterior',
  renew string COMMENT 'Severidade renovada',
  currentdeadline timestamp COMMENT 'Data da conclusão ajustada anterior a formalização do aceite',
  hash string,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de decessões ou alterações de severidade realizadas nas solicitações de serviço'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.dim_failures (
  id string,
  br_failures string COMMENT 'Bridge para acesso unitário do grão de serviço a partir dos fatos',
  service string,
  worklog string,
  site string COMMENT 'Área de negócio da organização',
  description string COMMENT 'Justificativa para falha ou reprovação',
  reprovaldate timestamp COMMENT 'Data da reprovação ou averiguação da falha',
  hash string,
  year_month string,
  PRIMARY KEY (id) DISABLE NOVALIDATE,
  UNIQUE (service) DISABLE NOVALIDATE
) COMMENT 'Dimensão de falhas ou reprovações de entrega realizadas nas solicitações de serviço'
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");

CREATE TABLE IF NOT EXISTS sla.fact_sla (
  id string,
  sk_service string,
  sk_contract string,
  sk_history string,
  sk_product string,
  sk_extensions string,
  sk_agreements string,
  sk_decessions string,
  sk_failures string,
  site string COMMENT 'Área de negócio da organização',
  contractualfulfillment boolean COMMENT 'Determinação de cumprimento do prazo sem qualquer incidencia de negociação',
  fulfillment boolean COMMENT 'Determinação de cumprimento do prazo com influência de negociações',
  holdtime decimal(16, 6) COMMENT 'Tempo total de espera durante toda vida da solicitação',
  leadtime decimal(16, 6) COMMENT 'Tempo total para entrega da solicitação, considera da data de criação a data de encerramento',
  cycletime decimal(16, 6) COMMENT 'Tempo total de produção da solicitação, agrega também as pausas ocorridas nesse processo',
  reactiontime decimal(16, 6) COMMENT 'Tempo de reação para inicio do trabalho na solicitação, considera da data de criação ao inicio efetivo',
  agregatedtime decimal(16, 6) COMMENT 'Tempo agregado referentes a processos secundários ao fluxo normal que estendem o prazo estabelecido em contrato',
  failures int COMMENT 'Quantidade de reprovações envolvidas',
  extensions int COMMENT 'Quantidade de prorrogações efetivadas',
  agreements int COMMENT 'Quantidade de acordos de prazo efetivados',
  decessions int COMMENT 'Quantidade de alterações de severidade',
  chargedate timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data de carregamento do registro',
  hash string,
  archived boolean COMMENT 'Indica se o registro está arquivado',
  PRIMARY KEY (id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_service FOREIGN KEY (sk_service) REFERENCES sla.dim_service(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_contract FOREIGN KEY (sk_contract) REFERENCES sla.dim_contract(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_product FOREIGN KEY (sk_product) REFERENCES sla.dim_product(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_history FOREIGN KEY (sk_history) REFERENCES sla.dim_history(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_extensions FOREIGN KEY (sk_extensions) REFERENCES sla.dim_extensions(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_failures FOREIGN KEY (sk_failures) REFERENCES sla.dim_failures(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_agreements FOREIGN KEY (sk_agreements) REFERENCES sla.dim_agreements(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_decessions FOREIGN KEY (sk_decessions) REFERENCES sla.dim_decessions(id) DISABLE NOVALIDATE
)
COMMENT 'Fato de acordos do nível de serviço, centraliza a informação sobre o progresso do trabalho das solicitações e o vinculo com as determinações contratuais'
PARTITIONED BY (sysorigin string, year_month string)
CLUSTERED BY (site) INTO 8 BUCKETS
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB", "orc.create.index"="true");