CREATE DATABASE IF NOT EXISTS account_receivables;

CREATE TABLE IF NOT EXISTS account_receivables.dim_cost_center (
  id string,
  costCenterCode string,
  costCenterName string,
  costCenterType string,
  costCenterStatus string,
  costCenterApportionmentgroup string,
  costCenterClassification string,
  costCenterManager string,
  costCenterManageremail string,
  costCenterResponsible string,
  costCenterResponsibleemail string,
  businessUnitId string,
  businessUnitInitials string,
  businessUnitName string,
  verticalName string,
  verticalType string,
  verticalInitials string,
  boardName string,
  resultCenterName string,
  resultCenterClassification string,
  resultCenterMode string,
  resultCenterPhase string,
  managementName string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de estrutura de negócio'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("PRIMARY KEY"="id", "transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_business_unit (
  id string,
  code string,
  initials string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de Unidade de negócio'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_product (
  id string,
  code string,
  name string,
  description string,
  classification string,
  status string,
  resultCenterName string,
  businessUnitName string,
  businessUnitInitiails string,
  verticalName string,
  boardName string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de Produtos do Oracle'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_segment (
  id string,
  code string,
  name string,
  businessUnitName string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de segmentos'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_account (
  id string,
  code string,
  name string,
  package string,
  groupname string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de conta'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_company (
  id string,
  code string,
  name string,
  cnpj string,
  groupName string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de empresa'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_service (
  id string,
  code string,
  name string,
  class string,
  recurrence string,
  businessUnitInitials string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de serviço'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_intercia (
  id string,
  code string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de InterCia'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_oracle_client (
  id string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de Clientes do Oracle'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.dim_oracle_contract (
  id string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de contratos do Oracle'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS account_receivables.fact_account_receivables (
  sk_date_issuance int NOT NULL,
  sk_due_date int NOT NULL,
  sk_date_received int,
  sk_cost_center string,
  sk_business_unit string NOT NULL,
  sk_product string,
  sk_segment string,
  sk_account string,
  sk_company string,
  sk_service string,
  sk_intercia string,
  sk_oracle_client string,
  sk_oracle_contract string,
  nrRPS string,
  nrNFSE string,
  receivedAmount double COMMENT 'Valor recebido',
  grossAmount double COMMENT 'Valor bruto',
  netAmount double COMMENT 'Valor líquido',
  withheldTaxAmount double COMMENT 'Valor imposto retido',
  originalReceivedAmount double COMMENT 'Valor original recebido',
  grossReceivedAmount double COMMENT 'Valor bruto recebido',
  adjustedAmount double COMMENT 'Valor de ajustes',
  adjustedTaxAmount double COMMENT 'Valor de ajustes de taxas',
  balanceReceivedAmount double COMMENT 'Sado recebido',
  additionAmount double COMMENT 'Valores de acréscimo',
  chargedate timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data de carregamento do registro',
  archived boolean COMMENT 'Indica se o registro está arquivado',
  CONSTRAINT c_sk_date_issuance FOREIGN KEY (sk_date_issuance) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_due_date FOREIGN KEY (sk_due_date) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_date_received FOREIGN KEY (sk_date_received) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_cost_center FOREIGN KEY (sk_cost_center) REFERENCES account_receivables.dim_cost_center(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_business_unit FOREIGN KEY (sk_business_unit) REFERENCES account_receivables.dim_business_unit(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_product FOREIGN KEY (sk_product) REFERENCES account_receivables.dim_product(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_segment FOREIGN KEY (sk_segment) REFERENCES account_receivables.dim_segment(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_account FOREIGN KEY (sk_account) REFERENCES account_receivables.dim_account(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_company FOREIGN KEY (sk_company) REFERENCES account_receivables.dim_company(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_service FOREIGN KEY (sk_service) REFERENCES account_receivables.dim_service(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_intercia FOREIGN KEY (sk_intercia) REFERENCES account_receivables.dim_intercia(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_oracle_client FOREIGN KEY (sk_oracle_client) REFERENCES account_receivables.dim_oracle_client(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_ar_oracle_contract FOREIGN KEY (sk_oracle_contract) REFERENCES account_receivables.dim_oracle_contract(id) DISABLE NOVALIDATE
)
COMMENT 'Fato de recebimentos'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");
