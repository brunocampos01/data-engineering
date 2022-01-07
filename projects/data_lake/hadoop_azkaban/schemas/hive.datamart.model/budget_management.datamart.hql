CREATE DATABASE IF NOT EXISTS budget_management;

CREATE TABLE IF NOT EXISTS budget_management.dim_version (
  id string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de versões do hyperion'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS budget_management.dim_observation (
  id string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de observações do hyperion'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS budget_management.dim_account_hierarchy (
  id string,
  source string,
  hierarchy string,
  orderNumber int,
  account string,
  accountlevel01 string,
  accountlevel02 string,
  accountlevel03 string,
  accountlevel04 string,
  accountlevel05 string,
  accountlevel06 string,
  accountlevel07 string,
  accountlevel08 string,
  accountlevel09 string,
  accountlevel10 string,
  orderlevel01 int,
  orderlevel02 int,
  orderlevel03 int,
  orderlevel04 int,
  orderlevel05 int,
  orderlevel06 int,
  orderlevel07 int,
  orderlevel08 int,
  orderlevel09 int,
  orderlevel10 int,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de Hierarquia de contas'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS budget_management.fact_budget_management (
  sk_date int NOT NULL,
  sk_business_structure string,
  sk_coa string,
  sk_account_hierarchy string,
  sk_version string,
  sk_obs string,
  budgetedValue double COMMENT 'Valor orçado',
  accomplishedValue double COMMENT 'Valor realizado',
  forecastValue double COMMENT 'Valor Previsto',
  chargedate timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data de carregamento do registro',
  archived boolean COMMENT 'Indica se o registro está arquivado',
  CONSTRAINT c_sk_date FOREIGN KEY (sk_date) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_bm_business_structure FOREIGN KEY (sk_business_structure) REFERENCES default.dim_business_structure(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_bm_coa FOREIGN KEY (sk_coa) REFERENCES default.dim_coa(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_bm_account_hierarchy FOREIGN KEY (sk_account_hierarchy) REFERENCES budget_management.dim_account_hierarchy(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_bm_version FOREIGN KEY (sk_version) REFERENCES budget_management.dim_version(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_bm_obs FOREIGN KEY (sk_obs) REFERENCES budget_management.dim_observation(id) DISABLE NOVALIDATE
)
COMMENT 'Fato de gestão orçamentaria'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");
