CREATE DATABASE IF NOT EXISTS general_ledger;

CREATE TABLE IF NOT EXISTS general_ledger.dim_purchase_order (
  id string,
  code string,
  invoiceId string,
  itemName string,
  info string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão do Ordem de compra'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS general_ledger.dim_currency (
  id string,
  code string,
  name string,
  category string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de moeda'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS general_ledger.dim_provider (
  id string,
  name string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de fornecedores'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS general_ledger.dim_allocation (
  id string,
  code string,
  descLevel1 string,
  descLevel2 string,
  descLevel3 string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de alocação'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS general_ledger.fact_general_ledger (
  sk_date int,
  sk_business_structure string,
  sk_coa string,
  sk_purchase_order string,
  sk_currency string,
  sk_provider string,
  sk_allocation string,
  debitValue decimal(16, 6) COMMENT 'Valor de débito',
  creditValue decimal(16, 6) COMMENT 'Valor de crédito',
  balanceValue decimal(16, 6) COMMENT 'Saldo',
  chargedate timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data de carregamento do registro',
  archived boolean COMMENT 'Indica se o registro está arquivado',
  CONSTRAINT c_sk_gl_date FOREIGN KEY (sk_date) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_business_structure FOREIGN KEY (sk_business_structure) REFERENCES default.dim_business_structure(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_coa FOREIGN KEY (sk_coa) REFERENCES default.dim_coa(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_purchase_order FOREIGN KEY (sk_purchase_order) REFERENCES general_ledger.dim_purchase_order(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_currency FOREIGN KEY (sk_currency) REFERENCES general_ledger.dim_currency(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_provider FOREIGN KEY (sk_provider) REFERENCES general_ledger.dim_provider(id) DISABLE NOVALIDATE,
  CONSTRAINT c_sk_gl_allocation FOREIGN KEY (sk_allocation) REFERENCES general_ledger.dim_allocation(id) DISABLE NOVALIDATE
)
COMMENT 'Fato de general ledger'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");
