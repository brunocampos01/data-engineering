CREATE DATABASE IF NOT EXISTS headcount;

CREATE TABLE IF NOT EXISTS headcount.dim_version (
  id string NOT NULL,
  name string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de versões do hyperion'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS headcount.dim_handle (
  id string NOT NULL,
  name string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de movimentos centros de custo do hyperion'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS headcount.dim_position (
  id string NOT NULL,
  name string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de tipo de profissional'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS headcount.fact_headcount (
  sk_date int,
  sk_business_structure string,
  sk_version string,
  sk_handle string,
  sk_position string,
  budgeted int COMMENT 'Valor total de headcount orçado',
  accomplished int COMMENT 'Valor total de headcount realizado',
  chargedate timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT 'Data de carregamento do registro',
  archived boolean COMMENT 'Indica se o registro está arquivado',
  CONSTRAINT c_hc_sk_date FOREIGN KEY (sk_date) REFERENCES default.dim_date(id) DISABLE NOVALIDATE,
  CONSTRAINT c_hc_sk_business_structure FOREIGN KEY (sk_business_structure) REFERENCES default.dim_business_structure(id) DISABLE NOVALIDATE,
  CONSTRAINT c_hc_sk_version FOREIGN KEY (sk_version) REFERENCES headcount.dim_version(id) DISABLE NOVALIDATE,
  CONSTRAINT c_hc_sk_handle FOREIGN KEY (sk_handle) REFERENCES headcount.dim_handle(id) DISABLE NOVALIDATE,
  CONSTRAINT c_hc_sk_position FOREIGN KEY (sk_position) REFERENCES headcount.dim_position(id) DISABLE NOVALIDATE
)
COMMENT 'Fato de total de colaboradores (headcount)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");
