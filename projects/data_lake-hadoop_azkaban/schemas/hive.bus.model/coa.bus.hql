CREATE TABLE IF NOT EXISTS default.dim_coa (
  id string,
  businessUnitId string,
  costCenterId string,
  projectId string,
  projectName string,
  productName string,
  productDescription string,
  productClassification string,
  serviceId string,
  serviceName string,
  serviceRecorrence string,
  segmentId string,
  segmentName string,
  companyId string,
  companyName string,
  companyCNPJ string,
  companyGroup string,
  interciaId string,
  interciaName string,
  accountId string,
  accountName string,
  accountPackage string,
  accountGroup string,
  UNIQUE (id) DISABLE,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de Chart of accounts'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("PRIMARY KEY"="id", "transactional"="true", "orc.compress"="ZLIB");
