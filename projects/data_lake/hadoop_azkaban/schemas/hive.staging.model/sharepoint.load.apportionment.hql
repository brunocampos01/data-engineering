CREATE TABLE IF NOT EXISTS staging.sharepoint_apportionment (
  idDate timestamp,
  costCenterFrom string,
  costCenterTo string,
  percentage double
)
COMMENT 'Dados de Rateio do sharepoint organizados por linha'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');
