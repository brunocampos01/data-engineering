SELECT
  MARKET_DATA_TYPE_SK,
  TRIM(MARKET_DATA_TYPE_CODE) AS MARKET_DATA_TYPE_CODE,
  TRIM(MARKET_DATA_TYPE_NAME) AS MARKET_DATA_TYPE_NAME,
  TRIM(MARKET_DATA_BUSINESS_TYPE_NAME) AS MARKET_DATA_BUSINESS_TYPE_NAME
--  ETL_CREATED_DATE,
--  ETL_CREATED_PROCESS_ID,
--  ETL_UPDATED_DATE,
--  ETL_UPDATED_PROCESS_ID
FROM
  dw.DIM_MARKET_DATA_TYPE