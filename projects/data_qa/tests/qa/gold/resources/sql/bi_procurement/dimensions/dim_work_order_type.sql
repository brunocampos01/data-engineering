SELECT
	WO_TYPE_SK,
    WO_TYPE_NAME,
    WO_TYPE_CODE
    -- ETL_CREATED_DATE,
    -- ETL_CREATED_PROCESS_ID,
    -- ETL_UPDATED_DATE,
    -- ETL_UPDATED_PROCESS_ID
  FROM dw.DIM_WORK_ORDER_TYPE
  