SELECT 
    VESSEL_INV_SK
    ,VESSEL_INV_COY_CODE
    ,VESSEL_INV_VOUCHER_CODE
    ,VESSEL_INV_STATUS_CODE
    ,VESSEL_INV_STATUS_DESC
    ,VESSEL_INV_STATUS_DATE
    ,VESSEL_INV_ORDER_ORIG_NO
    ,VESSEL_INV_ORDER_NO
    ,VESSEL_INV_ORDER_SK
    ,VESSEL_INV_CURR_ORIG_CODE
    ,VESSEL_INV_CURR_SK
    ,VESSEL_INV_CURR_CODE
    ,VESSEL_INV_CURR_NAME
    ,VESSEL_INV_CURR_AMT
    ,VESSEL_INV_BASE_AMT
    ,VESSEL_INV_CURR_CAD_AMT
    ,VESSEL_INV_USD_AMT
    ,VESSEL_INV_EUR_AMT
    ,VESSEL_INV_SUPPLIER_ORIG_CODE
    ,VESSEL_INV_SUPPLIER_SK
    ,VESSEL_INV_SUPPLIER_CODE
    ,VESSEL_INV_SUPPLIER_NAME
    ,VESSEL_INV_SUPPLIER_INVOICE_NO
    ,VESSEL_INV_SUPPLIER_INVOICE_DATE
    ,VESSEL_INV_CODE
    ,ETL_CREATED_DATE
    ,ETL_CREATED_PROCESS_ID
    ,ETL_UPDATED_DATE
    ,ETL_UPDATED_PROCESS_ID
FROM dw.DIM_VESSEL_INVOICE_HEADER