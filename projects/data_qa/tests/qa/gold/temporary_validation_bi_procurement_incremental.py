# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Procurement: Incremental validation

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, expr

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_po_header`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_commercial.dim_port

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_po_header where ifnull(order_no, 'Unknown') = 'Unknown';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_invoice_header where ifnull(order_no, 'Unknown') = 'Unknown' and order_orig_no is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_po_header where coy_code in(
# MAGIC select distinct coy_code from test_gold.dw_procurement.dim_ship_invoice_header where ifnull(order_no, 'Unknown') = 'Unknown' and order_orig_no is null);

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table test_gold.dw_commercial.fact_ship_cust_forecast_mthly
# MAGIC select * from test_gold.dw_commercial.fact_ship_cust_forecast_mthly

# COMMAND ----------

# DBTITLE 1,dim_ship_po_header
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_po_header where coy_code = '999999'and order_no = '999999';
# MAGIC select * from test_gold.dw_procurement.dim_ship_invoice_header where order_no = 'Unknown';--coy_code = '999999'and order_no = '999999';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_account_shipsure`

# COMMAND ----------

# DBTITLE 1,dim_account_shipsure
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_account_shipsure where code like '%999999%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_contact_shipsure`

# COMMAND ----------

# DBTITLE 1,dim_contact_shipsure
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_contact_shipsure where code like '%999999%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_part`

# COMMAND ----------

# DBTITLE 1,dim_ship_part
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_part where code like '%999999%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_shipsure`

# COMMAND ----------

# DBTITLE 1,dim_ship_shipsure
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_shipsure where code like '%999999%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_component`

# COMMAND ----------

# DBTITLE 1,dim_ship_component
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_component where code like '%999999%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_procurement_item_type`

# COMMAND ----------

# DBTITLE 1,dim_procurement_item_type
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_procurement_item_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_procurement_status`

# COMMAND ----------

# DBTITLE 1,dim_procurement_status
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_procurement_status where code like'%TA%'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dw_market_data.dim_currency`

# COMMAND ----------

# DBTITLE 1,dw_market_data.dim_currency
# MAGIC %sql
# MAGIC select * from test_gold.dw_market_data.dim_currency where code like '%FVV%'

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Validations `FACT_SHIP_ENQUIRY_LINES`

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'purchorder' as table_ref, *--coy_id, ord_orderno, cmp_id, acc_id, ves_id, ptr_id, cur_id, ORD_Stage 
# MAGIC from purchorder where coy_id = '999999' and  (ORD_Stage = 'Enquiry' or ORD_Stage = 'Authorised Enquiry') --union
# MAGIC --select 'suporder' as table_ref, coy_id, ord_orderno, cmp_id, rsp_id, rsp_ordered, rsp_status, cur_id, '' as ORD_Stage from suporder where coy_id = '999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC --select 'orderline' as table_ref, * from orderline where coy_id = '999999' --union
# MAGIC select 'suporderline' as table_ref, coy_id, ord_orderno, cmp_id, sol_id, rod_id, rsp_id, cur_id from suporderline where coy_id = '999999';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC     select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
# MAGIC         PO.ord_dateExpectDelivery,  PO.ord_delivery,  PO.cmp_id,  PO.cur_id,   OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,
# MAGIC         PO.ord_amountCurr, PO.rsp_totalCost, OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
# MAGIC     from header PO inner join lines OL 
# MAGIC     on PO.coy_id = OL.coy_id  AND
# MAGIC         PO.ord_orderNo = OL.ord_orderNo  AND
# MAGIC         PO.rsp_id = OL.rsp_id
# MAGIC         where PO.coy_id='999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select 'purchorder' as table_ref, coy_id, ord_orderno, cmp_id, acc_id, ves_id, ptr_id, cur_id from purchorder where coy_id = '999999' union
# MAGIC -- select 'audit_purchorder' as table_ref, coy_id, ord_orderno, cmp_id, acc_id, ves_id, ptr_id, cur_id from audit_purchorder where coy_id = '999999' union
# MAGIC -- select 'suporder' as table_ref, coy_id, ord_orderno, cmp_id, rsp_id, rsp_ordered, rsp_status, cur_id from suporder where coy_id = '999999' union
# MAGIC -- select 'orderline' as table_ref, coy_id, ord_orderno, rod_id, rsp_id, viv_id, '0' as ptr_id, 'EL' as cur_id from orderline where coy_id = '999999' union
# MAGIC -- select 'suporderline' as table_ref, coy_id, ord_orderno, cmp_id, sol_id, rod_id, rsp_id, cur_id from suporderline where coy_id = '999999' union
# MAGIC -- select 'audit_suporder' as table_ref, coy_id, ord_orderno, cmp_id, rsp_id, rsp_status, updatetype, cur_id from audit_suporder where coy_id = '999999'
# MAGIC --'purchorder','audit_purchorder','suporder','orderline','suporderline', 'audit_suporder'
# MAGIC
# MAGIC -- select * from purchaseOrder where coy_id = '999999'
# MAGIC
# MAGIC with header as
# MAGIC (
# MAGIC     select distinct 
# MAGIC     PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated, PO.ord_dateEntered, PO.ord_dateExpectDelivery,  PO.ord_delivery , PO.ord_amountCurr, SO.cmp_id,  SO.cur_id,  SO.rsp_id, SO.rsp_totalCost
# MAGIC     from purchorder PO
# MAGIC     left join suporder SO on PO.coy_id = SO.coy_id  AND
# MAGIC         PO.ord_OrderNo = SO.ord_OrderNo
# MAGIC     where (PO.ORD_Stage = 'Enquiry' or PO.ORD_Stage = 'Authorised Enquiry' )
# MAGIC )
# MAGIC ,lines
# MAGIC as(
# MAGIC     select distinct OL.coy_id,  OL.ord_orderNo,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,  SOL.sol_id,  SOL.rsp_id, SOL.sol_unitPrice
# MAGIC     from orderline OL left join suporderline SOL on OL.rod_id = SOL.rod_id
# MAGIC )
# MAGIC ,headerlines as
# MAGIC (
# MAGIC     select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
# MAGIC         PO.ord_dateExpectDelivery,  PO.ord_delivery,  PO.cmp_id,  PO.cur_id,   OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,
# MAGIC         PO.ord_amountCurr, PO.rsp_totalCost, OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
# MAGIC     from header PO inner join lines OL 
# MAGIC     on PO.coy_id = OL.coy_id  AND
# MAGIC         PO.ord_orderNo = OL.ord_orderNo  AND
# MAGIC         PO.rsp_id = OL.rsp_id
# MAGIC     union all
# MAGIC     select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
# MAGIC         PO.ord_dateExpectDelivery,  PO.ord_delivery,  NULL as cmp_id,  NULL as cur_id,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,	   
# MAGIC         PO.ord_amountCurr, NULL AS rsp_totalCost,  OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
# MAGIC     from PurchOrder PO
# MAGIC     inner join lines OL on PO.coy_id = OL.coy_id  AND
# MAGIC         PO.ord_orderNo = OL.ord_orderNo  AND
# MAGIC         OL.rsp_id IS NULL
# MAGIC     where PO.ORD_Stage = 'Enquiry' or PO.ORD_Stage = 'Authorised Enquiry'
# MAGIC     union all
# MAGIC     select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
# MAGIC         PO.ord_dateExpectDelivery,  PO.ord_delivery, PO.cmp_id,  PO.cur_id,     NULL as rod_id,  NULL as rod_lineNo,  NULL as viv_id,  NULL as ROD_QuantityEnquired,
# MAGIC         PO.ord_amountCurr, PO.rsp_totalCost, NULL AS sol_id,  NULL AS rsp_id, NULL AS sol_unitPrice
# MAGIC     from header PO  where NOT EXISTS (select 1 from lines OL where PO.coy_id = OL.coy_id and PO.ord_orderNo = OL.ord_orderNo)
# MAGIC )
# MAGIC ,hist as
# MAGIC (
# MAGIC     Select DISTINCT au.coy_id,  au.ord_OrderNo,  au.ord_stage,  au.ord_status,  au.ord_dateStatus from audit_PurchOrder au where
# MAGIC     ord_stage in  ('Enquiry', 'Authorised Enquiry')   AND
# MAGIC     ord_status is not null   AND
# MAGIC     ord_dateStatus is not null
# MAGIC )
# MAGIC ,firstEnquryDate1 as
# MAGIC (
# MAGIC     select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo,cmp_id ORDER BY RSP_UpdatedON ASC) AS EnqrStageRowNum
# MAGIC     ,coy_id,ord_OrderNo, cmp_id, RSP_UpdatedON
# MAGIC     From AUDIT_SUPORDER where RSP_Status = 'TA'
# MAGIC )
# MAGIC ,firstEnquryDate2 as
# MAGIC (
# MAGIC     select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo ORDER BY ord_dateStatus ASC) AS EnqrStageRowNum
# MAGIC     ,coy_id,ord_OrderNo,ord_stage,ord_dateStatus
# MAGIC     From Audit_PurchOrder where ord_stage in ('Enquiry', 'Authorised Enquiry')
# MAGIC )
# MAGIC ,result as
# MAGIC (
# MAGIC     select PO.coy_id, 
# MAGIC         PO.ord_orderNo, 
# MAGIC         PO.acc_id, 
# MAGIC         PO.ves_id,  
# MAGIC         PO.ptr_id,  
# MAGIC         PO.ord_type, 
# MAGIC         PO.cmp_id, 
# MAGIC         PO.cur_id,
# MAGIC         PO.viv_id,  
# MAGIC         case when PO.ord_type like '%Service%' then 'S' else case when PO.ord_type like '%Material%' then 'M' else 'M' end end as proc_item_type_code
# MAGIC         ,ifnull(hist.ord_status,PO.ord_status) as proc_status
# MAGIC         ,ifnull(hist.ORD_Stage,PO.ORD_Stage) as proc_stage
# MAGIC         ,case when hist.ord_status is not null Then hist.ord_dateStatus else case when PO.ord_dateStatus is not null Then PO.ord_dateStatus else PO.ord_dateEntered end end as FVEL_ENQR_STATUS_DATE
# MAGIC         ,case when PO.ROD_ID is null then 1 else PO.ROD_LineNo end as fvel_enqr_line_no
# MAGIC         ,timestamp(date(PO.ord_dateOriginated)) as ord_dateOriginated_notime
# MAGIC         ,timestamp(date(PO.ord_dateEntered)) as ord_dateEntered_notime
# MAGIC         ,timestamp(date(PO.ord_dateExpectDelivery)) as ord_dateExpectDelivery_notime
# MAGIC         ,timestamp(date(PO.ord_delivery)) as ord_delivery_notime
# MAGIC         ,case when PO.rod_id IS NULL Then 1 else case when PO.rod_quantityRequested is null then 0 else PO.rod_quantityRequested end end as FVEL_ENQUIRY_QTY
# MAGIC         ,'CA' as region_code
# MAGIC         ,case when PO.rsp_id IS NOT NULL then case when PO.sol_unitPrice IS NOT NULL then PO.sol_unitPrice else 0 end else case when PO.ord_type like 'Service%' then case when PO.ord_amountCurr IS NOT NULL then PO.ord_amountCurr
# MAGIC         else 0 end end end AS FVEL_UNIT_PRICE_AMT
# MAGIC         ,case when PO.rsp_totalCost IS NOT NULL then PO.rsp_totalCost else 0 end AS FVEL_ENQUIRY_SUPPLIER_AMT
# MAGIC         ,timestamp(date(Coalesce(Coalesce(d1.RSP_UpdatedON, d2.ord_dateStatus), PO.ord_dateEntered))) as firstEnquryDate
# MAGIC         ,min(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as min_cur
# MAGIC         ,max(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as max_cur
# MAGIC     from headerlines PO
# MAGIC         left join firstEnquryDate1 d1 on PO.COY_ID=d1.COY_ID and PO.ORD_OrderNo=d1.ORD_OrderNo and PO.cmp_id = d1.cmp_id and d1.EnqrStageRowNum=1
# MAGIC         left join firstEnquryDate2 d2 on PO.COY_ID=d2.COY_ID and PO.ORD_OrderNo=d2.ORD_OrderNo and d2.EnqrStageRowNum=1
# MAGIC         left join hist on
# MAGIC         PO.coy_id = hist.coy_id   AND 
# MAGIC         PO.ord_orderNo = hist.ord_orderNo  AND
# MAGIC         PO.ord_stage = hist.ord_stage
# MAGIC )
# MAGIC select * from result where coy_id='999999';

# COMMAND ----------

# DBTITLE 1,Notebook to generate the fact
from functools import reduce
from pyspark.sql import (DataFrame, 
                         SparkSession,)

for table_name in ['purchorder','audit_purchorder','suporder','orderline','suporderline', 'audit_suporder']:
    spark.sql(f"""
        with lookup as (select distinct COY_ID, ORD_OrderNo 
        from test_silver.shipsure.po_delta
        where src_syst_effective_to is null
        )
        select a.* from  test_silver.shipsure.{table_name} a 
        join lookup b on a.coy_id = b.coy_id and a.ord_orderno = b.ord_orderno
        where src_syst_effective_to is null
    """).createOrReplaceTempView(table_name)

df_purchorder = spark.sql(f"""
            with header
            as
            (
                select distinct 
                PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered, 
                PO.ord_dateExpectDelivery,  PO.ord_delivery , PO.ord_amountCurr, SO.cmp_id,  SO.cur_id,  SO.rsp_id, SO.rsp_totalCost
                from purchorder PO
                left join suporder SO on PO.coy_id = SO.coy_id  AND
                    PO.ord_OrderNo = SO.ord_OrderNo
                where (PO.ORD_Stage = 'Enquiry' or PO.ORD_Stage = 'Authorised Enquiry' )
            )
            ,lines
            as(
                select distinct OL.coy_id,  OL.ord_orderNo,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,  SOL.sol_id,  SOL.rsp_id, SOL.sol_unitPrice
                from orderline OL left join suporderline SOL on OL.rod_id = SOL.rod_id
            )
            ,headerlines
            as
            (
            select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery,  PO.cmp_id,  PO.cur_id,   OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,
                PO.ord_amountCurr, PO.rsp_totalCost, OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
            from header PO inner join lines OL 
            on PO.coy_id = OL.coy_id  AND
                PO.ord_orderNo = OL.ord_orderNo  AND
                PO.rsp_id = OL.rsp_id
            union all
            select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery,  NULL as cmp_id,  NULL as cur_id,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,	   
                PO.ord_amountCurr, NULL AS rsp_totalCost,  OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
            from PurchOrder PO
            inner join lines OL on PO.coy_id = OL.coy_id  AND
                PO.ord_orderNo = OL.ord_orderNo  AND
                OL.rsp_id IS NULL
            where PO.ORD_Stage = 'Enquiry' or PO.ORD_Stage = 'Authorised Enquiry'
            union all
            select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery, PO.cmp_id,  PO.cur_id,     NULL as rod_id,  NULL as rod_lineNo,  NULL as viv_id,  NULL as ROD_QuantityEnquired,
                PO.ord_amountCurr, PO.rsp_totalCost, NULL AS sol_id,  NULL AS rsp_id, NULL AS sol_unitPrice
            from header PO  where NOT EXISTS (select 1 from lines OL where PO.coy_id = OL.coy_id and PO.ord_orderNo = OL.ord_orderNo)
            )
            ,hist
            as
            (
            Select DISTINCT au.coy_id,  au.ord_OrderNo,  au.ord_stage,  au.ord_status,  au.ord_dateStatus from audit_PurchOrder au where
                ord_stage in  ('Enquiry', 'Authorised Enquiry')   AND
                ord_status is not null   AND
                ord_dateStatus is not null
            )
            ,firstEnquryDate1
            as
            (
            select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo,cmp_id ORDER BY RSP_UpdatedON ASC) AS EnqrStageRowNum
            ,coy_id,ord_OrderNo, cmp_id, RSP_UpdatedON
            From AUDIT_SUPORDER where RSP_Status = 'TA'
            )
            ,firstEnquryDate2
            as
            (
            select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo ORDER BY ord_dateStatus ASC) AS EnqrStageRowNum
            ,coy_id,ord_OrderNo,ord_stage,ord_dateStatus
            From Audit_PurchOrder where ord_stage in ('Enquiry', 'Authorised Enquiry')
            )
            ,result
            as
            (select PO.coy_id, 
            PO.ord_orderNo, 
            PO.acc_id, 
            PO.ves_id,  
            PO.ptr_id,  
            PO.ord_type, 
            PO.cmp_id, 
            PO.cur_id,
            PO.viv_id,  
            case when PO.ord_type like '%Service%' then 'S' else case when PO.ord_type like '%Material%' then 'M' else 'M' end end as proc_item_type_code
            ,ifnull(hist.ord_status,PO.ord_status) as proc_status
            ,ifnull(hist.ORD_Stage,PO.ORD_Stage) as proc_stage
            ,case when hist.ord_status is not null Then hist.ord_dateStatus else case when PO.ord_dateStatus is not null Then PO.ord_dateStatus else PO.ord_dateEntered end end as FVEL_ENQR_STATUS_DATE
            ,case when PO.ROD_ID is null then 1 else PO.ROD_LineNo end as fvel_enqr_line_no
            ,timestamp(date(PO.ord_dateOriginated)) as ord_dateOriginated_notime
            ,timestamp(date(PO.ord_dateEntered)) as ord_dateEntered_notime
            ,timestamp(date(PO.ord_dateExpectDelivery)) as ord_dateExpectDelivery_notime
            ,timestamp(date(PO.ord_delivery)) as ord_delivery_notime
            ,case when PO.rod_id IS NULL Then 1 else case when PO.rod_quantityRequested is null then 0 else PO.rod_quantityRequested end end as FVEL_ENQUIRY_QTY
            ,'CA' as region_code
            ,case when PO.rsp_id IS NOT NULL then case when PO.sol_unitPrice IS NOT NULL then PO.sol_unitPrice else 0 end else case when PO.ord_type like 'Service%' then case when PO.ord_amountCurr IS NOT NULL then PO.ord_amountCurr
            else 0 end end end AS FVEL_UNIT_PRICE_AMT
            ,case when PO.rsp_totalCost IS NOT NULL then PO.rsp_totalCost else 0 end AS FVEL_ENQUIRY_SUPPLIER_AMT
            ,timestamp(date(Coalesce(Coalesce(d1.RSP_UpdatedON, d2.ord_dateStatus), PO.ord_dateEntered))) as firstEnquryDate
            ,min(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as min_cur
            ,max(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as max_cur
            from headerlines PO
                left join firstEnquryDate1 d1 on PO.COY_ID=d1.COY_ID and PO.ORD_OrderNo=d1.ORD_OrderNo and PO.cmp_id = d1.cmp_id and d1.EnqrStageRowNum=1
                left join firstEnquryDate2 d2 on PO.COY_ID=d2.COY_ID and PO.ORD_OrderNo=d2.ORD_OrderNo and d2.EnqrStageRowNum=1
                left join hist on
                PO.coy_id = hist.coy_id   AND 
                PO.ord_orderNo = hist.ord_orderNo  AND
                PO.ord_stage = hist.ord_stage
            )
    select * from result where ifnull(min_cur,'')=ifnull(max_cur,'')
""")
# df_purchorder.createOrReplaceTempView("purchaseOrder")

df_purchorder_enquiry = spark.sql(f"""                            
            with audit_purchorder_cte AS ( --DS1
            SELECT DISTINCT
            AP.coy_id, 
            AP.ord_OrderNo, 
            AP.ACC_ID ,
            AP.VES_ID , 
            P.ORD_Type, 
            AP.ord_stage,
            AP.ord_status,
            AP.ord_dateStatus, 
            P.ORD_DateExpectDelivery AS RequestedDeliveryDate, 
            P.ORD_Delivery AS ExpectedDeliveryDate, 
            P.ORD_DateReceived,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.ord_stage, AP.ord_status ORDER BY AP.ord_dateStatus ASC) AS StatusRowNum,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.ord_stage ORDER BY AP.ord_dateStatus ASC, AP.UpdateDate ASC) AS StageRowNum  
            FROM
            AUDIT_PURCHORDER AP
            INNER JOIN PurchOrder P
                ON AP.coy_id = P.coy_id AND AP.ord_OrderNo = P.ord_OrderNo AND AP.ACC_ID = P.ACC_ID AND AP.VES_ID=P.VES_ID
            WHERE
            AP.ord_dateStatus IS NOT NULL AND AP.ord_status IS NOT NULL AND AP.ord_stage IN ('Purchase Order')
            --and AP.COY_ID='3400' and AP.ORD_OrderNo='02701'   
            )
            ,header as (
                select distinct 
                    PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered, 
                    PO.ord_dateExpectDelivery,  PO.ord_delivery , PO.ord_amountCurr, SO.cmp_id,  SO.cur_id,  SO.rsp_id, SO.rsp_totalCost
                from PurchOrder PO
                    left join SupOrder SO on PO.coy_id = SO.coy_id  AND PO.ord_OrderNo = SO.ord_OrderNo
                    inner join audit_purchorder_cte op on op.coy_id=PO.coy_id and op.ord_orderNo=PO.ord_orderNo and PO.acc_id=op.acc_id and PO.ves_id = op.ves_id and StageRowNum=1
                    where (PO.ORD_Stage = 'Purchase Order') 
                    and op.ord_dateStatus >= dateadd(hour, -17 ,'1900-01-01 00:00:00.00')
            )
            ,lines as
            (select distinct OL.coy_id,  OL.ord_orderNo,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,  SOL.sol_id,  SOL.rsp_id, SOL.sol_unitPrice from OrderLine OL left join SupOrderLine SOL on OL.rod_id = SOL.rod_id
            )
            ,headerlines as
            (
                select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                    PO.ord_dateExpectDelivery,  PO.ord_delivery,  PO.cmp_id,  PO.cur_id,   OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,
                    PO.ord_amountCurr, PO.rsp_totalCost, OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
                from header PO inner join lines OL 
                on PO.coy_id = OL.coy_id  AND
                    PO.ord_orderNo = OL.ord_orderNo  AND
                    PO.rsp_id = OL.rsp_id
                union all
                select 
                PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                    PO.ord_dateExpectDelivery,  PO.ord_delivery,  NULL as cmp_id,  NULL as cur_id,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,	   
                    PO.ord_amountCurr, NULL AS rsp_totalCost,  OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
                from PurchOrder PO
                inner join lines OL on PO.coy_id = OL.coy_id  AND
                    PO.ord_orderNo = OL.ord_orderNo  AND
                    OL.rsp_id IS NULL
                inner join audit_purchorder_cte op on op.coy_id=PO.coy_id and op.ord_orderNo=PO.ord_orderNo and PO.acc_id=op.acc_id and PO.ves_id = op.ves_id and StageRowNum=1
                where (PO.ORD_Stage = 'Purchase Order') 
                and op.ord_dateStatus >= dateadd(hour, -17 ,'1900-01-01 00:00:00.00')
                union all
                select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                    PO.ord_dateExpectDelivery,  PO.ord_delivery, PO.cmp_id,  PO.cur_id,     NULL as rod_id,  NULL as rod_lineNo,  NULL as viv_id,  NULL as ROD_QuantityEnquired,
                    PO.ord_amountCurr, PO.rsp_totalCost, NULL AS sol_id,  NULL AS rsp_id, NULL AS sol_unitPrice
                from header PO  where NOT EXISTS (select 1 from lines OL where PO.coy_id = OL.coy_id and PO.ord_orderNo = OL.ord_orderNo)
            )
            ,hist as (
                Select DISTINCT au.coy_id,  au.ord_OrderNo,  au.ord_stage,  au.ord_status,  au.ord_dateStatus from Audit_PurchOrder au 
                where ord_stage in  ('Enquiry') AND ord_status is not null AND ord_dateStatus is not null
            )
            ,firstEnquryDate1 as (
                select ROW_NUMBER() OVER(PARTITION BY coy_id, ord_OrderNo, cmp_id ORDER BY RSP_UpdatedON ASC) AS EnqrStageRowNum, coy_id, ord_OrderNo, 
                cmp_id, RSP_UpdatedON
                From AUDIT_SUPORDER where RSP_Status = 'TA'
            )
            ,firstEnquryDate2 as (select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo ORDER BY ord_dateStatus ASC) AS EnqrStageRowNum
                ,coy_id,ord_OrderNo,ord_stage,ord_dateStatus
                From Audit_PurchOrder where ord_stage in ('Enquiry')
            )
            ,result as (select PO.coy_id, 
                PO.ord_orderNo, 
                PO.acc_id, 
                PO.ves_id,  
                PO.ptr_id,  
                PO.ord_type,  
                PO.cmp_id, 
                PO.cur_id,
                PO.viv_id,  
                case when PO.ord_type like '%Service%' then 'S' else case when PO.ord_type like '%Material%' then 'M' else 'M' end end as proc_item_type_code
                ,ifnull(hist.ord_status,'TA') as proc_status
                ,ifnull(hist.ORD_Stage,'Enquiry') as proc_stage
                ,case when hist.ord_status is not null Then hist.ord_dateStatus else case when PO.ord_dateStatus is not null Then PO.ord_dateStatus else PO.ord_dateEntered end end as FVEL_ENQR_STATUS_DATE
                ,case when PO.ROD_ID is null then 1 else PO.ROD_LineNo end as fvel_enqr_line_no
                ,timestamp(date(PO.ord_dateOriginated)) as ord_dateOriginated_notime
                ,timestamp(date(PO.ord_dateEntered)) as ord_dateEntered_notime
                ,timestamp(date(PO.ord_dateExpectDelivery)) as ord_dateExpectDelivery_notime
                ,timestamp(date(PO.ord_delivery)) as ord_delivery_notime
                ,case when PO.rod_id IS NULL Then 1 else case when PO.rod_quantityRequested is null then 0 else PO.rod_quantityRequested end end as FVEL_ENQUIRY_QTY
                ,'CA' as region_code
                ,case when PO.rsp_id IS NOT NULL then case when PO.sol_unitPrice IS NOT NULL then PO.sol_unitPrice else 0 end else case when PO.ord_type like 'Service%' then case when PO.ord_amountCurr IS NOT NULL then PO.ord_amountCurr
                else 0 end end end AS FVEL_UNIT_PRICE_AMT
                ,case when PO.rsp_totalCost IS NOT NULL then PO.rsp_totalCost else 0 end AS FVEL_ENQUIRY_SUPPLIER_AMT
                ,timestamp(date(Coalesce(Coalesce(d1.RSP_UpdatedON, d2.ord_dateStatus), PO.ord_dateEntered))) as firstEnquryDate
                ,min(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as min_cur
                ,max(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as max_cur
                from headerlines PO
                    left join firstEnquryDate1 d1 on PO.COY_ID=d1.COY_ID and PO.ORD_OrderNo=d1.ORD_OrderNo and PO.cmp_id = d1.cmp_id and d1.EnqrStageRowNum=1
                    left join firstEnquryDate2 d2 on PO.COY_ID=d2.COY_ID and PO.ORD_OrderNo=d2.ORD_OrderNo and d2.EnqrStageRowNum=1
                    left join hist on PO.coy_id = hist.coy_id AND PO.ord_orderNo = hist.ord_orderNo  
            )
        select distinct * from result where ifnull(min_cur,'')=ifnull(max_cur,'')
""")
# .createOrReplaceTempView("purchorder_enquiry")

spark.sql(f"""
    select * 
        EXCEPT (
            ship_orig_code,
            ship_sk, 
            ship_code,
            ship_name,
            ship_to_comp_name,
            ship_to_addr,
            ship_to_city_name,
            ship_to_state_name,
            ship_to_post_code,
            ship_to_country_name,
            ship_to_phone,
            ship_to_email
        ),
        ship_orig_code as vessel_orig_code,
        ship_sk as vessel_sk,
        ship_code as vessel_code,
        ship_name as vessel_name,
        ship_to_comp_name as vessel_to_comp_name,
        ship_to_addr as vessel_to_addr,
        ship_to_city_name as vessel_to_city_name,
        ship_to_state_name as vessel_to_state_name,
        ship_to_post_code as vessel_to_post_code,
        ship_to_country_name as vessel_to_country_name,
        ship_to_phone as vessel_to_phone,
        ship_to_email as vessel_to_email
    from test_gold.dw_procurement.dim_ship_po_header
""").createOrReplaceTempView("DIM_VESSEL_PO_HEADER")

spark.sql(f"""
    select * from test_gold.dw_procurement.dim_contact_shipsure
""").createOrReplaceTempView("DIM_CONTACT_SHIPSURE")

df_purchorder_authorized = spark.sql("""
            with audit_purchorder_aux AS ( --DS1
            SELECT DISTINCT
            AP.coy_id, 
            AP.ord_OrderNo, 
            AP.ACC_ID ,
            AP.VES_ID , 
            P.ORD_Type, 
            AP.ord_stage,
            AP.ord_status,
            AP.ord_dateStatus, 
            P.ORD_DateExpectDelivery AS RequestedDeliveryDate, 
            P.ORD_Delivery AS ExpectedDeliveryDate, 
            P.ORD_DateReceived,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.ord_stage, AP.ord_status ORDER BY AP.ord_dateStatus ASC) AS StatusRowNum,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.ord_stage ORDER BY AP.ord_dateStatus ASC, AP.UpdateDate ASC) AS StageRowNum  
            FROM
            AUDIT_PURCHORDER AP
            INNER JOIN PurchOrder P
                ON AP.coy_id = P.coy_id AND AP.ord_OrderNo = P.ord_OrderNo AND AP.ACC_ID = P.ACC_ID AND AP.VES_ID=P.VES_ID
            WHERE
            AP.ord_dateStatus IS NOT NULL AND AP.ord_status IS NOT NULL AND AP.ord_stage IN ('Purchase Order')
            --and AP.COY_ID='3400' and AP.ORD_OrderNo='02701'   
            )
            ,header
            as
            (
            select distinct 
                PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered, 
                PO.ord_dateExpectDelivery,  PO.ord_delivery , PO.ord_amountCurr, SO.cmp_id,  SO.cur_id,  SO.rsp_id, SO.rsp_totalCost
            from PurchOrder PO
            left join SupOrder SO on PO.coy_id = SO.coy_id  AND
                PO.ord_OrderNo = SO.ord_OrderNo
            inner join DIM_VESSEL_PO_HEADER ph on PO.COY_ID = ph.COY_CODE and po.ORD_OrderNo= ph.ORDER_NO 
            inner join DIM_CONTACT_SHIPSURE cs on cs.CODE = po.CMP_ID and cs.SK = ph.SUPPLIER_SK
            inner join audit_purchorder_aux op on op.coy_id=PO.coy_id and op.ord_orderNo=PO.ord_orderNo and PO.acc_id=op.acc_id and PO.ves_id = op.ves_id and StageRowNum=1
            where (PO.ORD_Stage = 'Purchase Order' ) 
            and op.ord_dateStatus >= dateadd(hour, -17 ,'1900-01-01 00:00:00.00')
            )
            ,lines
            as(
            select distinct OL.coy_id,  OL.ord_orderNo,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,  SOL.sol_id,  SOL.rsp_id, SOL.sol_unitPrice
            from OrderLine OL left join SupOrderLine SOL on OL.rod_id = SOL.rod_id
            --where coy_id='3667' and ord_orderNo='02132'
            )
            ,headerlines
            as
            (
            select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery,  PO.cmp_id,  PO.cur_id,   OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,
                PO.ord_amountCurr, PO.rsp_totalCost, OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
            from header PO inner join lines OL 
            on PO.coy_id = OL.coy_id  AND
                PO.ord_orderNo = OL.ord_orderNo  AND
                PO.rsp_id = OL.rsp_id
            union all
            select 
            PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery,  NULL as cmp_id,  NULL as cur_id,  OL.rod_id,  OL.rod_lineNo,  OL.viv_id,  OL.rod_quantityRequested,	   
                PO.ord_amountCurr, NULL AS rsp_totalCost,  OL.sol_id,  OL.rsp_id, OL.sol_unitPrice
            from PurchOrder PO
            inner join lines OL on PO.coy_id = OL.coy_id  AND
                PO.ord_orderNo = OL.ord_orderNo  AND
                OL.rsp_id IS NULL
            inner join DIM_VESSEL_PO_HEADER ph on PO.COY_ID = ph.COY_CODE and po.ORD_OrderNo= ph.ORDER_NO 
            inner join DIM_CONTACT_SHIPSURE cs on cs.CODE = po.CMP_ID and cs.SK = ph.SUPPLIER_SK
            inner join audit_purchorder_aux op on op.coy_id=PO.coy_id and op.ord_orderNo=PO.ord_orderNo and PO.acc_id=op.acc_id and PO.ves_id = op.ves_id and StageRowNum=1
            where (PO.ORD_Stage = 'Purchase Order' ) 
            and op.ord_dateStatus >= dateadd(hour, -17 ,'1900-01-01 00:00:00.00')
            union all
            select PO.coy_id,  PO.ord_orderNo,  PO.acc_id,  PO.ves_id,  PO.ptr_id,  PO.ord_type,  PO.ord_stage,  PO.ord_status,  PO.ord_dateStatus,  PO.ord_dateOriginated,  PO.ord_dateEntered,  
                PO.ord_dateExpectDelivery,  PO.ord_delivery, PO.cmp_id,  PO.cur_id,     NULL as rod_id,  NULL as rod_lineNo,  NULL as viv_id,  NULL as ROD_QuantityEnquired,
                PO.ord_amountCurr, PO.rsp_totalCost, NULL AS sol_id,  NULL AS rsp_id, NULL AS sol_unitPrice
            from header PO  where NOT EXISTS (select 1 from lines OL where PO.coy_id = OL.coy_id and PO.ord_orderNo = OL.ord_orderNo)
            )
            ,hist
            as
            (
            Select DISTINCT au.coy_id,  au.ord_OrderNo,  au.ord_stage,  au.ord_status,  au.ord_dateStatus from Audit_PurchOrder au where
                ord_stage in  ('Authorised Enquiry')   AND
                ord_status is not null   AND
                ord_dateStatus is not null
            )
            ,firstEnquryDate1
            as
            (
            select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo,cmp_id ORDER BY RSP_UpdatedON ASC) AS EnqrStageRowNum
            ,coy_id,ord_OrderNo, cmp_id, RSP_UpdatedON
            From AUDIT_SUPORDER where RSP_Status = 'TA'
            )
            ,firstEnquryDate2
            as
            (select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo ORDER BY ord_dateStatus ASC) AS EnqrStageRowNum
            ,coy_id,ord_OrderNo,ord_stage,ord_dateStatus
            From Audit_PurchOrder where ord_stage in ('Authorised Enquiry')
            )
            ,result
            as
            (select PO.coy_id, 
            PO.ord_orderNo, 
            PO.acc_id, 
            PO.ves_id,  
            PO.ptr_id,  
            PO.ord_type,  
            PO.cmp_id, 
            PO.cur_id,
            PO.viv_id,  
            case when PO.ord_type like '%Service%' then 'S' else case when PO.ord_type like '%Material%' then 'M' else 'M' end end as proc_item_type_code
            ,ifnull(hist.ord_status,'TR') as proc_status
            ,ifnull(hist.ORD_Stage,'Authorised Enquiry') as proc_stage
            ,case when hist.ord_status is not null Then hist.ord_dateStatus else case when PO.ord_dateStatus is not null Then PO.ord_dateStatus else PO.ord_dateEntered end end as FVEL_ENQR_STATUS_DATE
            ,case when PO.ROD_ID is null then 1 else PO.ROD_LineNo end as fvel_enqr_line_no
            ,timestamp(date(PO.ord_dateOriginated)) as ord_dateOriginated_notime
            ,timestamp(date(PO.ord_dateEntered)) as ord_dateEntered_notime
            ,timestamp(date(PO.ord_dateExpectDelivery)) as ord_dateExpectDelivery_notime
            ,timestamp(date(PO.ord_delivery)) as ord_delivery_notime
            ,case when PO.rod_id IS NULL Then 1 else case when PO.rod_quantityRequested is null then 0 else PO.rod_quantityRequested end end as FVEL_ENQUIRY_QTY
            ,'CA' as region_code
            ,case when PO.rsp_id IS NOT NULL then case when PO.sol_unitPrice IS NOT NULL then PO.sol_unitPrice else 0 end else case when PO.ord_type like 'Service%' then case when PO.ord_amountCurr IS NOT NULL then PO.ord_amountCurr
            else 0 end end end AS FVEL_UNIT_PRICE_AMT
            ,case when PO.rsp_totalCost IS NOT NULL then PO.rsp_totalCost else 0 end AS FVEL_ENQUIRY_SUPPLIER_AMT
            ,timestamp(date(Coalesce(Coalesce(d1.RSP_UpdatedON, d2.ord_dateStatus), PO.ord_dateEntered))) as firstEnquryDate
            ,min(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as min_cur
            ,max(PO.cur_id) over (partition by PO.coy_id, PO.ord_orderNo, PO.cmp_id) as max_cur
            from headerlines PO
            inner join DIM_VESSEL_PO_HEADER ph on PO.COY_ID = ph.COY_CODE and po.ORD_OrderNo= ph.ORDER_NO 
            inner join DIM_CONTACT_SHIPSURE cs on cs.CODE = po.CMP_ID and cs.SK = ph.SUPPLIER_SK
            left join firstEnquryDate1 d1 on PO.COY_ID=d1.COY_ID and PO.ORD_OrderNo=d1.ORD_OrderNo and PO.cmp_id = d1.cmp_id and d1.EnqrStageRowNum=1
            left join firstEnquryDate2 d2 on PO.COY_ID=d2.COY_ID and PO.ORD_OrderNo=d2.ORD_OrderNo and d2.EnqrStageRowNum=1
            left join hist on
            PO.coy_id = hist.coy_id   AND 
            PO.ord_orderNo = hist.ord_orderNo  
            )
            select distinct * from result where ifnull(min_cur,'')=ifnull(max_cur,'')                                   
        """)

df_stg = reduce(DataFrame.unionAll, [df_purchorder, df_purchorder_enquiry, df_purchorder_authorized]).drop('min_cur', 'max_cur')

df_stg = df_stg.orderBy('coy_id', 'ord_orderNo', 'acc_id', 'ves_id', 'ptr_id', 
                        'ord_type', 'cmp_id', 'cur_id', 'viv_id', 'proc_item_type_code',
                        'proc_status', 'ord_dateOriginated_notime', 'ord_dateEntered_notime', 
                        'ord_dateExpectDelivery_notime', 'ord_delivery_notime', 'region_code',
                        'FVEL_UNIT_PRICE_AMT', 'FVEL_ENQUIRY_SUPPLIER_AMT', 'FVEL_ENQUIRY_QTY', 
                        'FVEL_ENQR_STATUS_DATE', 'fvel_enqr_line_no', 'proc_stage', 'firstEnquryDate').dropDuplicates()

df_stg.createOrReplaceTempView("stg")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,Check STG  result
# MAGIC %sql
# MAGIC select * from stg where coy_id = '999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH CTE_SHIP_PO_HEADER AS (
# MAGIC     select 
# MAGIC         SK, 
# MAGIC         COY_CODE, 
# MAGIC         ORDER_NO, 
# MAGIC         AMT 
# MAGIC     from test_gold.dw_procurement.dim_ship_po_header
# MAGIC ),
# MAGIC CTE_SHIP_PART AS (
# MAGIC     -- 364
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_part
# MAGIC ),
# MAGIC CTE_SHIP_SHIPSURE AS (
# MAGIC     -- 43
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_shipsure
# MAGIC ),
# MAGIC CTE_SHIP_COMPONENT AS (
# MAGIC     -- 79_218
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_component
# MAGIC ),
# MAGIC CTE_ACCOUNT_SHIPSURE AS (
# MAGIC     -- 1114
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.DIM_ACCOUNT_SHIPSURE
# MAGIC ),
# MAGIC CTE_PROCUREMENT_ITEM_TYPE AS (
# MAGIC     -- 4
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.DIM_PROCUREMENT_ITEM_TYPE
# MAGIC ),
# MAGIC CTE_PROCUREMENT_STATUS AS (
# MAGIC     -- 59
# MAGIC     select
# MAGIC         sk,
# MAGIC         code, 
# MAGIC         name,
# MAGIC         stage_name
# MAGIC     from test_gold.dw_procurement.dim_procurement_status
# MAGIC ),
# MAGIC CTE_CONTACT_SHIPSURE AS (
# MAGIC     -- 78713
# MAGIC     select 
# MAGIC         sk, code
# MAGIC     from test_gold.dw_procurement.DIM_CONTACT_SHIPSURE
# MAGIC ),
# MAGIC CTE_CURRENCY AS (
# MAGIC     -- 174
# MAGIC     select 
# MAGIC         sk, code 
# MAGIC     from test_gold.dw_conformed.DIM_CURRENCY
# MAGIC ),
# MAGIC CTE_TIME_CREATED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_ENTERED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_REQUESTED_DELIVERED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_EXPECTED_DELIVERY AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_DIVISION AS (
# MAGIC     -- 8
# MAGIC     select 
# MAGIC         sk, code 
# MAGIC     from test_gold.dw_conformed.dim_region
# MAGIC ),
# MAGIC
# MAGIC -- STEP 21: Lookup transformation
# MAGIC CTE_DATE_TMP AS(
# MAGIC select 
# MAGIC     date_sk
# MAGIC     ,date_dt
# MAGIC     ,LAST_DAY(ADD_MONTHS(date_dt, -1)) AS LAST_DAY_PREV_MONTH
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_DATE AS(
# MAGIC     select 
# MAGIC         CTE_DATE_TMP.date_sk
# MAGIC         ,CTE_DATE_TMP.date_dt
# MAGIC         ,CTE_DATE_TMP.last_day_prev_month
# MAGIC         ,DT.date_sk as LAST_DAY_PREV_MONTH_SK
# MAGIC     from 
# MAGIC         CTE_DATE_TMP
# MAGIC         join test_gold.dw_conformed.dim_time as DT on CTE_DATE_TMP.last_day_prev_month = DT.date_dt
# MAGIC ),
# MAGIC -- STEP 22 + STEP 23
# MAGIC CTE_ALL_CURRENCY_TMP AS(
# MAGIC     select 
# MAGIC         FC.FROM_CURRENCY_SK,
# MAGIC         DT.CAL_MONTH_END_DAY,
# MAGIC         FC.CURRENCY_RATE,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY FC.from_currency_sk, DT.CAL_MONTH_KEY_NUM ORDER BY DT.DATE_SK DESC) AS ROW_VERSN
# MAGIC     from 
# MAGIC         test_gold.dw_market_data.FACT_CURRENCY_CONVERSION AS FC
# MAGIC         JOIN test_gold.dw_conformed.dim_currency as CAD on FC.TO_CURRENCY_SK = CAD.SK and CAD.code = 'CAD'
# MAGIC         JOIN test_gold.dw_conformed.dim_time as DT on FC.currency_date_sk = dt.DATE_SK
# MAGIC ),
# MAGIC CTE_ALL_CURRENCY AS(
# MAGIC     -- 33
# MAGIC     -- Return only the last rate found for each month and assign it the last calendar day of the month
# MAGIC     select 
# MAGIC         FC.CAL_MONTH_END_DAY as CURRENCY_DATE_SK
# MAGIC         ,CUR.code
# MAGIC         -- ,FC.CURRENCY_RATE
# MAGIC         ,fc.CURRENCY_RATE
# MAGIC     from 
# MAGIC         CTE_ALL_CURRENCY_TMP as FC
# MAGIC         JOIN test_gold.dw_conformed.dim_currency as CUR on FC.from_currency_sk = CUR.sk
# MAGIC     where ROW_VERSN = 1
# MAGIC     order by code
# MAGIC )
# MAGIC --select * from CTE_SHIP_PO_HEADER where COY_CODE='999999';
# MAGIC --select * from CTE_SHIP_PART where code LIKE '%999999%';
# MAGIC --select * from CTE_SHIP_SHIPSURE where code LIKE '%999999%';
# MAGIC --select * from CTE_SHIP_COMPONENT where code LIKE '%999999%';
# MAGIC --select * from CTE_ACCOUNT_SHIPSURE where code LIKE '%999999%';
# MAGIC --select * from CTE_CONTACT_SHIPSURE where code LIKE '%999999%';
# MAGIC --select * from CTE_CURRENCY where code LIKE '%BRCL%';
# MAGIC SELECT
# MAGIC         stg.cmp_id,
# MAGIC         COALESCE(vpo.sk, -1) AS ship_po_sk,
# MAGIC         CASE WHEN UPPER(ord_type) = 'SERVICES' THEN -3 ELSE COALESCE(vp.sk, -1) END AS ship_part_sk,
# MAGIC         COALESCE(vc.sk, -1) AS ship_comp_sk,
# MAGIC         COALESCE(vs.sk, -1) AS ship_sk,
# MAGIC         div.sk AS region_sk,
# MAGIC         COALESCE(ct.sk, -1) AS supplier_sk,
# MAGIC         COALESCE(acc.sk, -1) AS account_proc_sk,
# MAGIC         COALESCE(pit.sk, -1) AS proc_item_type_sk,
# MAGIC         int(COALESCE(stg.fvel_enqr_line_no, 0)) AS enqr_line_no,
# MAGIC         COALESCE(ps.sk, -1) AS enqr_status_sk,
# MAGIC         stg.FVEL_ENQR_STATUS_DATE AS enqr_status_date,
# MAGIC         COALESCE(cur.sk, -1) AS currency_sk,
# MAGIC         COALESCE(dtOrig.date_sk, 19750101) AS reqs_created_date_sk,
# MAGIC         COALESCE(dtEntered.date_sk, 19750101) AS reqs_entrd_date_sk,
# MAGIC         COALESCE(if(dtDelivery.date_sk < 19750101, 19750101,dtDelivery.date_sk), 19750101) AS expected_delivery_date_sk,
# MAGIC         COALESCE(if(dtExpectDelivery.date_sk < 19750101, 19750101,dtExpectDelivery.date_sk), 19750101) AS requested_delivery_date_sk,
# MAGIC         cast(COALESCE(stg.FVEL_UNIT_PRICE_AMT, 0)as decimal(16,4)) AS unit_price_amt,
# MAGIC         cast(COALESCE(stg.FVEL_UNIT_PRICE_AMT, 0) * if
# MAGIC                                                         (
# MAGIC                                                         stg.cur_id == "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                         1, --if 
# MAGIC                                                         if
# MAGIC                                                             ( --else
# MAGIC                                                             stg.cur_id != "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                             0, ifnull(ac.CURRENCY_RATE,0)
# MAGIC                                                             ) 
# MAGIC                                                         )as decimal(16,4)) AS unit_price_cad_amt,
# MAGIC         int(stg.FVEL_ENQUIRY_QTY) AS enquiry_qty,
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_QTY * FVEL_UNIT_PRICE_AMT, 0)as decimal(16,4)) AS enquiry_amt,
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_QTY * UNIT_PRICE_CAD_AMT, 0)as decimal(16,4)) AS enquiry_cad_amt,
# MAGIC         cast(stg.FVEL_ENQUIRY_SUPPLIER_AMT as decimal(16,4)) AS enquiry_supplier_amt,
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_SUPPLIER_AMT, 0) * if
# MAGIC                                                         (
# MAGIC                                                         stg.cur_id == "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                         1, --if 
# MAGIC                                                         if
# MAGIC                                                             ( --else
# MAGIC                                                             stg.cur_id != "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                             0, ifnull(ac.CURRENCY_RATE,0)
# MAGIC                                                             ) 
# MAGIC                                                         )as decimal(16,4)) AS enquiry_supplier_cad_amt
# MAGIC         ,monotonically_increasing_id() as idx
# MAGIC     FROM 
# MAGIC         stg stg
# MAGIC         LEFT JOIN CTE_SHIP_PO_HEADER vpo ON stg.coy_id = vpo.coy_code
# MAGIC                                         AND stg.ord_orderNo = vpo.order_no 
# MAGIC         LEFT JOIN CTE_SHIP_PART vp ON stg.viv_id = vp.code
# MAGIC         LEFT JOIN CTE_SHIP_SHIPSURE vs ON stg.ves_id = vs.code
# MAGIC         LEFT JOIN CTE_SHIP_COMPONENT vc ON stg.ptr_id = vc.code
# MAGIC         LEFT JOIN CTE_ACCOUNT_SHIPSURE acc ON stg.acc_id = acc.code
# MAGIC         LEFT JOIN CTE_PROCUREMENT_ITEM_TYPE pit ON stg.proc_item_type_code = pit.code
# MAGIC         LEFT JOIN CTE_PROCUREMENT_STATUS ps ON stg.proc_status = ps.code
# MAGIC                                             AND stg.proc_stage = ps.stage_name
# MAGIC         LEFT JOIN CTE_CONTACT_SHIPSURE ct ON stg.cmp_id = ct.code 
# MAGIC         LEFT JOIN CTE_CURRENCY cur ON stg.cur_id = cur.code
# MAGIC         LEFT JOIN CTE_TIME_CREATED dtOrig ON stg.ord_dateOriginated_notime = dtOrig.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_ENTERED dtEntered ON stg.ord_dateEntered_notime = dtEntered.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_REQUESTED_DELIVERED dtDelivery ON stg.ord_delivery_notime = dtDelivery.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_EXPECTED_DELIVERY dtExpectDelivery ON stg.ord_dateExpectDelivery_notime = dtExpectDelivery.DATE_DT
# MAGIC         LEFT JOIN CTE_DIVISION div ON stg.region_code = div.code
# MAGIC         LEFT JOIN CTE_DATE cd ON DATE_TRUNC('day', stg.firstEnquryDate) = cd.DATE_DT
# MAGIC         LEFT JOIN CTE_ALL_CURRENCY ac ON stg.cur_id = ac.code 
# MAGIC                                          AND cd.LAST_DAY_PREV_MONTH_SK = ac.CURRENCY_DATE_SK
# MAGIC where coy_id = '999999'

# COMMAND ----------

# DBTITLE 1,Check final result for notebook
# MAGIC %sql
# MAGIC WITH CTE_SHIP_PO_HEADER AS (
# MAGIC     select 
# MAGIC         SK, 
# MAGIC         COY_CODE, 
# MAGIC         ORDER_NO, 
# MAGIC         AMT 
# MAGIC     from test_gold.dw_procurement.dim_ship_po_header
# MAGIC ),
# MAGIC CTE_SHIP_PART AS (
# MAGIC     -- 364
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_part
# MAGIC ),
# MAGIC CTE_SHIP_SHIPSURE AS (
# MAGIC     -- 43
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_shipsure
# MAGIC ),
# MAGIC CTE_SHIP_COMPONENT AS (
# MAGIC     -- 79_218
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.dim_ship_component
# MAGIC ),
# MAGIC CTE_ACCOUNT_SHIPSURE AS (
# MAGIC     -- 1114
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.DIM_ACCOUNT_SHIPSURE
# MAGIC ),
# MAGIC CTE_PROCUREMENT_ITEM_TYPE AS (
# MAGIC     -- 4
# MAGIC     select sk, code
# MAGIC     from test_gold.dw_procurement.DIM_PROCUREMENT_ITEM_TYPE
# MAGIC ),
# MAGIC CTE_PROCUREMENT_STATUS AS (
# MAGIC     -- 59
# MAGIC     select
# MAGIC         sk,
# MAGIC         code, 
# MAGIC         name,
# MAGIC         stage_name
# MAGIC     from test_gold.dw_procurement.dim_procurement_status
# MAGIC ),
# MAGIC CTE_CONTACT_SHIPSURE AS (
# MAGIC     -- 78713
# MAGIC     select 
# MAGIC         sk, code
# MAGIC     from test_gold.dw_procurement.DIM_CONTACT_SHIPSURE
# MAGIC ),
# MAGIC CTE_CURRENCY AS (
# MAGIC     -- 174
# MAGIC     select 
# MAGIC         sk, code 
# MAGIC     from test_gold.dw_market_data.DIM_CURRENCY
# MAGIC ),
# MAGIC CTE_TIME_CREATED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_ENTERED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_REQUESTED_DELIVERED AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_TIME_EXPECTED_DELIVERY AS (
# MAGIC     --27_760
# MAGIC     select 
# MAGIC         date_sk,
# MAGIC         date_dt 
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_DIVISION AS (
# MAGIC     -- 8
# MAGIC     select 
# MAGIC         sk, code 
# MAGIC     from test_gold.dw_commercial.dim_region
# MAGIC ),
# MAGIC
# MAGIC -- STEP 21: Lookup transformation
# MAGIC CTE_DATE_TMP AS(
# MAGIC select 
# MAGIC     date_sk
# MAGIC     ,date_dt
# MAGIC     ,LAST_DAY(ADD_MONTHS(date_dt, -1)) AS LAST_DAY_PREV_MONTH
# MAGIC     from test_gold.dw_conformed.dim_time
# MAGIC ),
# MAGIC CTE_DATE AS(
# MAGIC     select 
# MAGIC         CTE_DATE_TMP.date_sk
# MAGIC         ,CTE_DATE_TMP.date_dt
# MAGIC         ,CTE_DATE_TMP.last_day_prev_month
# MAGIC         ,DT.date_sk as LAST_DAY_PREV_MONTH_SK
# MAGIC     from 
# MAGIC         CTE_DATE_TMP
# MAGIC         join test_gold.dw_conformed.dim_time as DT on CTE_DATE_TMP.last_day_prev_month = DT.date_dt
# MAGIC ),
# MAGIC -- STEP 22 + STEP 23
# MAGIC CTE_ALL_CURRENCY_TMP AS(
# MAGIC     select 
# MAGIC         FC.FROM_CURRENCY_SK,
# MAGIC         DT.CAL_MONTH_END_DAY,
# MAGIC         FC.CURRENCY_RATE,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY FC.from_currency_sk, DT.CAL_MONTH_KEY_NUM ORDER BY DT.DATE_SK DESC) AS ROW_VERSN
# MAGIC     from 
# MAGIC         test_gold.dw_market_data.FACT_CURRENCY_CONVERSION AS FC
# MAGIC         JOIN test_gold.dw_market_data.dim_currency as CAD on FC.TO_CURRENCY_SK = CAD.SK and CAD.code = 'CAD'
# MAGIC         JOIN test_gold.dw_conformed.dim_time as DT on FC.currency_date_sk = dt.DATE_SK
# MAGIC ),
# MAGIC CTE_ALL_CURRENCY AS(
# MAGIC     -- 33
# MAGIC     -- Return only the last rate found for each month and assign it the last calendar day of the month
# MAGIC     select 
# MAGIC         FC.CAL_MONTH_END_DAY as CURRENCY_DATE_SK
# MAGIC         ,CUR.code
# MAGIC         -- ,FC.CURRENCY_RATE
# MAGIC         ,fc.CURRENCY_RATE
# MAGIC     from 
# MAGIC         CTE_ALL_CURRENCY_TMP as FC
# MAGIC         JOIN test_gold.dw_market_data.dim_currency as CUR on FC.from_currency_sk = CUR.sk
# MAGIC     where ROW_VERSN = 1
# MAGIC     order by code
# MAGIC ), final as(
# MAGIC     SELECT
# MAGIC         COALESCE(vpo.sk, -1) AS ship_po_sk,
# MAGIC         CASE WHEN UPPER(ord_type) = 'SERVICES' THEN -3 ELSE COALESCE(vp.sk, -1) END AS ship_part_sk,
# MAGIC         COALESCE(vc.sk, -1) AS ship_comp_sk,
# MAGIC         COALESCE(vs.sk, -1) AS ship_sk,
# MAGIC         div.sk AS region_sk,
# MAGIC         COALESCE(ct.sk, -1) AS supplier_sk,
# MAGIC         COALESCE(acc.sk, -1) AS account_proc_sk,
# MAGIC         COALESCE(pit.sk, -1) AS proc_item_type_sk,
# MAGIC         int(COALESCE(stg.fvel_enqr_line_no, 0)) AS enqr_line_no,
# MAGIC         COALESCE(ps.sk, -1) AS enqr_status_sk,
# MAGIC         stg.FVEL_ENQR_STATUS_DATE AS enqr_status_date,
# MAGIC         COALESCE(cur.sk, -1) AS currency_sk,
# MAGIC         COALESCE(dtOrig.date_sk, 19750101) AS reqs_created_date_sk,
# MAGIC         COALESCE(dtEntered.date_sk, 19750101) AS reqs_entrd_date_sk,
# MAGIC         COALESCE(if(dtDelivery.date_sk < 19750101, 19750101,dtDelivery.date_sk), 19750101) AS expected_delivery_date_sk,
# MAGIC         COALESCE(if(dtExpectDelivery.date_sk < 19750101, 19750101,dtExpectDelivery.date_sk), 19750101) AS requested_delivery_date_sk ,
# MAGIC         -- (DT_NUMERIC,16,4)REPLACENULL(FVEL_UNIT_PRICE_AMT,0)
# MAGIC         cast(COALESCE(stg.FVEL_UNIT_PRICE_AMT, 0)as decimal(16,4)) AS unit_price_amt,
# MAGIC
# MAGIC         -- (DT_NUMERIC,16,4)(ROUND(REPLACENULL(FVEL_UNIT_PRICE_AMT,0) * REPLACENULL(FCC_CURRENCY_RATE,0),4))
# MAGIC         cast(COALESCE(stg.FVEL_UNIT_PRICE_AMT, 0) * if
# MAGIC                                                         (
# MAGIC                                                         stg.cur_id == "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                         1, --if 
# MAGIC                                                         if
# MAGIC                                                             ( --else
# MAGIC                                                             stg.cur_id != "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                             0, ifnull(ac.CURRENCY_RATE,0)
# MAGIC                                                             ) 
# MAGIC                                                         )as decimal(16,4)) AS unit_price_cad_amt,
# MAGIC
# MAGIC         -- (DT_NUMERIC,16,4)(FVEL_ENQUIRY_QTY * FVEL_UNIT_PRICE_CAD_AMT)
# MAGIC         int(stg.FVEL_ENQUIRY_QTY) AS enquiry_qty,
# MAGIC
# MAGIC         -- (DT_NUMERIC,16,4)(FVEL_ENQUIRY_QTY * FVEL_UNIT_PRICE_AMT)
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_QTY * FVEL_UNIT_PRICE_AMT, 0)as decimal(16,4)) AS enquiry_amt,
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_QTY * UNIT_PRICE_CAD_AMT, 0)as decimal(16,4)) AS enquiry_cad_amt,
# MAGIC         cast(stg.FVEL_ENQUIRY_SUPPLIER_AMT as decimal(16,4)) AS enquiry_supplier_amt,
# MAGIC
# MAGIC         -- (DT_NUMERIC,16,4)(ROUND(REPLACENULL(FVEL_ENQUIRY_SUPPLIER_AMT,0) * REPLACENULL(FCC_CURRENCY_RATE,0),4))
# MAGIC         cast(COALESCE(stg.FVEL_ENQUIRY_SUPPLIER_AMT, 0) * if
# MAGIC                                                         (
# MAGIC                                                         stg.cur_id == "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                         1, --if 
# MAGIC                                                         if
# MAGIC                                                             ( --else
# MAGIC                                                             stg.cur_id != "CAD" and ac.CURRENCY_RATE is null,
# MAGIC                                                             0, ifnull(ac.CURRENCY_RATE,0)
# MAGIC                                                             ) 
# MAGIC                                                         )as decimal(16,4)) AS enquiry_supplier_cad_amt
# MAGIC         ,monotonically_increasing_id() as idx
# MAGIC     FROM 
# MAGIC         stg stg
# MAGIC         LEFT JOIN CTE_SHIP_PO_HEADER vpo ON stg.coy_id = vpo.coy_code
# MAGIC                                         AND stg.ord_orderNo = vpo.order_no 
# MAGIC         LEFT JOIN CTE_SHIP_PART vp ON stg.viv_id = vp.code
# MAGIC         LEFT JOIN CTE_SHIP_SHIPSURE vs ON stg.ves_id = vs.code
# MAGIC         LEFT JOIN CTE_SHIP_COMPONENT vc ON stg.ptr_id = vc.code
# MAGIC         LEFT JOIN CTE_ACCOUNT_SHIPSURE acc ON stg.acc_id = acc.code
# MAGIC         LEFT JOIN CTE_PROCUREMENT_ITEM_TYPE pit ON stg.proc_item_type_code = pit.code
# MAGIC         LEFT JOIN CTE_PROCUREMENT_STATUS ps ON stg.proc_status = ps.code
# MAGIC                                             AND stg.proc_stage = ps.stage_name
# MAGIC         LEFT JOIN CTE_CONTACT_SHIPSURE ct ON stg.cmp_id = ct.code 
# MAGIC         LEFT JOIN CTE_CURRENCY cur ON stg.cur_id = cur.code
# MAGIC         LEFT JOIN CTE_TIME_CREATED dtOrig ON stg.ord_dateOriginated_notime = dtOrig.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_ENTERED dtEntered ON stg.ord_dateEntered_notime = dtEntered.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_REQUESTED_DELIVERED dtDelivery ON stg.ord_delivery_notime = dtDelivery.DATE_DT
# MAGIC         LEFT JOIN CTE_TIME_EXPECTED_DELIVERY dtExpectDelivery ON stg.ord_dateExpectDelivery_notime = dtExpectDelivery.DATE_DT
# MAGIC         LEFT JOIN CTE_DIVISION div ON stg.region_code = div.code
# MAGIC         LEFT JOIN CTE_DATE cd ON DATE_TRUNC('day', stg.firstEnquryDate) = cd.DATE_DT
# MAGIC         LEFT JOIN CTE_ALL_CURRENCY ac ON stg.cur_id = ac.code 
# MAGIC                                         AND cd.LAST_DAY_PREV_MONTH_SK = ac.CURRENCY_DATE_SK
# MAGIC )
# MAGIC , deluxe as (select
# MAGIC         *,
# MAGIC         count(*) over 
# MAGIC                     (
# MAGIC                     partition by 
# MAGIC                         SHIP_PO_SK, SHIP_PART_SK, SHIP_COMP_SK, SHIP_SK, 
# MAGIC                         REGION_SK, SUPPLIER_SK, ACCOUNT_PROC_SK, PROC_ITEM_TYPE_SK, 
# MAGIC                         ENQR_STATUS_SK, ENQR_STATUS_DATE, ENQR_LINE_NO
# MAGIC                     )  AS duplicates
# MAGIC         from final
# MAGIC where 1=1
# MAGIC )
# MAGIC select * from final where ship_po_sk = 3193389
# MAGIC -- select * except(duplicates, idx), current_timestamp as src_syst_effective_from
# MAGIC -- from deluxe
# MAGIC -- where duplicates = 1
# MAGIC --     and ship_po_sk != -1
# MAGIC --     and ship_sk != -1
# MAGIC --     and account_proc_sk != -1
# MAGIC --     and enqr_status_sk != -1
# MAGIC --     and proc_item_type_sk != -1
# MAGIC --     and reqs_created_date_sk > 19750101
# MAGIC --     and reqs_entrd_date_sk > 19750101
# MAGIC --     and ship_po_sk = 3193389

# COMMAND ----------

# DBTITLE 1,purchorder_enquiry
# MAGIC %sql
# MAGIC select * from purchorder_enquiry where coy_id='999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC select ROW_NUMBER() OVER(PARTITION BY coy_id,ord_OrderNo ORDER BY ord_dateStatus ASC) AS EnqrStageRowNum
# MAGIC             ,coy_id,ord_OrderNo,ord_stage,ord_dateStatus
# MAGIC             From Audit_PurchOrder where ord_stage in ('Authorised Enquiry') AND COY_ID='999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from purchaseOrder where coy_id = '999999';
# MAGIC select * from purchorder_enquiry where coy_id = '999999';
# MAGIC
# MAGIC --select * from AUDIT_PURCHORDER where coy_id = '999999'

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from purchorder where coy_id='999999';
# MAGIC select * from audit_purchorder where coy_id='999999';
# MAGIC select * from suporder where coy_id='999999';
# MAGIC select * from orderline where coy_id='999999';
# MAGIC select * from suporderline where coy_id='999999';
# MAGIC select * from audit_suporder where coy_id='999999';
# MAGIC --'audit_purchorder','suporder','orderline','suporderline', 'audit_suporder'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.fact_ship_enquiry_lines where ship_po_sk= 3193389

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from DIM_VESSEL_PO_HEADER where coy_code = '999999'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_CONTACT_SHIPSURE where code like'%999999%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from purchaseOrder where coy_id = '999999'

# COMMAND ----------

# DBTITLE 1,dim_contact_shipsure
# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_contact_shipsure where code like'%999999%'

# COMMAND ----------

# DBTITLE 1,dim_contact_shipsure
from pyspark.sql.functions import col, regexp_replace, when, upper
df_stg = spark.sql(f'''
            select c.* 
            from test_silver.shipsure.company c
            join (
                select distinct * from 
                (
                    select cmp_id from test_silver.shipsure.purchorder where src_syst_effective_to is null 
                    union all
                    select cmp_id from test_silver.shipsure.suporder where src_syst_effective_to is null 
                    union all
                    select cmp_id from test_silver.shipsure.invoicehdr where src_syst_effective_to is null 
                    union all
                    select cmp_id from test_silver.shipsure.deliveryaddress where src_syst_effective_to is null 
                    union all
                    select cmp_id from test_silver.shipsure.gl_transaction where src_syst_effective_to is null 
                )
            ) as j
                on j.cmp_id = c.cmp_id
            where src_syst_effective_to is null 
                and etl_processing_datetime >= '2024-08-20T23:59:32.400+00:00'
        ''')
for column in ['CMP_Name','CMP_Addr','CMP_Telephone', 'CMP_Fax', "CMP_Email", "CMP_Telex", "CMP_Mobile"]:
    df_stg = df_stg.withColumn(column, regexp_replace(col(column), '�', ''))
df_stg.createOrReplaceTempView("stg_shipsure_company")

df = spark.sql(f'''
                              
        select distinct
            upper(cp.CMP_ID) as code
            ,COALESCE(cp.CMP_Name, '') as name
            ,replace(cp.CMP_Town, 'ß', 'DEUTSCH_BETA') as city_name
            ,replace(cp.CMP_Addr, 'ß', 'DEUTSCH_BETA') as addr
            ,CASE 
                WHEN ifnull(cp.CMP_State, '') = '' THEN 'Unknown'
                ELSE cp.CMP_State
            END as state_name
            ,CASE 
                WHEN ifnull(cp.CMP_PostCode, '') = '' THEN 'Unknown'
                ELSE cp.CMP_PostCode
            END as post_code
            ,CASE 
                WHEN ifnull(cp.CMP_Telephone, '') = '' THEN 'Unknown'
                ELSE cp.CMP_Telephone
            END as phone_no
            ,CASE 
                WHEN ifnull(cp.CMP_Mobile, '') = '' THEN 'Unknown'
                ELSE cp.CMP_Mobile
            END as mobile_no
            ,CASE 
                WHEN ifnull(cp.CMP_Email, '') = '' THEN 'Unknown'
                ELSE cp.CMP_Email
            END as email_adr
            ,CASE 
                WHEN ifnull(cn.CNT_ID, '') = '' THEN 'UN'
                ELSE cn.CNT_ID
            END as country_code
            ,CASE 
                WHEN ifnull(cn.CNT_Desc,'') = '' THEN 'Unknown'
                ELSE UPPER(cn.CNT_Desc)
            END AS country_name
            ,CASE 
                WHEN we.len_list_email = 0 or we.len_list_email is null
                    THEN 'Unknown'
                WHEN we.len_list_email = 1 
                    THEN element_at(we.list_email, 1)
                WHEN element_at(we.list_email, 1) = cp.CMP_Email
                    THEN element_at(we.list_email, 2)
                ELSE element_at(we.list_email, 1)
            END as other_email_adr
            ,cp.CMP_UpdatedOn as src_syst_effective_from

        from stg_shipsure_company cp

        left join test_silver.shipsure.country cn
            on cp.CNT_ID = cn.CNT_ID
                and cn.src_syst_effective_to is null

        left join (
                    select 
                        USR_Office, 
                        collect_set(USR_Email) as list_email, 
                        size(collect_set(USR_Email)) as len_list_email
                    from test_silver.shipsure.webuserid 
                    where src_syst_effective_to is null
                    group by 1
                ) we
            on we.USR_Office = upper(cp.CMP_ID)
        
''').withColumn("city_name", 
                when((col("city_name").isNull()) | (col("city_name") == "") | (upper(col("city_name")) == "UNKNOWN"), "Unknown")
                .otherwise(regexp_replace(upper(col("city_name")), "DEUTSCH_BETA", "ß"))) \
    .withColumn("addr", 
                when((col("addr").isNull()) | (col("addr") == "") | (col("addr") == "Unknown"), "Unknown")
                .otherwise(regexp_replace(col("addr"), "DEUTSCH_BETA", "ß")))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg_shipsure_company --where cmp_coyid = '999999'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validations `FACT_SHIP_ENQR_SUPPL_CUMUL_SNAPSHOT`

# COMMAND ----------

# DBTITLE 1,FACT_SHIP_ENQR_SUPPL_CUMUL_SNAPSHOT
########################
##### ASQL CLEANER #####
########################
lookup_table = spark.table(f'test_silver.shipsure.po_delta') \
                    .filter('src_syst_effective_to is null') \
                    .select(["COY_ID", "ORD_OrderNo"]) \
                    .distinct()

df_audit_purchorder = spark.table(f'test_silver.shipsure.audit_purchorder') \
                        .filter('src_syst_effective_to is null') \
                        .join(lookup_table, ["COY_ID", "ORD_OrderNo"])
df_audit_purchorder.createOrReplaceTempView("stg_shipsure_audit_purchorder")

df_purchorder = spark.table(f'test_silver.shipsure.purchorder') \
                    .filter('src_syst_effective_to is null') \
                    .join(lookup_table, ["COY_ID", "ORD_OrderNo"])
df_purchorder.createOrReplaceTempView("stg_shipsure_purchorder")

df_audit_suporder = spark.table(f'test_silver.shipsure.audit_suporder') \
                        .filter('src_syst_effective_to is null') \
                        .join(lookup_table, ["COY_ID", "ORD_OrderNo"])
df_audit_suporder.createOrReplaceTempView("stg_shipsure_audit_suporder")

df_suporder = spark.table(f'test_silver.shipsure.suporder') \
                    .filter('src_syst_effective_to is null') \
                    .join(lookup_table, ["COY_ID", "ORD_OrderNo"])
df_suporder.createOrReplaceTempView("stg_shipsure_suporder")

#####################
##### df_source #####
#####################
df_src = spark.sql(f'''

    WITH AUDIT_PURCHORDER AS 
    ( --DS1
        SELECT DISTINCT
            AP.coy_id,
            AP.ord_OrderNo,
            P.ACC_ID ,
            AP.VES_ID ,
            P.ORD_Type,
            AP.ord_stage,
            AP.ord_status,
            AP.ord_dateStatus,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.ord_stage, AP.ord_status ORDER BY AP.ord_dateStatus ASC) AS StatusRowNum
        
        FROM stg_shipsure_audit_purchorder AP
        
        INNER JOIN stg_shipsure_purchorder P
            ON AP.coy_id = P.coy_id 
                AND AP.ord_OrderNo = P.ord_OrderNo 
                AND AP.VES_ID = P.VES_ID
        
        WHERE AP.ord_dateStatus IS NOT NULL 
            AND AP.ord_status IS NOT NULL 
            AND AP.ord_stage IN ('Enquiry')
    )
    , AUDIT_SUPORDER AS 
    ( --DS1
        SELECT DISTINCT
            AP.coy_id,
            AP.ord_OrderNo,
            AP.RSP_ID ,
            AP.CMP_ID ,
            P.RSP_TotalCost,
            P.RSP_Authorised,
            P.RSP_Comments,
            AP.RSP_UpdatedOn,
            AP.RSP_Status,
            P.RSP_Status as P_RSP_Status,
            ROW_NUMBER() OVER(PARTITION BY AP.coy_id, AP.ord_OrderNo, AP.RSP_ID, AP.CMP_ID, AP.RSP_Status, P.RSP_Status ORDER BY AP.RSP_UpdatedOn ASC) AS StatusRowNum,
            ap.updateType
        
        FROM stg_shipsure_audit_suporder AP
        
        INNER JOIN stg_shipsure_suporder P
            ON AP.coy_id = P.coy_id
                AND AP.ord_OrderNo = P.ord_OrderNo
                AND AP.RSP_ID = p.RSP_ID
                AND AP.CMP_ID = P.CMP_ID
                AND AP.RSP_Status IS NOT NULL
            AND P.RSP_Status IS NOT NULL
        
        INNER JOIN (
                select coy_id, ord_orderNo, rsp_id, cmp_id, updateDate, RSP_Status,
                    row_number() over (partition by coy_id, rsp_id, ord_orderNo, cmp_id order by updateType asc, RSP_UpdatedOn desc) as rsp_suppl_seq
                from stg_shipsure_audit_suporder
                where coalesce(Upper(updateType),'DELETE') != 'DELETE'
                    and RSP_Status IS NOT NULL
            ) PP
            on p.COY_ID=pp.COY_ID
                and p.ORD_OrderNo=pp.ORD_OrderNo
                and p.RSP_ID=pp.RSP_ID
                and p.CMP_ID=pp.CMP_ID
        
        WHERE AP.RSP_UpdatedOn IS NOT NULL
            AND AP.RSP_Status IS NOT NULL
            AND PP.RSP_Status IS NOT NULL
            AND P.RSP_Status  IS NOT NULL
            AND rsp_suppl_seq = 1
    )
    , EP_TO_TA_DAYS AS 
    ( -- DS6
        SELECT
            EP.coy_id,
            EP.ord_OrderNo,
            EP.ACC_ID,
            EP.VES_ID,
            EP.ORD_Type,
            TA.RSP_ID ,
            TA.CMP_ID ,
            (unix_timestamp(TA.RSP_UpdatedOn) - (unix_timestamp(EP.ord_dateStatus)))/60/60/24  as FVESCS_SUPPL_EP_TO_TA_DAYS_NUM
        FROM AUDIT_PURCHORDER EP
        
        INNER JOIN AUDIT_SUPORDER TA
            ON  EP.coy_id = TA.coy_id 
                AND EP.ord_OrderNo = TA.ord_OrderNo
        
        WHERE EP.ord_status = 'EP' 
            AND EP.StatusRowNum = 1 
            AND TA.RSP_Status = 'TA' 
            AND TA.StatusRowNum = 1
    )
    , Status_Date AS
    (
        SELECT DISTINCT
            coy_id,
            ord_OrderNo,
            RSP_ID,
            CMP_ID,
            case when RSP_Status = 'TA' and StatusRowNum=1 then RSP_UpdatedOn end as TA_STATUS_DATE,
            case when RSP_Status = 'TX' and StatusRowNum=1 then RSP_UpdatedOn end as TX_STATUS_DATE,
            case when RSP_Status = 'TH' and StatusRowNum=1 then RSP_UpdatedOn end as TH_STATUS_DATE,
            case when RSP_Status = 'TI' and StatusRowNum=1 then RSP_UpdatedOn end as TI_STATUS_DATE
        
        FROM AUDIT_SUPORDER

        WHERE RSP_Status IS NOT NULL
    )
    , Status_Date_EP AS
    (
        SELECT DISTINCT
            coy_id,
            ord_OrderNo,
            ACC_ID,
            VES_ID,
            case when ord_status = 'EP' and StatusRowNum=1 then ORD_DateStatus end as ENQR_EP_STATUS_DATE
        FROM AUDIT_PURCHORDER
    )
    , Orders_Listing AS
    (
        SELECT DISTINCT
            O.coy_id,
            O.ord_OrderNo,
            ACC_ID,
            VES_ID,
            SU.CMP_ID,
            SU.RSP_ID,
            ORD_Type,
            SU.P_RSP_Status,
            (select max(TA_STATUS_DATE) from Status_Date where coy_id = O.coy_id and ord_OrderNo= O.ord_OrderNo and RSP_ID= SU.RSP_ID and CMP_ID= SU.CMP_ID group by coy_id,ord_OrderNo,RSP_ID,CMP_ID) as FVESCS_SUPPL_TA_STATUS_DATE,
            (select max(TX_STATUS_DATE) from Status_Date where coy_id = O.coy_id and ord_OrderNo= O.ord_OrderNo and RSP_ID= SU.RSP_ID and CMP_ID= SU.CMP_ID group by coy_id,ord_OrderNo,RSP_ID,CMP_ID) as FVESCS_SUPPL_TX_STATUS_DATE,
            (select max(TH_STATUS_DATE) from Status_Date where coy_id = O.coy_id and ord_OrderNo= O.ord_OrderNo and RSP_ID= SU.RSP_ID and CMP_ID= SU.CMP_ID group by coy_id,ord_OrderNo,RSP_ID,CMP_ID) as FVESCS_SUPPL_TH_STATUS_DATE,
            (select max(TI_STATUS_DATE) from Status_Date where coy_id = O.coy_id and ord_OrderNo= O.ord_OrderNo and RSP_ID= SU.RSP_ID and CMP_ID= SU.CMP_ID group by coy_id,ord_OrderNo,RSP_ID,CMP_ID) as FVESCS_SUPPL_TI_STATUS_DATE,
            (select max(ENQR_EP_STATUS_DATE) from Status_Date_EP where coy_id = O.coy_id and ord_OrderNo= O.ord_OrderNo and VES_ID= O.VES_ID group by coy_id,ord_OrderNo,VES_ID) as FVESCS_SUPPL_EP_STATUS_DATE,
            RSP_UpdatedOn,
            updateType

        FROM AUDIT_PURCHORDER O 
        
        INNER JOIN AUDIT_SUPORDER SU
            ON  O.coy_id = SU.coy_id 
                AND O.ord_OrderNo = SU.ord_OrderNo
    )
    
    ---------------------------------------

    SELECT DISTINCT
        O.coy_id,
        O.ord_OrderNo,
        O.ACC_ID,
        O.VES_ID,
        -- O.ORD_Type,
        O.CMP_ID,
        -- O.RSP_ID,
        'CA' as DIVISION,
        FVESCS_SUPPL_EP_STATUS_DATE,
        FVESCS_SUPPL_TA_STATUS_DATE,
        FVESCS_SUPPL_TX_STATUS_DATE,
        FVESCS_SUPPL_TH_STATUS_DATE,
        FVESCS_SUPPL_TI_STATUS_DATE,
        FVESCS_SUPPL_EP_TO_TA_DAYS_NUM,
        P_RSP_Status,
        case 
            when O.ORD_Type like 'Service%' then 'S' 
            else case 
                    when O.ORD_Type like 'Material%' then 'M' 
                    else 'M' 
                end 
        end as proc_item_type_code,
        RSP_UpdatedOn,
        updateType
    
    FROM Orders_Listing O
    
    left join EP_TO_TA_DAYS EPTA
        ON O.coy_id = EPTA.coy_id 
            AND O.ord_OrderNo = EPTA.ord_OrderNo 
            AND O.VES_ID = EPTA.VES_ID
            AND O.CMP_ID = EPTA.CMP_ID
            AND O.RSP_ID = EPTA.RSP_ID

''').withColumn("rn", expr("row_number() over(partition by coy_id, ord_orderNo, cmp_id order by updateType asc, RSP_UpdatedOn desc)")) \
    .filter('rn = 1') \
    .drop('rn', 'RSP_UpdatedOn', 'updateType')
df_src.createOrReplaceTempView('vw_src')

df = spark.sql(f'''

    select 
        coalesce(dsph.sk, -1) as ship_po_sk,
        coalesce(dss.sk, -1) as ship_sk,
        coalesce(dd.sk, -1) as region_sk,
        coalesce(dcs.sk, -1) as supplier_sk,
        coalesce(das.sk, -1) as account_proc_sk,
        coalesce(dpit.sk, -1) as proc_item_type_sk,
        coalesce(dps.sk, -1) as suppl_last_status_sk,
        cte.FVESCS_SUPPL_EP_STATUS_DATE as suppl_ep_status_date,
        cte.FVESCS_SUPPL_TI_STATUS_DATE as suppl_ti_status_date,
        cte.FVESCS_SUPPL_TH_STATUS_DATE as suppl_th_status_date,
        cte.FVESCS_SUPPL_TX_STATUS_DATE as suppl_tx_status_date,
        cte.FVESCS_SUPPL_TA_STATUS_DATE as suppl_ta_status_date,
        cte.FVESCS_SUPPL_EP_TO_TA_DAYS_NUM as suppl_ep_to_ta_days_num
    from vw_src cte
    left join test_gold.dw_procurement.DIM_SHIP_PO_HEADER dsph
        on dsph.coy_code = cte.coy_id
            and dsph.ORDER_NO = cte.ord_OrderNo
    
    left join test_gold.dw_procurement.DIM_SHIP_SHIPSURE dss
        on dss.code = cte.VES_ID
    
    left join test_gold.dw_commercial.dim_region dd
        on dd.code = 'CA'
    
    left join test_gold.dw_procurement.DIM_CONTACT_SHIPSURE dcs
        on dcs.code = cte.cmp_id
    
    left join test_gold.dw_procurement.DIM_ACCOUNT_SHIPSURE das
        on das.code = cte.ACC_ID
    
    left join test_gold.dw_procurement.DIM_PROCUREMENT_ITEM_TYPE dpit
        on dpit.code = cte.proc_item_type_code
    
    left join test_gold.dw_procurement.DIM_PROCUREMENT_STATUS dps
        on dps.stage_name = 'Enquiry Supplier'
            and dps.code = cte.P_RSP_Status

''').withColumn("suppl_ep_to_ta_days_num", col("suppl_ep_to_ta_days_num").cast('double')) \
    .withColumn('src_syst_effective_from', current_timestamp())
df.createOrReplaceTempView('vw_final')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_final where ship_po_sk = -1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_src

# COMMAND ----------

# LOOKUPS
df_fcc_currency = spark.sql(f"""
with CTE_ALL_CURRENCY as
(
    select FC.from_currency_sk as FCC_FROM_CURRENCY_SK
        ,DT.CAL_MONTH_END_DAY
        ,FC.currency_rate as FCC_CURRENCY_RATE
        ,row_number() over (partition by fc.from_currency_sk, DT.CAL_MONTH_KEY_NUM order by DT.DATE_SK desc) as ROW_VERSN
    from test_gold.dw_market_data.FACT_CURRENCY_CONVERSION as FC
    join test_gold.dw_conformed.dim_currency as C on FC.TO_CURRENCY_SK = C.SK and c.CODE = 'CAD'
    join test_gold.dw_conformed.DIM_TIME as DT on FC.CURRENCY_DATE_SK = dt.DATE_SK
)
-- Return only the last rate found for each month and assign it the last calendar day of the month
select FC.CAL_MONTH_END_DAY as FCC_CURRENCY_DATE_SK
    ,CUR.code as CURRENCY_CODE
    ,FC.FCC_CURRENCY_RATE
from CTE_ALL_CURRENCY as FC
join test_gold.dw_conformed.dim_currency as CUR on FC.FCC_FROM_CURRENCY_SK = CUR.SK
where ROW_VERSN = 1
order by FCC_CURRENCY_DATE_SK, FCC_FROM_CURRENCY_SK
""")
df_fcc_currency.createOrReplaceTempView("fcc_currency")

df_dim_time_last_day_prev_period = spark.sql(f"""
with CTE_DATE as
(
select DATE_SK
    ,DATE_DT
    ,date_add(date_format(date_dt, 'yyyy-MM-01'),-1) as LAST_DAY_PREV_MONTH
    from test_gold.dw_conformed.DIM_TIME
)
select CTE_DATE.DATE_SK
    ,CTE_DATE.DATE_DT
    ,CTE_DATE.LAST_DAY_PREV_MONTH
    ,DT.DATE_SK as LAST_DAY_PREV_MONTH_SK
from CTE_DATE
join test_gold.dw_conformed.DIM_TIME as DT on CTE_DATE.LAST_DAY_PREV_MONTH = DT.DATE_DT
""")
df_dim_time_last_day_prev_period.createOrReplaceTempView("dim_time_last_day_prev_period")

df = spark.sql(f"""       
select
stg.coy_code
,stg.voucher_code
,int(stg.status_code) as status_code
,ref_code.ShipInvoiceStatusName_EN as status_desc
,stg.status_date
,stg.order_orig_no
,ifnull(vessel_po.order_no,"Unknown") as order_no
,ifnull(vessel_po.sk,-1) as order_sk
,stg.CURR_ORIG_CODE
,ifnull(dim_cur.sk,-1) as curr_sk
,ifnull(dim_cur.code,"UN") as curr_code
,ifnull(dim_cur.name,"Unknown") as curr_name
,stg.curr_amt
,stg.base_amt
,cast(round(CURR_AMT * (
                        CASE WHEN isnull(fcc_curr.FCC_CURRENCY_RATE) and dim_cur.code == "CAD" THEN 1
                            WHEN isnull(fcc_curr.FCC_CURRENCY_RATE) and dim_cur.code != "CAD" THEN 0
                            ELSE ifnull(fcc_curr.FCC_CURRENCY_RATE,0)
                        END
                        )
            ,2) 
        as decimal(16,4)) as curr_cad_amt
,stg.usd_amt
,stg.eur_amt
,stg.supplier_orig_code
,ifnull(contact_ship.sk,-1) as supplier_sk
,ifnull(contact_ship.code,"UN") as supplier_code
,ifnull(contact_ship.name,"Unknown") as supplier_name
,stg.supplier_invoice_no
,stg.supplier_invoice_date
,stg.code
,stg.src_syst_effective_from
from (
    select 
        COY_ID as COY_CODE
        ,INH_Voucher as VOUCHER_CODE
        ,bigint(INH_Status) as STATUS_CODE
        ,timestamp(INH_DateStatChgAct) as STATUS_DATE
        ,INH_Order as ORDER_ORIG_NO
        ,CUR_ID as CURR_ORIG_CODE
        ,cast(ifnull(INH_TotalCurr,0) as decimal(16,4)) as curr_amt
        ,cast(ifnull(INH_TotalBase,0) as decimal(16,4)) as base_amt
        ,cast(ifnull(INH_TotalUSD,0) as decimal(16,4)) as usd_amt
        ,cast(ifnull(INH_TotalEuro,0) as decimal(16,4)) as eur_amt
        ,upper(CMP_ID) as supplier_orig_code
        ,ifnull(INH_SupInv, '') as SUPPLIER_INVOICE_NO
        ,timestamp(INH_DateSupInv) as supplier_invoice_date
        ,INH_ID as CODE
        ,src_syst_effective_from
    from test_silver.shipsure.invoicehdr
    where src_syst_effective_to is null
        and etl_processing_datetime > '2024-08-28T14:16:23.992+00:00'
) stg
left join test_silver.xref.v_ship_invoice_status ref_code
    on      ref_code.ShipInvoiceStatusCode = string(stg.status_code)
left join test_gold.dw_conformed.dim_currency dim_cur
    on dim_cur.code = stg.curr_orig_code
left join dim_time_last_day_prev_period ldpp
    on ldpp.date_dt = stg.supplier_invoice_date 
left join fcc_currency fcc_curr
    on      fcc_curr.fcc_currency_date_sk = ldpp.last_day_prev_month_sk 
        and fcc_curr.currency_code = dim_cur.code
left join test_gold.dw_procurement.DIM_CONTACT_SHIPSURE contact_ship
    on      upper(contact_ship.code) = stg.supplier_orig_code
left join test_gold.dw_procurement.dim_ship_po_header vessel_po
    on      vessel_po.coy_code = stg.coy_code
        and vessel_po.order_no = stg.order_orig_no
""")
df.createOrReplaceTempView("finalvw")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finalvw

# COMMAND ----------

# DBTITLE 1,fact_ship_cust_forecast_mthly
# MAGIC %sql
# MAGIC select count(*) from test_gold.dw_commercial.fact_ship_cust_forecast_mthly;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM INFORMATION_SCHEMA.VIEWS
# MAGIC where VIEW_DEFINITION like '%FACT_SHIP_CUST_BUDGET_MTHLY%'
