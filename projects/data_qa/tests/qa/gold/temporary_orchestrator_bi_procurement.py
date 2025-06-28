# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestrator for BI Procurement: Dimensions

# COMMAND ----------

# DBTITLE 1,Load the notebook reference and some variables
core_notebook = "silver_to_gold"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_po_header`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_vessel_po_header",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_po_header",
    "table_type": "dim",
    "col_prefix": "vessel_po",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_po_header.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_po_header.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_account_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_account_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_account_shipsure",
    "table_type": "dim",
    "col_prefix": "account_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_account_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_account_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_contact_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_contact_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_contact_shipsure",
    "table_type": "dim",
    "col_prefix": "contact_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_contact_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_contact_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_department_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_department_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_department_shipsure",
    "table_type": "dim",
    "col_prefix": "dept_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_department_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_department_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_job_class_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_job_class_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_job_class_shipsure",
    "table_type": "dim",
    "col_prefix": "job_class_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_job_class_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_job_class_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_procurement_item_type`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_procurement_item_type",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_procurement_item_type",
    "table_type": "dim",
    "col_prefix": "proc_item_type",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_procurement_item_type.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_procurement_item_type.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_reference_code`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_reference_code",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_reference_code",
    "table_type": "dim",
    "col_prefix": "ref_code",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_reference_code.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_reference_code.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_component`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_vessel_component",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_component",
    "table_type": "dim",
    "col_prefix": "vessel_comp",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_component.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_component.json',
    "list_skip_cols_check_content": "delete_or_inactive_date, model_desc, position_short_desc, serial_no, name",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_part`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_silver.shipsure.vesinventory where VIV_ID IN('NA', 'SERVICE')

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct trim(makers_ref_desc) as makers_ref_desc from test_gold.dw_procurement.dim_ship_part order by 1 asc
# MAGIC --select distinct makers_ref_desc from test_gold.dw_procurement.dim_ship_part order by 1 asc
# MAGIC select * from test_gold.dw_procurement.dim_ship_part where code IN('NUKU00001930', 'NUKU00001931')--makers_ref_desc LIKE '%Not a%'
# MAGIC --code in('CSTL00031512', 'LIMA00226879', 'MONT00034669')--makers_ref_desc = '-' order by 3 asc
# MAGIC
# MAGIC -- ''
# MAGIC -- '	4000K722'
# MAGIC
# MAGIC --select count(distinct trim(drawing_pos_desc)) total_ro from test_gold.dw_procurement.dim_ship_part order by 1 asc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_vessel_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_ship_shipsure",
    "table_type": "dim",
    "col_prefix": "vessel_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_ship_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_ship_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_work_order_type`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_work_order_type",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_work_order_type",
    "table_type": "dim",
    "col_prefix": "wo_type",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_work_order_type.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_work_order_type.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_job_class_shipsure`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_job_class_shipsure",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_job_class_shipsure",
    "table_type": "dim",
    "col_prefix": "job_class_ss",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_job_class_shipsure.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_job_class_shipsure.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_rescheduled_reason`

# COMMAND ----------

execution_parameters = {
    "catalog_db_name": "XPTOdw",
    "schema_db_name": "dw",
    "table_db_name": "dim_rescheduled_reason",
    "schema_gold_name": "dw_procurement",
    "table_gold_name": "dim_rescheduled_reason",
    "table_type": "dim",
    "col_prefix": "resch_reason, resch",
    "query_path": "resources/sql/bi_procurement/dimensions/dim_rescheduled_reason.sql",
    "datatypes_definition_file": 'resources/schemas/bi_procurement/dimensions/dim_rescheduled_reason.json',
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_ship_invoice_header`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_procurement.dim_ship_invoice_header

# COMMAND ----------

# MAGIC %sql
# MAGIC --select distinct 'order_no' as field_name, order_no from test_gold.dw_procurement.dim_ship_invoice_header order by 1,2 desc
# MAGIC --select * from test_gold.dw_procurement.dim_ship_invoice_header where coy_code IN(3400, 3668) and ORDER_NO IN('02036', '03373')
# MAGIC -- union
# MAGIC --select distinct 'order_orig_no' as field_name, order_orig_no from test_gold.dw_procurement.dim_ship_invoice_header where trim(order_orig_no)='null'  order by 1,2 asc
# MAGIC
# MAGIC -- select coy_code, voucher_code, order_orig_no, count(order_orig_no) as tot from test_gold.dw_procurement.dim_ship_invoice_header
# MAGIC -- where coy_code='1093' --trim(order_orig_no)=''
# MAGIC -- --where trim(ifnull(order_orig_no,'1'))='1'
# MAGIC -- group by coy_code, voucher_code, order_orig_no
# MAGIC -- order by 3,4 asc
# MAGIC
# MAGIC Select COY_CODE, ORDER_ORIG_NO,
# MAGIC 	SUM(CASE WHEN ORDER_ORIG_NO IS NULL THEN 1 ELSE 0 END) TOTAL_NULL_VALUE,
# MAGIC 	SUM(CASE WHEN TRIM(ORDER_ORIG_NO) = '' THEN 1 ELSE 0 END) TOTAL_EMPTY_VALUE,
# MAGIC 	SUM(CASE WHEN TRIM(ORDER_ORIG_NO) = 'Unknown' THEN 1 ELSE 0 END) TOTAL_Unknown_VALUE
# MAGIC from test_gold.dw_procurement.dim_ship_invoice_header
# MAGIC where COY_CODE = '1093' and (ORDER_ORIG_NO is null or TRIM(ORDER_ORIG_NO) = '')
# MAGIC group by COY_CODE, ORDER_ORIG_NO
# MAGIC order by 2, 3, 4 asc

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from test_silver.shipsure.purchorder where (ORD_OrderNo like '%02036%' or ORD_OrderNo like'%03373%') and COY_ID IN(3400, 3668)
# MAGIC
# MAGIC --select * from test_silver.shipsure.invoicehdr where COY_ID IN(3400, 3668) and INH_Voucher IN('200269MT', '200588MT')
# MAGIC --(INH_Origin like '%02036%' or INH_Origin like'%03373%')
# MAGIC
# MAGIC select * from test_silver.shipsure.orderauditlog where --ORD_OrderNo IN ('02036 ','03373 ')
# MAGIC
# MAGIC ORD_OrderNo like '%02036%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 'parent_sk' field_desc, parent_sk from test_gold.dw_finance.dim_cost_center order by 1, 2
# MAGIC --union
# MAGIC -- select distinct 'ship_sk' field_desc, ship_sk from test_gold.dw_finance.dim_cost_center order by 1, 2
# MAGIC --delete from test_gold.dw_finance.dim_cost_center where sk=394 and code='1234';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 'ship_sk' field_desc, ship_sk from test_gold.dw_finance.dim_cost_center order by 1, 2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `dim_maintenance_job`

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from test_gold.dw_procurement.dim_maintenance_job
# MAGIC
# MAGIC select distinct cancel_date from test_gold.dw_procurement.dim_maintenance_job

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from test_gold.dw_finance.dim_account
# MAGIC --select * from test_silver.oracle_sftp_accounting.gl_seg_val_hier_rf where pk1_value like '%911%'
# MAGIC --select * from test_gold.dw_finance.fact_accounting_transactions where account_sk> 3860-- in()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ShipOwnerCode as code
# MAGIC     ,ShipOwnerName as name
# MAGIC     ,int(ShipOwnerOrder) as order
# MAGIC     ,src_syst_effective_from
# MAGIC FROM test_silver.xref.v_ship_owner

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_commercial.dim_pool_owner

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     am.DW_Type_Code as code,
# MAGIC     am.Account as ledger_acct_code,
# MAGIC     ft.sk as sk,
# MAGIC     --ft.FUEL_TYPE_CODE,
# MAGIC     am.src_syst_effective_from
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         CAST(DataLayerTypeCode AS VARCHAR(3)) AS DW_Type_Code,
# MAGIC         CAST(GLAccount AS VARCHAR(10)) AS Account,
# MAGIC         src_syst_effective_from
# MAGIC     FROM test_silver.xref.v_account_imos_gl_group_category_type_details
# MAGIC     WHERE
# MAGIC         Category = 'Asset'
# MAGIC         AND src_syst_effective_to is null
# MAGIC ) AS am
# MAGIC LEFT JOIN test_gold.dw_commercial.DIM_FUEL_TYPE AS ft ON am.DW_Type_Code = ft.code;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from test_silver.xref.v_account_imos_gl_group_category_type_details
# MAGIC where GLAccount like'%14100-0019%'--DataLayerTypeCode like "%60"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_commercial.DIM_FUEL_TYPE
# MAGIC --select * from test_gold.dw_commercial.dim_fuel_type_ledger

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     vtype AS code,
# MAGIC     short_name AS SHORT_NAME,
# MAGIC     vdesc AS DESC,
# MAGIC     COALESCE(sulfGrade, 'UN') AS SULFUR_GRADE_CODE,
# MAGIC     BACKUPFUELTYPE AS BACKUP_ORIG_CODE,
# MAGIC     CAST(COALESCE(co2Emissions, '0') AS DECIMAL(10,4)) AS CO2_EMISSION_FACT,
# MAGIC     COALESCE(grade, 0) AS GRADE_NUM,
# MAGIC     COALESCE(trackSulfurContent, 0) AS TRACK_SULFUR_PERCENT,
# MAGIC     src_syst_effective_from
# MAGIC FROM test_silver.imos_datalake_api.fueltype
# MAGIC WHERE
# MAGIC     src_syst_effective_to is null
# MAGIC     --AND etl_processing_datetime >= '{last_success_date}'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct *--table_catalog, table_schema, table_name, column_name
# MAGIC FROM `system`.`information_schema`.`columns` where table_catalog = 'test_gold' and table_name like '%fact_ship_procurement_agg%' ORDER BY table_schema
# MAGIC
# MAGIC --table_catalog = 'test_gold' and

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Tests for `fact_ship_procurement_agg`

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select count(*) AS QtyRows from test_gold.dw_procurement.fact_ship_procurement_agg
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT ship_po_sk,
# MAGIC ship_inv_sk,
# MAGIC ship_sk,
# MAGIC region_sk,
# MAGIC supplier_sk,
# MAGIC account_sk,
# MAGIC proc_item_type_sk,
# MAGIC currency_po_sk,
# MAGIC currency_inv_sk,
# MAGIC order_date_sk,
# MAGIC requested_delivery_date_sk,
# MAGIC expected_delivery_date_sk,
# MAGIC received_date_sk,
# MAGIC invoice_date_sk,
# MAGIC order_material_net_amt,
# MAGIC order_material_net_cad_amt,
# MAGIC order_freight_amt,
# MAGIC order_freight_cad_amt,
# MAGIC invoice_material_amt,
# MAGIC invoice_material_cad_amt,
# MAGIC invoice_freight_amt,
# MAGIC invoice_freight_cad_amt,
# MAGIC etl_created_datetime,
# MAGIC etl_created_job_id,
# MAGIC etl_created_job_run_id,
# MAGIC etl_updated_datetime,
# MAGIC etl_updated_job_id,
# MAGIC etl_updated_job_run_id,
# MAGIC etl_rec_version_start_date,
# MAGIC etl_rec_version_end_date,
# MAGIC etl_rec_version_current_flag
# MAGIC from test_gold.dw_procurement.fact_ship_procurement_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'UC-Test-FACT_SHIP_ENQR_SUPPL_CUMUL_SNAPSHOT' as fact_name, COUNT(*) AS QtyRows from test_gold.dw_procurement.FACT_SHIP_ENQR_SUPPL_CUMUL_SNAPSHOT union
# MAGIC select 'UC-Test-FACT_SHIP_WORK_ORDER_DUE_AGG_SNAPSHOT' as fact_name, COUNT(*) AS QtyRows from test_gold.dw_procurement.FACT_SHIP_WORK_ORDER_DUE_AGG_SNAPSHOT union
# MAGIC select 'UC-Test-FACT_SHIP_PO_LINES_AGG' as fact_name, COUNT(*) AS QtyRows from test_gold.dw_procurement.FACT_SHIP_PO_LINES_AGG union
# MAGIC select 'UC-Test-FACT_SHIP_PROCUREMENT_AGG' as fact_name, COUNT(*) AS QtyRows from test_gold.dw_procurement.FACT_SHIP_PROCUREMENT_AGG union
# MAGIC select 'UC-Test-FACT_SHIP_PROCUREMENT_CUMUL_SNAPSHOT' as fact_name, COUNT(*) AS QtyRows from test_gold.dw_procurement.FACT_SHIP_PROCUREMENT_CUMUL_SNAPSHOT
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct order_material_net_amt from test_gold.dw_procurement.FACT_SHIP_PO_LINES_AGG

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.fact_accounting_transactions where account_sk = 3872;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_conformed.dim_fiscal_period where UPPER(period_name) = 'APR-24' and period_num_year=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from test_silver.oracle_sftp_accounting.GL_CODE_COMBINATIONS
# MAGIC select period_name, * from test_silver.oracle_sftp_accounting.GL_JE_Lines order by effective_date asc;
# MAGIC select * from test_silver.oracle_sftp_accounting.fnd_vs_value_sets where value like '%999%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.dim_cost_center --where sk in(213, 283, 336);
# MAGIC

# COMMAND ----------

df_src = spark.sql(f"""

    select cast(PK1_VALUE as varchar(25)) as COST_CENTER_CODE
        ,PK2_VALUE as segment_value_value_set_code
        ,cast(case
            when ANCESTOR_PK1_VALUE is null or ANCESTOR_PK1_VALUE = ''
            then 'NA' else cast(ANCESTOR_PK1_VALUE as varchar(25))
            end as varchar(25)
            ) as COST_CENTER_PARENT_CODE
        ,src_syst_effective_from
    FROM test_silver.oracle_sftp_accounting.gl_seg_val_hier_rf
    WHERE src_syst_effective_to is null
        AND etl_processing_datetime >= '2024-06-28T15:36:23.297+00:00'
        AND current_timestamp() between effective_start_date and effective_end_date
        AND distance = 1
        AND tree_code = 'XPTO_Cost_Center_Main'

""")
df_src.createOrReplaceTempView('stg')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg

# COMMAND ----------

df_nodes = spark.sql("""
    select COST_CENTER_CODE, cast(COST_CENTER_CODE as varchar(255)) as COST_CENTER_PARENT_CODE_FULL
    from stg
    where lower(COST_CENTER_PARENT_CODE) = 'na'
""")
df_nodes.createOrReplaceTempView('n')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from n

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, lit, when, col
from functools import reduce
# creating recursive CTE
df_aux = df_nodes
dfs_to_union = []
loops_qty = 0
while df_aux.count() > 0:
    df_aux = spark.sql("""
        select dim.COST_CENTER_CODE, cast(concat(cte.COST_CENTER_PARENT_CODE_FULL, '\\\\', dim.COST_CENTER_CODE) as varchar(255)) as COST_CENTER_PARENT_CODE_FULL
        from stg as dim
        join n as cte on cte.COST_CENTER_CODE = dim.COST_CENTER_PARENT_CODE
        where lower(COST_CENTER_PARENT_CODE) <> 'na'
    """)
    df_aux.createOrReplaceTempView("n")
    loops_qty+=1
    dfs_to_union.append(df_aux)

print("Numbers of recursion", loops_qty)
df_recursive = reduce(DataFrame.unionAll, dfs_to_union+[df_nodes])
df_recursive.createOrReplaceTempView('cte_denormalize_hierarchy')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cte_denormalize_hierarchy

# COMMAND ----------

df_main = spark.sql(f'''

                with cte_hierarchy as
                (
                    select src.COST_CENTER_CODE
                        ,src.SEGMENT_VALUE_VALUE_SET_CODE
                        ,src.COST_CENTER_PARENT_CODE
                        ,cte.COST_CENTER_PARENT_CODE_FULL
                        ,src_syst_effective_from
                    from stg as src
                    left join cte_denormalize_hierarchy as cte on src.COST_CENTER_CODE = cte.COST_CENTER_CODE
                )

                --------------------------------------------------

                SELECT
                    CAST(dim_segment_value.value AS VARCHAR(25)) AS code
                    ,CAST(description AS VARCHAR(100)) AS name
                    ,CAST(CONCAT(dim_segment_value.value,' - ', description) AS VARCHAR(100)) AS full_name
                    ,CASE
                        WHEN CAST(DIM_SEGMENT_VALUE.VALUE AS INT) BETWEEN 1000 AND 7999 THEN 'Vessel'
                        WHEN CAST(DIM_SEGMENT_VALUE.VALUE AS INT) BETWEEN 8000 AND 8999 THEN 'Department'
                        ELSE 'Not Applicable'
                    END AS type_code
                    ,IF(dim_segment_value.summary_flag = 'Y', 0, 1) AS detail_flag
                    ,IF(dim_segment_value.enabled_flag = 'Y', 1, 0) AS enabled_flag
                    --,par.sk AS parent_sk
                    ,cast(-2 as bigint) as parent_sk -- WILL BE UPDATED NEXT TASK
                    ,COALESCE(cte.COST_CENTER_PARENT_CODE, dim_cost_center.parent_code, 'NA') AS parent_code
                    ,COALESCE(dim_ship.sk, -1) AS ship_sk
                    ,IF(v_external_charter.ExternalCharterCode is null, 0, 1) AS external_charter_flag
                    ,COALESCE(cte.COST_CENTER_PARENT_CODE_FULL, dim_cost_center.parent_code_full, dim_segment_value.value) AS parent_code_full
                    ,greatest(cte.src_syst_effective_from, dim_segment_value.etl_rec_version_start_date) as src_syst_effective_from
                FROM test_gold.dw_finance.dim_segment_value dim_segment_value
                    LEFT JOIN cte_hierarchy cte ON cte.COST_CENTER_CODE = dim_segment_value.value AND cte.SEGMENT_VALUE_VALUE_SET_CODE = dim_segment_value.value_set_code
                    LEFT JOIN test_gold.dw_finance.dim_cost_center dim_cost_center ON dim_cost_center.code = dim_segment_value.value
                    LEFT JOIN test_silver.xref.v_ship_mapping xref ON xref.SourceShipCode = dim_segment_value.value AND lower(xref.SourceName) = 'oracle'
                    LEFT JOIN test_gold.dw_commercial.dim_ship dim_ship ON dim_ship.code = xref.DwShipCode
                    LEFT JOIN test_silver.xref.v_external_charter v_external_charter ON v_external_charter.ExternalCharterCode = dim_segment_value.value
                WHERE dim_segment_value.value is not null AND lower(dim_segment_value.value_set_code) = 'xxXPTO_dept_cc_vessel'
                ORDER BY dim_segment_value.value, dim_segment_value.value_set_code
    ''')
df_main.createOrReplaceTempView('main')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main

# COMMAND ----------

df_account_main = spark.sql("""
    SELECT NAME AS NOM
        ,cast(CODE as varchar(25)) AS CODE
        ,cast(PARENT_CODE as varchar(25)) AS PRT
    FROM main
    WHERE lower(TYPE_CODE) = 'department'
""")
df_account_main.createOrReplaceTempView('account_main')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from account_main

# COMMAND ----------

df_nodes2 = spark.sql("""
    SELECT NOM, CODE
        ,cast(Null as varchar(255)) as Path4_MainParent_Code
        ,cast(Null as varchar(255)) as Path4_MainParent_Name
        ,cast(Null as varchar(255)) as Path3_MainParent_Code
        ,cast(Null as varchar(255)) as Path3_MainParent_Name
        ,cast(Null as varchar(255)) as Path2_MainParent_Code
        ,cast(Null as varchar(255)) as Path2_MainParent_Name
        ,cast(Null as varchar(255)) as Path1_MainParent_Code
        ,cast(Null as varchar(255)) as Path1_MainParent_Name
        ,0 as Level
    FROM account_main
    WHERE PRT = '0001'
""")
df_nodes2.createOrReplaceTempView('n2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from n2

# COMMAND ----------

# creating recursive CTE
df_aux2 = df_nodes2
dfs_to_union2 = []
loops_qty2 = 0
while df_aux2.count() > 0:
    df_aux2 = spark.sql("""
        Select
            Child.NOM,
            CHILD.CODE,
            case when (n2.Level+1) = 1 then Child.CODE else Path4_MainParent_Code end as Path4_MainParent_Code,
            case when (n2.Level+1) = 1 then Child.NOM else Path4_MainParent_Name end as Path4_MainParent_Name,
            case when (n2.Level+1) = 2 then Child.CODE else Path3_MainParent_Code end as Path3_MainParent_Code,
            case when (n2.Level+1) = 2 then Child.NOM else Path3_MainParent_Name end as Path3_MainParent_Name,
            case when (n2.Level+1) = 3 then Child.CODE else Path2_MainParent_Code end as Path2_MainParent_Code,
            case when (n2.Level+1) = 3 then Child.NOM else Path2_MainParent_Name end as Path2_MainParent_Name,
            case when (n2.Level+1) = 4 then Child.CODE else Path1_MainParent_Code end as Path1_MainParent_Code,
            case when (n2.Level+1) = 4 then Child.NOM else Path1_MainParent_Name end as Path1_MainParent_Name,
            n2.Level+1 as `Level`
        from n2
        join account_main child
        on  child.PRT = n2.CODE
    """)
    df_aux2.createOrReplaceTempView("n2")
    loops_qty2+=1
    dfs_to_union2.append(df_aux2)

print("Numbers of recursion", loops_qty2)
df_pillar_recursive = reduce(DataFrame.unionAll, dfs_to_union2+[df_nodes2])
df_pillar_recursive.createOrReplaceTempView('cte_tree_main')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cte_tree_main

# COMMAND ----------

df = spark.sql(f'''
    with COST_CENTER_CODE_PER_PILLAR_CODE AS
    (
        SELECT CODE AS COST_CENTER_CODE, 'CORP' AS DEPARTMENT_PILLAR_CODE
        FROM cte_tree_main
        WHERE lower(NOM) LIKE '%corporate%'
        OR lower(Path4_MainParent_Name) LIKE '%corporate%'
        OR lower(Path3_MainParent_Name) LIKE '%corporate%'
        OR lower(Path2_MainParent_Name) LIKE '%corporate%'
        OR lower(Path1_MainParent_Name) LIKE '%corporate%'

        UNION ALL

        SELECT CODE AS COST_CENTER_CODE,'COMM' AS DEPARTMENT_PILLAR_CODE
        FROM cte_tree_main
        WHERE lower(NOM) LIKE '%commercial%'
        OR lower(Path4_MainParent_Name) LIKE '%commercial%'
        OR lower(Path3_MainParent_Name) LIKE '%commercial%'
        OR lower(Path2_MainParent_Name) LIKE '%commercial%'
        OR lower(Path1_MainParent_Name) LIKE '%commercial%'

        UNION ALL

        SELECT CODE AS COST_CENTER_CODE,'OPER' AS DEPARTMENT_PILLAR_CODE
        FROM cte_tree_main
        WHERE lower(NOM) LIKE '%operation%'
        OR lower(Path4_MainParent_Name) LIKE '%operation%'
        OR lower(Path3_MainParent_Name) LIKE '%operation%'
        OR lower(Path2_MainParent_Name) LIKE '%operation%'
        OR lower(Path1_MainParent_Name) LIKE '%operation%'
    )
    ,pillar_join AS
    (
        SELECT COST_CENTER_CODE
            , dim_pillar.sk as PILLAR_SK
            , dim_pillar.code as PILLAR_CODE
            , dim_pillar.name as PILLAR_NAME
            , dim_pillar.full_name as PILLAR_FULL_NAME
        FROM (select *, row_number() over(partition by COST_CENTER_CODE order by DEPARTMENT_PILLAR_CODE) as rn from COST_CENTER_CODE_PER_PILLAR_CODE) a
        LEFT JOIN test_gold.dw_conformed.dim_pillar dim_pillar ON dim_pillar.CODE = DEPARTMENT_PILLAR_CODE
        WHERE rn = 1
    )
    -------------------------------------------------------------------------------------

    select
        code
        ,name
        ,full_name
        ,type_code
        ,detail_flag
        ,enabled_flag
        ,COALESCE(parent_sk, -2) AS parent_sk
        ,parent_code
        ,COALESCE(ship_sk, -2) AS ship_sk
        ,COALESCE(pillar_join.pillar_sk, -2) AS pillar_sk
        ,COALESCE(pillar_join.pillar_code, 'NA') AS pillar_code
        ,COALESCE(pillar_join.pillar_name, 'Not applicable') AS pillar_name
        ,COALESCE(pillar_join.pillar_full_name, 'NA - Not applicable') AS pillar_full_name
        ,external_charter_flag
        ,parent_code_full
        ,src_syst_effective_from

    from main

    left join pillar_join
        on pillar_join.COST_CENTER_CODE = main.code

''').withColumn("detail_flag", col("detail_flag").cast("boolean")) \
.withColumn("enabled_flag", col("enabled_flag").cast("boolean")) \
.withColumn("external_charter_flag", col("external_charter_flag").cast("boolean"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_conformed.dim_pillar

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.dim_segment_value dim_segment_value WHERE dim_segment_value.value is not null AND lower(dim_segment_value.value_set_code) = 'xxXPTO_dept_cc_vessel';
# MAGIC --update test_gold.dw_finance.dim_segment_value set description='FICT2' where value = 8999;
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  with cte_LinesVersion as
# MAGIC (
# MAGIC     select *, row_number() over (partition by JE_HEADER_ID, JE_LINE_NUM order by Last_Update_date desc) as VERSION_NUM
# MAGIC     from test_silver.oracle_sftp_accounting.GL_JE_Lines
# MAGIC     where STATUS = 'P' and src_syst_effective_to is null
# MAGIC )
# MAGIC ,aux as
# MAGIC (
# MAGIC     select distinct
# MAGIC         cast(l.JE_HEADER_ID as int) as ACCT_TRANS_JE_HEADER_ID_ORIGIN
# MAGIC         ,cast(l.JE_LINE_NUM as int) as ACCT_TRANS_JE_LINE_NUM
# MAGIC         ,cc.SEGMENT1 as ACCT_COMPANY_CODE
# MAGIC         ,cc.SEGMENT2 as COST_CENTER_CODE
# MAGIC         ,cc.SEGMENT3 as ACCOUNT_CODE
# MAGIC         ,cc.SEGMENT4 as ACCT_PROJECT_CODE
# MAGIC         ,cc.SEGMENT5 as ACCT_INTERCO_CODE
# MAGIC         ,cast(l.LEDGER_ID as varchar(25)) as LEDGER_ID
# MAGIC         ,if(l.CURRENCY_CODE == "STAT", "NA",l.CURRENCY_CODE)  as ACCT_TRANS_CURRENCY_CODE
# MAGIC         ,ifnull(try_cast(l.ACCOUNTED_DR as numeric(18,2)),0) as FACCT_ACCTD_DR_AMOUNT
# MAGIC         ,ifnull(try_cast(l.ACCOUNTED_CR as numeric(18,2)),0) as FACCT_ACCTD_CR_AMOUNT
# MAGIC         ,ifnull((-1) * cast(nullif(l.ACCOUNTED_CR,'') as decimal(18,2)),0) + ifnull(cast(nullif(l.ACCOUNTED_DR,'') as decimal(18,2)),0) as FACCT_AMOUNT_OLD
# MAGIC         ,ifnull(try_cast(l.ENTERED_DR as numeric(18,2)),0) as FACCT_ENTD_DR_AMOUNT
# MAGIC         ,ifnull(try_cast(l.ENTERED_CR as numeric(18,2)),0) as FACCT_ENTD_CR_AMOUNT
# MAGIC         ,l.EFFECTIVE_DATE
# MAGIC         --,l.PERIOD_NAME   original to keep from april 2023
# MAGIC         --tempory fix during February and March 2023, because there is an issue in Oracle system, where it was put the period  11-23 instead FEB-23 and 12-23 instead MAR-23, they cannot change it in Oracle side
# MAGIC         --the temporary fix will be removed on April 2023 after month/year end is finished
# MAGIC         , IF (l.PERIOD_NAME='12-23', 'MAR-23',IF (l.PERIOD_NAME='11-23', 'FEB-23',l.PERIOD_NAME))  as PERIOD_NAME --this line is a temporary fix,  will be removed on April 2023 (put back the line commented above)
# MAGIC         ,cc.src_syst_effective_from
# MAGIC     from cte_LinesVersion as l
# MAGIC     join test_silver.oracle_sftp_accounting.GL_CODE_COMBINATIONS as cc on l.CODE_COMBINATION_ID = cc.CODE_COMBINATION_ID and cc.src_syst_effective_to is null
# MAGIC     where l.VERSION_NUM = 1
# MAGIC )
# MAGIC ,final as
# MAGIC (
# MAGIC     select
# MAGIC         ACCT_TRANS_JE_HEADER_ID_ORIGIN
# MAGIC         ,ACCT_TRANS_JE_LINE_NUM
# MAGIC         ,ACCT_COMPANY_CODE
# MAGIC         ,COST_CENTER_CODE
# MAGIC         ,ACCOUNT_CODE
# MAGIC         ,ACCT_PROJECT_CODE
# MAGIC         ,ACCT_INTERCO_CODE
# MAGIC         ,LEDGER_ID
# MAGIC         ,ACCT_TRANS_CURRENCY_CODE
# MAGIC         ,FACCT_ACCTD_DR_AMOUNT
# MAGIC         ,FACCT_ACCTD_CR_AMOUNT
# MAGIC         ,FACCT_AMOUNT_OLD
# MAGIC         ,FACCT_ENTD_DR_AMOUNT
# MAGIC         ,FACCT_ENTD_CR_AMOUNT
# MAGIC         ,EFFECTIVE_DATE
# MAGIC         ,PERIOD_NAME
# MAGIC         ,(FACCT_ACCTD_DR_AMOUNT - FACCT_ACCTD_CR_AMOUNT) as FACCT_ACCTD_NET_AMOUNT
# MAGIC         ,(FACCT_ENTD_DR_AMOUNT - FACCT_ENTD_CR_AMOUNT) as FACCT_ENTD_NET_AMOUNT
# MAGIC         ,da.sk as facct_account_sk
# MAGIC         ,da.CODE account_code
# MAGIC         ,da.TYPE account_type
# MAGIC         ,dl.sk as facct_ledger_sk
# MAGIC         ,dl.CODE as ledger_code
# MAGIC         ,dl.CURRENCY_CODE ledger_currency_code
# MAGIC         ,dl.FISCAL_YEAR_START_MONTH ledger_fiscal_year_start_month
# MAGIC         ,dca.code currency_code
# MAGIC         ,dca.sk facct_currency_sk
# MAGIC         ,dcb.code dcb_CURRENCY_CODE
# MAGIC         ,dcb.sk facct_orig_currency_sk
# MAGIC         ,cc.code COST_CENTER_CODE
# MAGIC         ,cc.sk as FACCT_COST_CENTER_SK
# MAGIC         ,cc.pillar_sk FACCT_PILLAR_SK
# MAGIC         ,dacx.code as ACCT_COMPANY_CODE
# MAGIC         ,dacx.sk FACCT_ACCT_COMPANY_SK
# MAGIC         ,dacx.region_sk FACCT_COMPANY_REGION_SK
# MAGIC         ,dacx.region_currency_sk ACCT_COMPANY_REGION_CURRENCY_SK
# MAGIC         ,dacy.code as ACCT_COMPANY_CODE
# MAGIC         ,dacy.sk FACCT_ACCT_INTERCO_SK
# MAGIC         ,dacy.region_sk FACCT_INTERCO_REGION_SK
# MAGIC         ,dacz.code as ACCT_PROJECT_CODE
# MAGIC         ,dacz.sk as FACCT_ACCT_PROJECT_SK
# MAGIC         ,dat.je_header_id_origin as ACCT_TRANS_JE_HEADER_ID_ORIGIN
# MAGIC         ,dat.je_line_num as ACCT_TRANS_JE_LINE_NUM
# MAGIC         ,dat.sk as FACCT_ACCT_TRANS_SK
# MAGIC         ,dt.DATE_DT  as date_dt
# MAGIC         ,dt.date_sk as FACCT_DATE_SK
# MAGIC         --,CTE_DATE.CTE_DATE_SK
# MAGIC         ,fx.FCC_CURRENCY_DATE_SK
# MAGIC         ,CASE
# MAGIC             WHEN ACCOUNT_CODE >= '000000' AND ACCOUNT_CODE <= '299999'
# MAGIC                 THEN CASE
# MAGIC                         WHEN dl.CURRENCY_CODE = "CAD" OR aux.ACCT_TRANS_CURRENCY_CODE = "NA" THEN 1
# MAGIC                         WHEN ISNULL(fx.GROUP_DAILY_RATE) THEN 0
# MAGIC                         ELSE fx.GROUP_DAILY_RATE
# MAGIC                     END
# MAGIC             WHEN ACCOUNT_CODE >= '300000' AND ACCOUNT_CODE <= '899999'
# MAGIC                 THEN CASE
# MAGIC                         WHEN LEDGER_CURRENCY_CODE = 'CAD' OR ACCT_TRANS_CURRENCY_CODE = 'NA' THEN 1
# MAGIC                         WHEN FACCT_DATE_SK < 20240401 THEN fx.GROUP_DAILY_RATE
# MAGIC                         WHEN GROUP_AVERAGE_RATE IS NULL THEN fx.GROUP_DAILY_RATE
# MAGIC                         ELSE fx.GROUP_AVERAGE_RATE
# MAGIC                     END
# MAGIC             ELSE 1
# MAGIC         END as FCC_CURRENCY_RATE
# MAGIC         ,fx.FCC_CURRENCY_DATE_SK
# MAGIC         ,fx.FCC_FROM_CURRENCY_SK
# MAGIC         ,fx.FCC_TO_CURRENCY_SK
# MAGIC         ,CASE
# MAGIC             WHEN ACCOUNT_CODE >= '000000' AND ACCOUNT_CODE <= '299999'
# MAGIC                 THEN CASE
# MAGIC                         WHEN FACCT_CURRENCY_SK = ACCT_COMPANY_REGION_CURRENCY_SK OR ACCT_TRANS_CURRENCY_CODE = 'NA' THEN 1
# MAGIC                         WHEN fx.REGION_DAILY_RATE IS NULL THEN 0
# MAGIC                         ELSE fx.REGION_DAILY_RATE
# MAGIC                     END
# MAGIC             WHEN ACCOUNT_CODE >= '300000' AND ACCOUNT_CODE <= '899999'
# MAGIC                 THEN CASE
# MAGIC                         WHEN FACCT_CURRENCY_SK = ACCT_COMPANY_REGION_CURRENCY_SK OR ACCT_TRANS_CURRENCY_CODE = 'NA' THEN 1
# MAGIC                         WHEN FACCT_DATE_SK < 20240401 THEN fx.REGION_DAILY_RATE
# MAGIC                         WHEN REGION_AVERAGE_RATE IS NULL THEN fx.REGION_DAILY_RATE
# MAGIC                         ELSE fx.REGION_AVERAGE_RATE
# MAGIC                     END
# MAGIC             ELSE 1
# MAGIC         END as FCC_CURRENCY_RATE_REGION
# MAGIC         ,aux.src_syst_effective_from
# MAGIC
# MAGIC     from aux
# MAGIC
# MAGIC     left join test_gold.dw_finance.dim_account da
# MAGIC         on aux.account_code = da.code
# MAGIC             and da.ETL_rec_VERSION_CURRENT_FLAG = 1
# MAGIC
# MAGIC     left join test_gold.dw_finance.dim_ledger dl
# MAGIC         on aux.ledger_id = dl.code
# MAGIC
# MAGIC     left join test_gold.dw_market_data.dim_currency dca
# MAGIC         on dl.currency_code = dca.code
# MAGIC
# MAGIC     left join test_gold.dw_market_data.dim_currency dcb
# MAGIC         on aux.ACCT_TRANS_CURRENCY_CODE = dcb.code
# MAGIC
# MAGIC     left join test_gold.dw_finance.dim_cost_center cc
# MAGIC         on aux.cost_center_code = cc.code
# MAGIC
# MAGIC     left join test_gold.dw_finance.dim_accounting_company dacx
# MAGIC         on aux.ACCT_COMPANY_CODE = dacx.code
# MAGIC
# MAGIC     left join test_gold.dw_finance.dim_accounting_company dacy
# MAGIC         on aux.acct_interco_code = dacy.code
# MAGIC
# MAGIC     left join test_gold.dw_finance.DIM_ACCOUNTING_PROJECT dacz
# MAGIC         on aux.acct_project_code = dacz.code
# MAGIC
# MAGIC     left join test_gold.dw_finance.DIM_ACCOUNTING_TRANS_INFO dat
# MAGIC         on aux.ACCT_TRANS_JE_HEADER_ID_ORIGIN = dat.je_header_id_origin
# MAGIC             and aux.acct_trans_je_LINE_NUM = dat.je_line_num
# MAGIC
# MAGIC     left join test_gold.dw_conformed.dim_time dt
# MAGIC         on dt.DATE_DT = aux.effective_date
# MAGIC
# MAGIC     left join test_gold.dw_finance.V_FACT_FX_RATE fx
# MAGIC         on fx.fcc_currency_date_sk = dt.date_sk
# MAGIC             and fx.fcc_from_currency_sk = dca.code
# MAGIC             and fx.fcc_to_currency_sk = dcb.code
# MAGIC )
# MAGIC
# MAGIC --------------------------------------------------------------
# MAGIC
# MAGIC select
# MAGIC     COALESCE(facct_account_sk, -1) as account_sk
# MAGIC     ,COALESCE(facct_ledger_sk, -1) as ledger_sk
# MAGIC     ,COALESCE(facct_currency_sk, -1) as currency_sk
# MAGIC     ,COALESCE(facct_orig_currency_sk, -1) as orig_currency_sk
# MAGIC     ,COALESCE(facct_cost_center_sk, -1) as cost_center_sk
# MAGIC     ,COALESCE(facct_acct_company_sk, -2) acct_company_sk
# MAGIC     ,COALESCE(facct_company_region_sk, -1) company_region_sk
# MAGIC     ,COALESCE(facct_acct_project_sk, -1) acct_project_sk
# MAGIC     ,COALESCE(facct_date_sk, 19750101) date_sk
# MAGIC     ,COALESCE(dfp.sk, 1197501) as fiscal_period_sk
# MAGIC     ,COALESCE(facct_acct_trans_sk, -1) acct_trans_sk
# MAGIC     ,COALESCE(facct_pillar_sk,-1) pillar_sk
# MAGIC     ,COALESCE(facct_acct_interco_sk,-2) acct_interco_sk
# MAGIC     ,COALESCE(facct_interco_region_sk,-1) interco_region_sk
# MAGIC     ,CAST(coalesce(facct_acctd_dr_amount,0) as DECIMAL(18,2)) acctd_dr_amount
# MAGIC     ,CAST(coalesce(facct_acctd_cr_amount,0)  as DECIMAL(18,2)) acctd_cr_amount
# MAGIC     ,CAST(coalesce(facct_acctd_net_amount,0)  as DECIMAL(18,2)) acctd_net_amount
# MAGIC     ,CAST(round(coalesce(facct_acctd_net_amount, 0) * coalesce(fcc_currency_rate, 0), 4) as DECIMAL(18,2)) as acctd_net_cad_amount
# MAGIC     ,CAST(coalesce(facct_amount_old,0) as DECIMAL(18,2)) as amount
# MAGIC     ,CAST(coalesce(facct_entd_dr_amount,0) as DECIMAL(18,2)) entd_dr_amount
# MAGIC     ,CAST(coalesce(facct_entd_cr_amount,0) as DECIMAL(18,2)) entd_cr_amount
# MAGIC     ,CAST(coalesce(facct_entd_net_amount,0) as DECIMAL(18,2)) entd_net_amount
# MAGIC     ,CAST(round(coalesce(facct_acctd_net_amount, 0) * coalesce(fcc_currency_rate_region, 0), 4) as DECIMAL(18,2)) as region_net_amount
# MAGIC     ,f.src_syst_effective_from
# MAGIC from final f
# MAGIC left join test_gold.dw_conformed.dim_fiscal_period dfp
# MAGIC     on cast(left(dfp.sk,1) as int) = f.ledger_fiscal_year_start_month
# MAGIC         and upper(dfp.period_name) = f.period_name
# MAGIC         and dfp.period_num >= 197501
# MAGIC where COALESCE(facct_account_sk, -1) = -1 or COALESCE(facct_account_sk, -1)>3872

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.dim_accounting_company dacx where code like '%999%';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_silver.xref.v_company_region_code as map

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT
# MAGIC --   DISTINCT CASE
# MAGIC --     WHEN `a`.`dest_code` = '3' THEN '003'
# MAGIC --     ELSE `a`.`dest_code`
# MAGIC --   END AS OracleCompany,
# MAGIC --   `a`.`dest_code_2` AS RegionCode,
# MAGIC --   `a`.`dest_code_3` AS CurrencyCode,
# MAGIC --   `a`.`modified` AS `src_syst_effective_from`,
# MAGIC --   `a`.`src_syst_effective_to`
# MAGIC -- FROM
# MAGIC --   mapping_value a
# MAGIC --   INNER JOIN mapping_definition b ON a.definition_id = b.id
# MAGIC -- WHERE
# MAGIC --   `b`.`code` = 'COMPANY'
# MAGIC --   AND `a`.`src_syst_effective_to` IS NULL
# MAGIC --   AND `a`.`dest_code_2` IS NOT NULL;
# MAGIC
# MAGIC select * from test_silver.xref.mapping_definition b where `b`.`code` = 'COMPANY';
# MAGIC select * from test_silver.xref.mapping_value a WHERE a.definition_id = 'F1F6018E-5F57-472E-8F8C-4E10ACCB8751' AND `a`.`src_syst_effective_to` IS NULL AND `a`.`dest_code_2` IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_commercial.dim_region

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
dbutils.notebook.exit(f'"{notebook_name} --> {final_text}"')

# COMMAND ----------

# MAGIC %md
# MAGIC DIM_ACC_Company

# COMMAND ----------

df_src = spark.sql(f"""
    select cast(PK1_VALUE as varchar(25))   as ACCT_COMPANY_CODE
        ,PK2_VALUE					    as segment_value_value_set_code
        ,cast(case
            when ANCESTOR_PK1_VALUE is null or ANCESTOR_PK1_VALUE = ''
            then 'NA' else cast(ANCESTOR_PK1_VALUE as varchar(25))
            end as varchar(25)
            ) as ACCT_COMPANY_PARENT_CODE
        ,src_syst_effective_from

    FROM test_silver.oracle_sftp_accounting.gl_seg_val_hier_rf

    WHERE src_syst_effective_to is null
        AND etl_processing_datetime >= '2024-06-28T17:59:37.892+00:00'
        AND current_timestamp() BETWEEN effective_start_date AND effective_end_date
        AND distance = 1
        AND tree_code = 'XPTO_Company_Main'

""")
df_src.createOrReplaceTempView('stg')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stg

# COMMAND ----------

df_nodes = spark.sql("""

    select ACCT_COMPANY_CODE
        ,cast(ACCT_COMPANY_CODE as varchar(255)) as ACCT_COMPANY_PARENT_CODE_FULL
    from stg
    where ACCT_COMPANY_PARENT_CODE = 'NA'

""")
df_nodes.createOrReplaceTempView('n')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from n

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, lit, when, col
from functools import reduce
# creating recursive CTE
df_aux = df_nodes
dfs_to_union = []
loops_qty = 0
while df_aux.count() > 0:
    df_aux = spark.sql("""
                    select
                        dim.ACCT_COMPANY_CODE
                        ,cast(concat(cte.ACCT_COMPANY_PARENT_CODE_FULL, '\\\\', dim.ACCT_COMPANY_CODE) as varchar(255)) as ACCT_COMPANY_PARENT_CODE_FULL

                    from stg as dim

                    join n as cte
                        on cte.ACCT_COMPANY_CODE = dim.ACCT_COMPANY_PARENT_CODE

                    where ACCT_COMPANY_PARENT_CODE <> 'NA'
    """)
    df_aux.createOrReplaceTempView("n")
    loops_qty+=1
    dfs_to_union.append(df_aux)

print("Numbers of recursion", loops_qty)
df_recursive = reduce(DataFrame.unionAll, dfs_to_union+[df_nodes])
df_recursive.createOrReplaceTempView('cte_denormalize_hierarchy')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cte_denormalize_hierarchy

# COMMAND ----------

df = spark.sql(f'''

                with cte_hierarchy as
                (
                    select *
                    from
                    (

                        select *
                            ,row_number() over(partition by ACCT_COMPANY_CODE order by RegionCode) as rn
                        from
                        (
                            select src.ACCT_COMPANY_CODE
                                ,src.SEGMENT_VALUE_VALUE_SET_CODE
                                ,src.ACCT_COMPANY_PARENT_CODE
                                ,cte.ACCT_COMPANY_PARENT_CODE_FULL
                                ,map.RegionCode as RegionCode
                                ,greatest(src.src_syst_effective_from,map.src_syst_effective_from) as src_syst_effective_from

                            from stg as src

                            left join cte_denormalize_hierarchy as cte
                                on src.ACCT_COMPANY_CODE = cte.ACCT_COMPANY_CODE

                            left join test_silver.xref.v_company_region_code as map
                                on charindex(map.OracleCompany, cte.ACCT_COMPANY_PARENT_CODE_FULL) > 0
                        )
                    )
                    where rn = 1
                )
                ,cte_dim_acct_company as
                (
                    select *
                    from
                    (
                        select *
                            ,row_number() over(partition by code order by char_ind) as rn
                        from
                        (
                            select cie.code
                                ,cie.parent_code
                                ,cie.parent_code_full
                                ,COALESCE(map.RegionCode, reg.code) as region_code
                                ,charindex(map.OracleCompany, cie.parent_code_full) as char_ind

                            from test_gold.dw_finance.dim_accounting_company as cie

                            left join test_silver.xref.v_company_region_code as map
                                on charindex(map.OracleCompany,cie.parent_code_full) > 0

                            left join test_gold.dw_commercial.dim_region as reg
                                on map.RegionCode = reg.code
                        )
                    )
                    where rn = 1
                )

                -------------------------------------------------------------------------------------

                SELECT
                    CAST(dim_segment_value.value AS VARCHAR(25)) AS code
                    ,CAST(description AS VARCHAR(100)) AS name
                    ,CAST(CONCAT(dim_segment_value.value,' - ', description) AS VARCHAR(100)) AS full_name
                    ,IF(dim_segment_value.summary_flag = 'Y', 0, 1) AS detail_flag
                    ,IF(dim_segment_value.enabled_flag = 'Y', 1, 0) AS enabled_flag
                    --,COALESCE(par.sk, -2) AS parent_sk
                    ,cast(-2 as bigint) as parent_sk -- WILL BE UPDATED NEXT TASK
                    ,COALESCE(cte.ACCT_COMPANY_PARENT_CODE, dim_acct.parent_code, 'NA') AS parent_code
                    ,COALESCE(dim_region.sk, -2) AS region_sk
                    ,COALESCE(dim_acct.region_code, 'NA') AS region_code
                    ,COALESCE(dim_region.name, 'Not applicable') AS region_name
                    ,CONCAT(COALESCE(dim_acct.region_code, 'NA'), ' - ', COALESCE(dim_region.name, 'Not applicable')) AS region_full_name
                    ,COALESCE(cte.ACCT_COMPANY_PARENT_CODE_FULL, dim_acct.parent_code_full, dim_segment_value.value) AS parent_code_full
                    ,COALESCE(dim_currency.sk, -2) AS region_currency_sk
                    ,COALESCE( IF(dim_acct.region_code = 'NA', xref_na.CurrencyCode, xref_notna.CurrencyCode), 'NA' ) AS region_currency_code
                    ,greatest(cte.src_syst_effective_from, dim_segment_value.etl_rec_version_start_date) as src_syst_effective_from

                FROM test_gold.dw_finance.dim_segment_value dim_segment_value

                LEFT JOIN cte_hierarchy cte
                    ON cte.ACCT_COMPANY_CODE = dim_segment_value.value
                        AND cte.SEGMENT_VALUE_VALUE_SET_CODE = dim_segment_value.value_set_code

                LEFT JOIN cte_dim_acct_company dim_acct
                    ON dim_acct.code = dim_segment_value.value

                LEFT JOIN test_gold.dw_commercial.dim_region dim_region
                    ON dim_region.code = dim_acct.region_code

                LEFT JOIN test_silver.xref.v_company_region_code xref_notna
                    ON xref_notna.RegionCode = dim_acct.region_code
                        AND xref_notna.RegionCode <> 'NA'

                LEFT JOIN test_silver.xref.v_company_region_code xref_na
                    ON xref_na.OracleCompany = dim_segment_value.value
                        AND xref_na.RegionCode = 'NA'

                LEFT JOIN test_gold.dw_market_data.dim_currency dim_currency
                    ON dim_currency.code = IF(dim_acct.region_code = 'NA', xref_na.CurrencyCode, xref_notna.CurrencyCode)

                WHERE dim_segment_value.value is not null
                    AND dim_segment_value.value_set_code = 'XXXPTO_COMPANY'

                ORDER BY dim_segment_value.value, dim_segment_value.value_set_code

        ''').withColumn("detail_flag", col("detail_flag").cast("boolean")) \
            .withColumn("enabled_flag", col("enabled_flag").cast("boolean")) \
            .distinct()

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC with result as
# MAGIC (
# MAGIC   select cast(det.JE_HEADER_ID as int) as je_header_id_origin
# MAGIC       ,cast(det.JE_LINE_NUM as int) as je_line_num
# MAGIC       ,COALESCE(det.DESCRIPTION, '') as je_line_desc
# MAGIC       ,COALESCE(cc.SEGMENT1, '') as je_line_segment_1
# MAGIC       ,COALESCE(cc.SEGMENT2, '') as je_line_segment_2
# MAGIC       ,COALESCE(cc.SEGMENT3, '') as je_line_segment_3
# MAGIC       ,COALESCE(cc.SEGMENT4, '') as je_line_segment_4
# MAGIC       ,COALESCE(cc.SEGMENT5, '') as je_line_segment_5
# MAGIC       ,COALESCE(cc.SEGMENT6, '') as je_line_segment_6
# MAGIC       ,COALESCE(hdr.JE_CATEGORY, '') as je_header_cat_name
# MAGIC       ,COALESCE(hdr.JE_SOURCE, '') as je_header_src_name
# MAGIC       ,COALESCE(hdr.NAME, '') as je_header_name
# MAGIC       ,COALESCE(hdr.DESCRIPTION, '') as je_header_desc
# MAGIC       ,cast(COALESCE(hdr.CREATED_BY, '') as varchar(100)) as je_header_created_by
# MAGIC       ,CAST(CAST(COALESCE(hdr.DATE_CREATED, '1900-01-01') AS DATE) AS TIMESTAMP) as je_header_created_date
# MAGIC       ,cast(COALESCE(hdr.LAST_UPDATED_BY, '') as varchar(100)) as je_header_updated_by
# MAGIC       ,CAST(CAST(COALESCE(hdr.LAST_UPDATE_DATE, '1900-01-01') AS DATE) AS TIMESTAMP) as je_header_updated_date
# MAGIC       ,COALESCE(bat.NAME, '') as je_batch_name
# MAGIC       ,COALESCE(bat.DESCRIPTION, '') as je_batch_desc
# MAGIC       ,COALESCE(bat.CREATED_BY, '') as je_batch_created_by
# MAGIC       ,CAST(CAST(COALESCE(bat.CREATION_DATE, '1900-01-01') AS DATE) AS TIMESTAMP) as je_batch_created_date
# MAGIC       ,COALESCE(bat.LAST_UPDATED_BY, '')as je_batch_updated_by
# MAGIC       ,CAST(CAST(COALESCE(bat.LAST_UPDATE_DATE, '1900-01-01') AS DATE) AS TIMESTAMP) as je_batch_updated_date
# MAGIC       ,ROW_NUMBER() OVER(PARTITION BY det.JE_HEADER_ID,det.JE_LINE_NUM ORDER BY bat.LAST_UPDATE_DATE DESC) AS RowNum
# MAGIC       ,greatest(det.src_syst_effective_from, hdr.src_syst_effective_from, bat.src_syst_effective_from) as src_syst_effective_from
# MAGIC   from test_silver.oracle_sftp_accounting.gl_je_lines as det
# MAGIC   left join test_silver.oracle_sftp_accounting.gl_je_headers as hdr on det.JE_HEADER_ID = hdr.JE_HEADER_ID and hdr.src_syst_effective_to is null
# MAGIC   left join test_silver.oracle_sftp_accounting.gl_je_batches as bat on hdr.JE_BATCH_ID = bat.JE_BATCH_ID and bat.src_syst_effective_to is null
# MAGIC   left join (
# MAGIC               select *,
# MAGIC                   ROW_NUMBER() OVER(PARTITION BY CODE_COMBINATION_ID ORDER BY etl_processing_datetime desc) as rn
# MAGIC               from test_silver.oracle_sftp_accounting.gl_code_combinations
# MAGIC               where src_syst_effective_to is null
# MAGIC           ) as cc
# MAGIC       on det.CODE_COMBINATION_ID = cc.CODE_COMBINATION_ID
# MAGIC           and cc.rn = 1
# MAGIC   where det.src_syst_effective_to is null and det.STATUS = 'P' and det.je_header_id = 999999999
# MAGIC )
# MAGIC
# MAGIC ----------------------------------------------------------------------
# MAGIC
# MAGIC select distinct * from result where RowNum=1

# COMMAND ----------

df_step1 = spark.sql(f"""

                with stg(
                    select
                    date_sk as FACCT_DATE_SK
                    ,account_sk as FACCT_ACCOUNT_SK
                    ,cost_center_sk as FACCT_COST_CENTER_SK
                    ,acct_company_sk as FACCT_ACCT_COMPANY_SK
                    ,ledger_sk as FACCT_LEDGER_SK
                    ,currency_sk as FACCT_CURRENCY_SK
                    ,fiscal_period_sk as FACCT_FISCAL_PERIOD_SK
                    ,etl_rec_version_start_date as src_syst_effective_from
                    ,etl_rec_version_current_flag
                    ,amount
                    ,etl_rec_version_start_date
                    FROM test_gold.dw_finance.FACT_ACCOUNTING_TRANSACTIONS
                    where etl_rec_version_current_flag = 1
                ), CTE_FACT_BK_GROUP AS
                (
                    -- FIND ALL POSSIBLE COMBINAISONS FOR THE BUSINESS KEY OF THE AGGREGATE FACT TABLE
                    SELECT dim_account.code as ACCOUNT_CODE
                        ,FACCT_LEDGER_SK
                        ,FACCT_CURRENCY_SK
                        ,DIM_COST_CENTER.code as COST_CENTER_CODE
                        ,dim_accounting_company.code as ACCT_COMPANY_CODE
                        ,MIN(CAL_MONTH_START_DAY) AS min_date
                        ,max(stg.src_syst_effective_from) as src_syst_effective_from
                    FROM stg
                        INNER JOIN test_gold.dw_conformed.DIM_TIME ON DATE_SK = FACCT_DATE_SK
                        INNER JOIN test_gold.dw_finance.DIM_ACCOUNT ON dim_account.sk = FACCT_ACCOUNT_SK
                        INNER JOIN test_gold.dw_finance.DIM_COST_CENTER ON dim_cost_center.sk = FACCT_COST_CENTER_SK
                        INNER JOIN test_gold.dw_finance.DIM_ACCOUNTING_COMPANY ON dim_accounting_company.sk = FACCT_ACCT_COMPANY_SK
                    WHERE right(FACCT_FISCAL_PERIOD_SK,2) <> 13 and right(FACCT_FISCAL_PERIOD_SK,2) != 0
                    GROUP BY ACCOUNT_CODE, FACCT_LEDGER_SK, FACCT_CURRENCY_SK, COST_CENTER_CODE, ACCT_COMPANY_CODE
                )
                ,CTE_CALENDAR_PERIOD_FACT AS
                (
                    -- BUILD A CALENDAR WITH ALL PERIODS FOR THE POSSIBLE COMBINAISONS
                    SELECT DATE_SK, ACCOUNT_CODE,FACCT_LEDGER_SK,FACCT_CURRENCY_SK,COST_CENTER_CODE,ACCT_COMPANY_CODE,min_date, FISC_MONTH_KEY_NUM
                    FROM test_gold.dw_conformed.DIM_TIME dt
                    INNER JOIN CTE_FACT_BK_GROUP ON dt.DATE_SK >= CTE_FACT_BK_GROUP.min_date AND CAL_DAY_OF_MONTH_NUM = 1 and DATE_SK <= date_format(current_date+interval 2 month,'yyyyMM01')
                    --WHERE 1=1
                    --AND
                    --ACCOUNT_CODE = '901105'
                    --AND COST_CENTER_CODE = '2002'
                    --AND ACCT_COMPANY_CODE = '210'
                    --AND DATE_SK = 20170401
                )
                ,CTE_FACT_SK_GROUP as
                (
                    SELECT DIM_ACCOUNT.code AS FSG_ACCOUNT_CODE
                        ,max(fat.FACCT_ACCOUNT_SK) as FSG_ACCOUNT_SK
                        ,fat.FACCT_LEDGER_SK AS FSG_FACCT_LEDGER_SK
                        ,fat.FACCT_CURRENCY_SK AS FSG_FACCT_CURRENCY_SK
                        ,dim_cost_center.code AS FSG_COST_CENTER_CODE
                        ,max(fat.FACCT_COST_CENTER_SK) as FSG_COST_CENTER_SK
                        ,DIM_ACCOUNTING_COMPANY.code AS FSG_ACCT_COMPANY_CODE
                        ,max(fat.FACCT_ACCT_COMPANY_SK) as FSG_ACCT_COMPANY_SK
                        ,MIN(CAL_MONTH_START_DAY) AS FSG_CAL_MONTH_START_DAY
                        ,max(fat.etl_rec_version_start_date) as src_syst_effective_from
                    FROM stg as fat
                    INNER JOIN test_gold.dw_conformed.DIM_TIME ON dim_time.DATE_SK = fat.FACCT_DATE_SK
                    INNER JOIN test_gold.dw_finance.DIM_ACCOUNT ON dim_account.sk = fat.FACCT_ACCOUNT_SK
                    INNER JOIN test_gold.dw_finance.DIM_COST_CENTER ON dim_cost_center.sk = fat.FACCT_COST_CENTER_SK
                    INNER JOIN test_gold.dw_finance.DIM_ACCOUNTING_COMPANY ON DIM_ACCOUNTING_COMPANY.sk = fat.FACCT_ACCT_COMPANY_SK
                    WHERE right(fat.FACCT_FISCAL_PERIOD_SK,2) <> 13 and right(fat.FACCT_FISCAL_PERIOD_SK,2) != 0
                        and  fat.etl_rec_version_current_flag = 1
                    GROUP BY FSG_ACCOUNT_CODE, FSG_FACCT_LEDGER_SK, FSG_FACCT_CURRENCY_SK, FSG_COST_CENTER_CODE, FSG_ACCT_COMPANY_CODE, CAL_MONTH_START_DAY
                )
                ,CTE_TRANSACTIONS_SUM as
                (
                    -- CALCULATE THE SUM OF TRANSACTION
                    SELECT CAL_MONTH_START_DAY
                        ,DIM_ACCOUNT.code as ACCOUNT_CODE
                        ,fat.FACCT_LEDGER_SK as FACCT_LEDGER_SK
                        ,fat.FACCT_CURRENCY_SK as FACCT_CURRENCY_SK
                        ,DIM_COST_CENTER.code as COST_CENTER_CODE
                        ,DIM_ACCOUNTING_COMPANY.code as ACCT_COMPANY_CODE
                        ,sum(fat.AMOUNT) as FACCT_AMOUNT
                        ,fat.FACCT_FISCAL_PERIOD_SK as FACCT_FISCAL_PERIOD_SK
                        ,max(fat.etl_rec_version_start_date) as src_syst_effective_from
                    FROM stg fat
                    INNER JOIN test_gold.dw_conformed.DIM_TIME ON dim_time.DATE_SK = fat.FACCT_DATE_SK
                    INNER JOIN test_gold.dw_finance.DIM_ACCOUNT ON DIM_ACCOUNT.sk = fat.FACCT_ACCOUNT_SK
                    INNER JOIN test_gold.dw_finance.DIM_COST_CENTER ON DIM_COST_CENTER.sk = fat.FACCT_COST_CENTER_SK
                    INNER JOIN test_gold.dw_finance.DIM_ACCOUNTING_COMPANY ON DIM_ACCOUNTING_COMPANY.sk = fat.FACCT_ACCT_COMPANY_SK
                    WHERE right(fat.FACCT_FISCAL_PERIOD_SK,2) <> 13 and right(fat.FACCT_FISCAL_PERIOD_SK,2) != 0
                    and  fat.etl_rec_version_current_flag = 1
                    GROUP BY CAL_MONTH_START_DAY, ACCOUNT_CODE, FACCT_LEDGER_SK, FACCT_CURRENCY_SK, COST_CENTER_CODE, ACCT_COMPANY_CODE, FACCT_FISCAL_PERIOD_SK
                )
                ,CTE_SUM_TRANS as
                (
                -- ADD THE TRANSACTIONS IN THE CALENDAR
                    select c.DATE_SK AS FACCM_DATE_SK
                        ,FISC_MONTH_KEY_NUM
                        ,c.ACCOUNT_CODE
                        ,max(FSG_ACCOUNT_SK) over (PARTITION BY c.ACCOUNT_CODE,c.FACCT_LEDGER_SK,c.FACCT_CURRENCY_SK,c.COST_CENTER_CODE,c.ACCT_COMPANY_CODE ORDER BY DATE_SK) as FACCM_ACCOUNT_SK
                        ,c.FACCT_LEDGER_SK AS FACCM_LEDGER_SK
                        ,c.FACCT_CURRENCY_SK AS FACCM_CURRENCY_SK
                        ,c.COST_CENTER_CODE
                        ,max(FSG_COST_CENTER_SK) over (PARTITION BY c.ACCOUNT_CODE,c.FACCT_LEDGER_SK,c.FACCT_CURRENCY_SK,c.COST_CENTER_CODE,c.ACCT_COMPANY_CODE ORDER BY DATE_SK) as FACCM_COST_CENTER_SK
                        ,c.ACCT_COMPANY_CODE
                        ,max(FSG_ACCT_COMPANY_SK) over (PARTITION BY c.ACCOUNT_CODE,c.FACCT_LEDGER_SK,c.FACCT_CURRENCY_SK,c.COST_CENTER_CODE,c.ACCT_COMPANY_CODE ORDER BY DATE_SK) as FACCM_ACCT_COMPANY_SK
                        ,trans.FACCT_AMOUNT as FACCM_AMOUNT
                        ,trans.FACCT_FISCAL_PERIOD_SK as FACCM_FISCAL_PERIOD_SK
                        ,SUM(trans.FACCT_AMOUNT) OVER (PARTITION BY c.ACCOUNT_CODE,c.FACCT_LEDGER_SK,c.FACCT_CURRENCY_SK,c.COST_CENTER_CODE,c.ACCT_COMPANY_CODE ORDER BY DATE_SK) as CURRENT_BALANCE
                        ,trans.src_syst_effective_from as src_syst_effective_from
                    FROM CTE_CALENDAR_PERIOD_FACT c
                    LEFT JOIN CTE_TRANSACTIONS_SUM trans on trans.CAL_MONTH_START_DAY = c.date_sk
                                                        and c.ACCOUNT_CODE = trans.ACCOUNT_CODE
                                                        and c.FACCT_LEDGER_SK = trans.FACCT_LEDGER_SK
                                                        and c.FACCT_CURRENCY_SK = trans.FACCT_CURRENCY_SK
                                                        and c.COST_CENTER_CODE = trans.COST_CENTER_CODE
                                                        and c.ACCT_COMPANY_CODE = trans.ACCT_COMPANY_CODE
                    LEFT JOIN CTE_FACT_SK_GROUP sk_group on sk_group.fsg_CAL_MONTH_START_DAY = c.date_sk
                                                        and c.ACCOUNT_CODE = sk_group.fsg_ACCOUNT_CODE
                                                        and c.FACCT_LEDGER_SK = sk_group.fsg_FACCT_LEDGER_SK
                                                        and c.FACCT_CURRENCY_SK = sk_group.fsg_FACCT_CURRENCY_SK
                                                        and c.COST_CENTER_CODE = sk_group.fsg_COST_CENTER_CODE
                                                        and c.ACCT_COMPANY_CODE = sk_group.fsg_ACCT_COMPANY_CODE
                )
                ,final as
                (
                -- P1 to P12, with missing data created for Openning Balance measure
                SELECT actual.ACCOUNT_CODE AS FACCM_ACCOUNT_CODE
                    ,actual.FACCM_ACCOUNT_SK
                    ,'ACT' AS FACCM_SCENARIO_CODE
                    ,actual.FACCM_LEDGER_SK
                    ,actual.FACCM_CURRENCY_SK
                    ,actual.COST_CENTER_CODE AS FACCM_COST_CENTER_CODE
                    ,actual.FACCM_COST_CENTER_SK
                    ,actual.ACCT_COMPANY_CODE AS FACCM_ACCT_COMPANY_CODE
                    ,actual.FACCM_ACCT_COMPANY_SK
                    ,actual.FACCM_DATE_SK
                    ,CASE WHEN actual.FACCM_FISCAL_PERIOD_SK IS NOT NULL THEN actual.FACCM_FISCAL_PERIOD_SK
                        WHEN actual.FACCM_FISCAL_PERIOD_SK IS NULL AND FISCAL_YEAR_START_MONTH = 1 THEN CONCAT(FISCAL_YEAR_START_MONTH, LEFT(actual.FACCM_DATE_SK, 6))
                        WHEN actual.FACCM_FISCAL_PERIOD_SK IS NULL AND FISCAL_YEAR_START_MONTH = 4 THEN CONCAT(FISCAL_YEAR_START_MONTH, actual.FISC_MONTH_KEY_NUM)
                    END AS FACCM_FISCAL_PERIOD_SK
                    ,actual.FACCM_AMOUNT
                    ,ifnull(previous.CURRENT_BALANCE,0.00) as FACCM_OPENING_BALANCE
                    ,actual.src_syst_effective_from as src_syst_effective_from
                FROM CTE_SUM_TRANS actual
                LEFT JOIN CTE_SUM_TRANS previous on actual.ACCOUNT_CODE = previous.ACCOUNT_CODE
                                                AND actual.FACCM_LEDGER_SK = previous.FACCM_LEDGER_SK
                                                AND actual.FACCM_CURRENCY_SK = previous.FACCM_CURRENCY_SK
                                                AND actual.COST_CENTER_CODE = previous.COST_CENTER_CODE
                                                AND actual.ACCT_COMPANY_CODE = previous.ACCT_COMPANY_CODE
                                                AND date_format(to_date(actual.FACCM_DATE_SK, 'yyyyMMdd')- interval 1 month,'yyyyMMdd') = previous.FACCM_DATE_SK
                INNER JOIN test_gold.dw_finance.DIM_LEDGER ON dim_ledger.sk = actual.FACCM_LEDGER_SK

                UNION ALL

                -- ADD P13 'as is'
                SELECT dim_account.CODE AS FACCM_ACCOUNT_CODE
                    ,fat.FACCT_ACCOUNT_SK as FACCM_ACCOUNT_SK
                    ,'ACT' AS FACCM_SCENARIO_CODE
                    ,fat.FACCT_LEDGER_SK AS FACCM_LEDGER_SK
                    ,fat.FACCT_CURRENCY_SK AS FACCM_CURRENCY_SK
                    ,dim_COST_CENTER.CODE AS FACCM_COST_CENTER_CODE
                    ,fat.FACCT_COST_CENTER_SK AS FACCM_COST_CENTER_SK
                    ,dim_accounting_company.CODE AS FACCM_ACCT_COMPANY_CODE
                    ,fat.FACCT_ACCT_COMPANY_SK AS FACCM_ACCT_COMPANY_SK
                    ,fat.FACCT_DATE_SK AS FACCM_DATE_SK
                    ,fat.FACCT_FISCAL_PERIOD_SK AS FACCM_FISCAL_PERIOD_SK
                    ,SUM(fat.AMOUNT) AS FACCM_AMOUNT
                    ,0.00 AS FACCM_OPENING_BALANCE
                    ,max(fat.etl_rec_version_start_date)
                FROM stg fat
                LEFT JOIN test_gold.dw_finance.DIM_ACCOUNT ON dim_account.sk = fat.FACCT_ACCOUNT_SK
                LEFT JOIN test_gold.dw_finance.DIM_COST_CENTER ON dim_COST_CENTER.SK = fat.FACCT_COST_CENTER_SK
                LEFT JOIN test_gold.dw_finance.DIM_ACCOUNTING_COMPANY ON DIM_ACCOUNTING_COMPANY.SK = fat.FACCT_ACCT_COMPANY_SK
                WHERE right(fat.FACCT_FISCAL_PERIOD_SK,2) = 13
                and  fat.etl_rec_version_current_flag = 1
                GROUP BY FACCM_ACCOUNT_CODE, FACCM_ACCOUNT_SK,FACCM_LEDGER_SK,FACCM_CURRENCY_SK,FACCM_COST_CENTER_CODE,FACCM_COST_CENTER_SK,FACCM_ACCT_COMPANY_CODE,FACCM_ACCT_COMPANY_SK,FACCM_DATE_SK,FACCM_FISCAL_PERIOD_SK
                )

                ----------------------------------------------------------

                select
                    COALESCE(FACCM_ACCOUNT_SK, -1) as account_sk
                    ,COALESCE(ds.sk, -1) as scenario_sk
                    ,COALESCE(FACCM_LEDGER_SK, -1) as ledger_sk
                    ,COALESCE(FACCM_CURRENCY_SK, -1)  as currency_sk
                    ,COALESCE(FACCM_COST_CENTER_SK, -1) as cost_center_sk
                    ,COALESCE(FACCM_ACCT_COMPANY_SK, -1) as acct_company_sk
                    ,COALESCE(FACCM_DATE_SK, 19750101) as date_sk
                    ,cast(FACCM_OPENING_BALANCE as decimal(18,2)) as opening_balance
                    ,cast(FACCM_AMOUNT as decimal(18,2)) as amount
                    ,bigint(FACCM_FISCAL_PERIOD_SK) as fiscal_period_sk
                    ,etl_rec_version_start_date as src_syst_effective_from

                from final
                left join test_gold.dw_finance.dim_scenario ds
                    on ds.code = final.FACCM_SCENARIO_CODE

        """)

df_step1.createOrReplaceTempView("FACT_ACCOUNTING_MONTHLY_AGG")

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(date_sk) dt1, max(date_sk) dt2 from test_gold.dw_conformed.dim_time --order by date_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Unity Catalog - Gold' as Environment, scenario_sk, count(*) as qty_scenarios from FACT_ACCOUNTING_MONTHLY_AGG group by scenario_sk--where account_sk >=3875

# COMMAND ----------

# DBTITLE 1,truncate FACT_ACCOUNTING_MONTHLY_AGG
# MAGIC %sql
# MAGIC --truncate table test_gold.dw_finance.FACT_ACCOUNTING_MONTHLY_AGG

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Unity Catalog - Gold' as Environment,
# MAGIC   scenario_sk,
# MAGIC   count(*) as qty_scenarios
# MAGIC from test_gold.dw_finance.FACT_ACCOUNTING_MONTHLY_AGG
# MAGIC group by scenario_sk

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.dim_scenario where code in('ACT', 'BDG') or version like'Incremental';

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Unity Catalog - Gold' as Environment, COUNT(*) as qty_rows
# MAGIC from test_gold.dw_finance.FACT_ACCOUNTING_TRANSACTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'Unity Catalog - Gold' as Environment, COUNT(*) as qty_rows
# MAGIC from test_gold.dw_finance.fact_accounting_monthly_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 'GL_JE_Lines' as tableName, etl_file_name, etl_created_datetime from test_silver.oracle_sftp_accounting.GL_JE_Lines where etl_file_name like 'GL_%'
# MAGIC order by 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 'GL_CODE_COMBINATIONS' as tableName, etl_file_name, etl_created_datetime from test_silver.oracle_sftp_accounting.GL_CODE_COMBINATIONS where etl_file_name like 'GL_%'
# MAGIC order by 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct pointofview
# MAGIC from test_silver.pbcs.budget
# MAGIC where src_syst_effective_to is null and (
# MAGIC                                             (
# MAGIC                                                 ((Account like '9%' or Account = 'Tonnage') and pointofview like '%,STAT,%')
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,100R,%' and pointofview like '%,CAD_Reporting,%')
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,200R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,300R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,AUD_Reporting,%'))
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,400R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,EUR_Reporting,%'))
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,500R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,600R,%' and (pointofview like '%,CAD_Reporting,%'))
# MAGIC                                                 or
# MAGIC                                                 (pointofview like '%,801,%' and (pointofview like '%,USD_Reporting,%'))
# MAGIC                                             )
# MAGIC         and pointofview like '%No Customer%'
# MAGIC         and pointofview like '%No Cargo%')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test_gold.dw_finance.fact_accounting_transactions_monthly_agg

# COMMAND ----------

import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, split, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
###################################
########## InitialValues ##########
###################################
# Define schema for the DataFrame
schema = StructType([
    StructField("department", StringType(), True),
    StructField("num", IntegerType(), True),
    StructField("version", StringType(), True),
    StructField("plan_type", StringType(), True),
    StructField("scenario", StringType(), True),
    StructField("month_abbr", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("latest_scenario_flag", IntegerType(), True),
    StructField("show_flag", IntegerType(), True)
])

# Define data
data = [
    ('Commercial', 190001, 'N/A', 'Actual', 'N/A', 'N/A', 'N/A', 1900, 'ACT', 0, 0),
    ('Commercial', 190001, 'Final', 'Budget', 'N/A', 'N/A', 'N/A', 1900, 'BDG', 0, 0),
    ('Commercial', 190000, 'Final', 'Forecast', 'N/A', 'N/A', 'N/A', 1900, 'FCST', 0, 0),
    ('Commercial', 190004, 'Final', 'Forecast', 'N/A', 'N/A', 'April', 1900, 'FCST_APR', 0, 0),
    ('Commercial', 190008, 'Final', 'Forecast', 'N/A', 'N/A', 'August', 1900, 'FCST_AUG', 0, 0),
    ('Commercial', 190012, 'Final', 'Forecast', 'N/A', 'N/A', 'December', 1900, 'FCST_DEC', 0, 0),
    ('Commercial', 190002, 'Final', 'Forecast', 'N/A', 'N/A', 'February', 1900, 'FCST_FEB', 0, 0),
    ('Commercial', 190001, 'Final', 'Forecast', 'N/A', 'N/A', 'January', 1900, 'FCST_JAN', 0, 0),
    ('Commercial', 190007, 'Final', 'Forecast', 'N/A', 'N/A', 'July', 1900, 'FCST_JUL', 0, 0),
    ('Commercial', 190006, 'Final', 'Forecast', 'N/A', 'N/A', 'June', 1900, 'FCST_JUN', 0, 0),
    ('Commercial', 190003, 'Final', 'Forecast', 'N/A', 'N/A', 'March', 1900, 'FCST_MAR', 0, 0),
    ('Commercial', 190005, 'Final', 'Forecast', 'N/A', 'N/A', 'May', 1900, 'FCST_MAY', 0, 0),
    ('Commercial', 190011, 'Final', 'Forecast', 'N/A', 'N/A', 'November', 1900, 'FCST_NOV', 0, 0),
    ('Commercial', 190010, 'Final', 'Forecast', 'N/A', 'N/A', 'October', 1900, 'FCST_OCT', 0, 0),
    ('Commercial', 190009, 'Final', 'Forecast', 'N/A', 'N/A', 'September', 1900, 'FCST_SEP', 0, 0)
]

# Create DataFrame
df_init = spark.createDataFrame(data, schema)

# COMMAND ----------

############################
########## BUDGET ##########
############################
df_scr_budget = spark.sql(f"""

    select distinct pointofview
    from test_silver.pbcs.budget
    where src_syst_effective_to is null and (
    (
        ((Account like '9%' or Account = 'Tonnage') and pointofview like '%,STAT,%')
        or
        (pointofview like '%,100R,%' and pointofview like '%,CAD_Reporting,%')
        or
        (pointofview like '%,200R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
        or
        (pointofview like '%,300R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,AUD_Reporting,%'))
        or
        (pointofview like '%,400R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,EUR_Reporting,%'))
        or
        (pointofview like '%,500R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
        or
        (pointofview like '%,600R,%' and (pointofview like '%,CAD_Reporting,%'))
        or
        (pointofview like '%,801,%' and (pointofview like '%,USD_Reporting,%'))

    )
    and pointofview like '%No Customer%'
    and pointofview like '%No Cargo%')

""")
df_scr_budget.createOrReplaceTempView('src_budget')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from src_budget

# COMMAND ----------

df_spl_budget = spark.sql(f'''

    select f.pointofview

    from src_budget as f
    group by f.pointofview

''')

df_spl_budget = df_spl_budget.withColumn("column_split", split(df_spl_budget["pointofview"], ",")) \
                            .withColumn("Pos1", col("column_split").getItem(0)) \
                            .withColumn("Pos2", col("column_split").getItem(1)) \
                            .withColumn("Pos3", col("column_split").getItem(2)) \
                            .withColumn("Pos4", col("column_split").getItem(3)) \
                            .withColumn("Pos5", col("column_split").getItem(4)) \
                            .withColumn("Pos6", col("column_split").getItem(5)) \
                            .withColumn("Pos7", col("column_split").getItem(6)) \
                            .withColumn("Pos8", col("column_split").getItem(7)) \
                            .withColumn("Pos9", col("column_split").getItem(8)) \
                            .drop("column_split")
df_spl_budget.createOrReplaceTempView('spl_budget')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from spl_budget

# COMMAND ----------

df_budget = spark.sql(f"""

    with sce as
    (
        select distinct
            cast(DATE_FORMAT(current_timestamp(),'yyyyMM') as varchar(6)) as scenario_num
            ,cast(DATE_FORMAT(current_timestamp(),'MMM') as varchar(3)) as scenario_month_abbr
            ,Pos1 as version
            ,'Budget' as plan_type
            ,'Financial' as SCENARIO_DEPARTMENT
        from spl_budget
    )
    ,scenario as
    (
        select
            scenario_num
            ,cast(version as varchar(15)) as version
            ,plan_type
            ,concat(left(scenario_num,4), '-', right(scenario_num,2)) as scenario
            ,scenario_month_abbr as month_abbr
            ,cast(DATE_FORMAT(cast(concat(left(scenario_num,4), '-', right(scenario_num,2), '-01') as timestamp), 'MMMM') as varchar(12)) as month
            ,left(scenario_num,4) as year
            ,case
                when YEAR(current_timestamp()) - left(scenario_num,4) > 1
                    then 0
                else 1
            end as show_flag
            ,SCENARIO_DEPARTMENT
        from sce
    )

    --------------------------------------------------------

    select
        sc.SCENARIO_DEPARTMENT as DEPARTMENT
        , sc.scenario_num as num
        , sc.version as version
        , sc.plan_type as plan_type
        , sc.scenario
        , sc.month_abbr
        , sc.month
        , sc.year
        , 'Budget' as code
        , 1 as latest_scenario_flag
        , sc.show_flag

    from scenario sc

    left join {gold_catalog}.dw_finance.DIM_SCENARIO dsc
        on sc.scenario_num = dsc.num
            and sc.plan_type = dsc.plan_type
            and sc.version =  dsc.version
            and sc.SCENARIO_DEPARTMENT = dsc.DEPARTMENT

""")
print('SUCCESS! df_budget')


##############################
########## FORECAST ##########
##############################
df_src_forecast = spark.sql(f'''

    select distinct pointofview

    from {silver_catalog}.pbcs.forecast

    where src_syst_effective_to is null and (
    (
        ((Account like '9%' or Account = 'Tonnage') and pointofview like '%,STAT,%')
        or
        (pointofview like '%,100R,%' and pointofview like '%,CAD_Reporting,%')
        or
        (pointofview like '%,200R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
        or
        (pointofview like '%,300R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,AUD_Reporting,%'))
        or
        (pointofview like '%,400R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,EUR_Reporting,%'))
        or
        (pointofview like '%,500R,%' and (pointofview like '%,CAD_Reporting,%' or pointofview like '%,USD_Reporting,%'))
        or
        (pointofview like '%,600R,%' and (pointofview like '%,CAD_Reporting,%'))
        or
        (pointofview like '%,801,%' and (pointofview like '%,USD_Reporting,%'))
    )
    and pointofview like '%No Customer%'
    and pointofview like '%No Cargo%')

''')
df_src_forecast.createOrReplaceTempView('src_forecast')


df_spl_forecast = spark.sql(f'''

    select f.pointofview

    from src_forecast as f
    group by f.pointofview

''')

df_spl_forecast = df_spl_forecast.withColumn("column_split", split(df_spl_forecast["pointofview"], ",")) \
                                .withColumn("Pos1", col("column_split").getItem(0)) \
                                .withColumn("Pos2", col("column_split").getItem(1)) \
                                .withColumn("Pos3", col("column_split").getItem(2)) \
                                .withColumn("Pos4", col("column_split").getItem(3)) \
                                .withColumn("Pos5", col("column_split").getItem(4)) \
                                .withColumn("Pos6", col("column_split").getItem(5)) \
                                .withColumn("Pos7", col("column_split").getItem(6)) \
                                .withColumn("Pos8", col("column_split").getItem(7)) \
                                .withColumn("Pos9", col("column_split").getItem(8)) \
                                .drop("column_split")
df_spl_forecast.createOrReplaceTempView('spl_forecast')

df_forecast = spark.sql(f"""

    with sce as
    (
        select distinct
            case
                when MONTH(current_timestamp()) < MONTH(cast(to_date(concat('01-', RIGHT(Pos3, 3), '-1900'), 'dd-MMM-yyy') as timestamp))
                    then concat( ( YEAR(current_timestamp()) -1 )*10, MONTH(cast(date_format(to_date(concat('01-', RIGHT(Pos3, 3), '-1900'), 'dd-MMM-yyyy'), 'yyyy-MM-dd HH:mm:ss') as timestamp) ) )
                else concat( ( YEAR(current_timestamp()))*10, MONTH(cast(date_format(to_date(concat('01-', RIGHT(Pos3, 3), '-1900'), 'dd-MMM-yyy'), 'yyyy-MM-dd HH:mm:ss') as timestamp) ) )
            end as scenario_num
            ,RIGHT(Pos3, 3) as scenario_month_abbr
            ,Pos1 as version
            ,'Forecast' as plan_type
            ,'Financial' as SCENARIO_DEPARTMENT
        from spl_forecast
    )
    ,scenario as
    (
        select
            scenario_num
            ,cast(version as varchar(15)) as version
            ,plan_type
            ,concat(left(scenario_num,4), '-', right(scenario_num,2)) as scenario
            ,scenario_month_abbr as month_abbr
            ,cast(DATE_FORMAT(cast(concat(left(scenario_num,4), '-', right(scenario_num,2), '-01') as timestamp), 'MMMM') as varchar(12)) as month
            ,left(scenario_num,4) as year
            ,case
                when YEAR(current_timestamp()) - left(scenario_num,4) > 1 then 0
                else 1
            end as show_flag
            ,SCENARIO_DEPARTMENT
        from sce
    )

    --------------------------------------------------------

    select
        sc.SCENARIO_DEPARTMENT as DEPARTMENT
        , sc.scenario_num as num
        , sc.version as version
        , sc.plan_type as plan_type
        , sc.scenario
        , sc.month_abbr
        , sc.month
        , sc.year
        , 'Forecast' as code
        , 1 as latest_scenario_flag
        , sc.show_flag

    from scenario sc

    left join {gold_catalog}.dw_finance.DIM_SCENARIO dsc
        on sc.scenario_num = dsc.num
            and sc.plan_type = dsc.plan_type
            and sc.version =  dsc.version
            and sc.SCENARIO_DEPARTMENT = dsc.DEPARTMENT

""")
print('SUCCESS! df_forecast')

df = df_init.union(df_budget) \
            .union(df_forecast) \
            .withColumn("src_syst_effective_from", current_timestamp()) \
            .withColumn("num", col("num").cast("int")) \
            .withColumn("latest_scenario_flag", col("latest_scenario_flag").cast("boolean")) \
            .withColumn("show_flag", col("show_flag").cast("boolean"))

return df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   --cl.column_name AS column_name,
# MAGIC   tc.constraint_name AS constraint_name,
# MAGIC   tc.constraint_type AS constraint_type,
# MAGIC   tc.table_name AS table_name,
# MAGIC   cl.ordinal_position,
# MAGIC   cl.data_type,
# MAGIC   cl.is_nullable,
# MAGIC   tc.constraint_type
# MAGIC FROM system.information_schema.table_constraints tc
# MAGIC     JOIN system.information_schema.constraint_column_usage c ON tc.constraint_name = c.constraint_name
# MAGIC     JOIN system.information_schema.columns cl ON cl.table_name = tc.table_name
# MAGIC WHERE
# MAGIC     tc.table_catalog = 'test_gold'
# MAGIC     AND tc.table_schema = 'dw_procurement'
# MAGIC     AND tc.table_name = 'fact_ship_procurement_agg'
# MAGIC     AND tc.constraint_type = 'FOREIGN KEY'
# MAGIC     ;

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from test_gold.dw_procurement.FACT_ship_PROCUREMENT_AGG
# MAGIC where ORDER_MATERIAL_NET_AMT = 4997.97

# COMMAND ----------

# MAGIC %sql
# MAGIC --select count(distinct order_material_net_amt) as order_material_net_amt from test_gold.dw_procurement.fact_ship_procurement_agg where order_material_net_amt=0.0000
# MAGIC --select count(*) as order_material_net_amt from test_gold.dw_procurement.fact_ship_procurement_agg where order_material_net_amt=0.0001;
# MAGIC
# MAGIC --select max(order_material_net_amt) as Max_order_material_net_amt, min(order_material_net_amt) as Min_order_material_net_amt  from test_gold.dw_procurement.fact_ship_procurement_agg where order_material_net_amt>0;
# MAGIC
# MAGIC select ORDER_MATERIAL_NET_AMT, COUNT(*) as ORDER_MATERIAL_NET_AMT_qty
# MAGIC from test_gold.dw_procurement.fact_ship_procurement_agg
# MAGIC where ORDER_MATERIAL_NET_AMT >= 4943.0000 and ORDER_MATERIAL_NET_AMT <= 12198.0300
# MAGIC group by ORDER_MATERIAL_NET_AMT
# MAGIC order by 1, 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct 'tt' as tt, NB_JOB_DUE_OTHER from test_gold.dw_procurement.FACT_ship_WORK_ORDER_DUE_AGG_SNAPSHOT union
# MAGIC select distinct 'tt2' as tt, nb_job_overdue_other from test_gold.dw_procurement.FACT_ship_WORK_ORDER_DUE_AGG_SNAPSHOT

# COMMAND ----------

def get_count_asql(tb_name: str, db_name: str, return_df: bool = False):
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    db_host = dbutils.secrets.get(scope=scope, key="ASQL-magellan-sqlsrv-hostname")
    tenant_id = dbutils.secrets.get(scope=scope, key="tenantid")
    context = adal.AuthenticationContext(f"https://login.windows.net/{tenant_id}")
    token = context.acquire_token_with_client_credentials(
        "https://database.windows.net/",
        dbutils.secrets.get(scope=scope, key="ASQL-magellan-sqlsrv-app-id"),
        dbutils.secrets.get(scope=scope, key="ASQL-magellan-sqlsrv-app")
    )
    asql_connection_args = {
        "driver": driver,
        "url": db_host,
        "encrypt": "true",
        "hostNameInCertificate": "*.database.windows.net",
        "accessToken": token["accessToken"],
        "databaseName": db_name,
    }
    query = f'select * from {tb_name}'
    df_asql = spark.read.format("jdbc").options(**asql_connection_args).option("query", query).load()
    print(f'{tb_name} --> ASQL ' + str(df_asql.count()) + ' / ASQL Distinct ' + str(df_asql.distinct().count()))
    if return_df == True:
        return df_asql

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - Collect AzureSQL
from library.database.azure_sql import AzureSQL
from pyspark.sql.functions import col
# from pyspark.sql import functions as F

loader = AzureSQL()
db_catalog_name = 'XPTODW'

queryAzure = f'SELECT FVESCS_SUPPL_EP_STATUS_DATE, FVESCS_SUPPL_TA_STATUS_DATE, FVESCS_SUPPL_EP_TO_TA_DAYS_NUM FROM dw.FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT'

df_az = AzureSQL.get_df_from_sql(spark=spark, catalog_azure_name=db_catalog_name, query=queryAzure)
df_az.createOrReplaceTempView('vw_asql_fact_vessel_enqr_supp_cum_snapshot')
df_az.display()
count_asql_notnull = df_az.where(col('FVESCS_SUPPL_EP_TO_TA_DAYS_NUM').isNotNull()).count()
count_asql = df_az.count()
print(f'Total General Rows on AzureSQL: {count_asql}')
print(f'Total Not NULL "FVESCS_SUPPL_EP_TO_TA_DAYS_NUM"  Rows on AzureSQL: {count_asql_notnull}')

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - Collect Unity Catalog
# preparing reference UC Data
df_uc = spark.sql('''
    SELECT SUPPL_EP_STATUS_DATE, SUPPL_TA_STATUS_DATE, SUPPL_EP_TO_TA_DAYS_NUM from test_gold.dw_procurement.fact_ship_enqr_suppl_cumul_snapshot
''')

df_uc.createOrReplaceTempView('vw_uc_fact_ship_enqr_supp_cum_snapshot')
df_uc.display()

count_uc_notnull = df_uc.where(col('SUPPL_EP_TO_TA_DAYS_NUM').isNotNull()).count()
count_uc = df_uc.count()
print(f'Total General Rows on Unity Catalog: {count_uc}')
print(f'Total Not NULL "SUPPL_EP_TO_TA_DAYS_NUM" Rows on Unity Catalog: {count_uc_notnull}')

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - sumary
# preparing reference output data
df_uc_result = spark.sql('''
    select
        uc.SUPPL_EP_STATUS_DATE,
        uc.SUPPL_TA_STATUS_DATE,
        uc.SUPPL_EP_TO_TA_DAYS_NUM,
        az.FVESCS_SUPPL_EP_TO_TA_DAYS_NUM,
        round((ifnull(az.FVESCS_SUPPL_EP_TO_TA_DAYS_NUM,0)-ifnull(uc.SUPPL_EP_TO_TA_DAYS_NUM,0)), 6)  as sub
    from vw_uc_fact_ship_enqr_supp_cum_snapshot uc
        inner join vw_asql_fact_vessel_enqr_supp_cum_snapshot az on az.FVESCS_SUPPL_EP_STATUS_DATE = uc.SUPPL_EP_STATUS_DATE and az.FVESCS_SUPPL_TA_STATUS_DATE = uc.SUPPL_TA_STATUS_DATE
    --where uc.SUPPL_EP_TO_TA_DAYS_NUM is not null or az.FVESCS_SUPPL_EP_TO_TA_DAYS_NUM is not null
    order by sub asc
''')

df_uc_result.createOrReplaceTempView('vw_uc_az')
df_uc_result.display()
count_uc_result = df_uc_result.count()
print(f'Total Not NULL "SUPPL_EP_TO_TA_DAYS_NUM" Rows on Unity Catalog: {count_uc_result}')

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - Max and Min differences
# MAGIC %sql
# MAGIC select max(sub) as MAX_difference_for_SUPPL_EP_TO_TA_DAYS_NUM, min(sub) as MIN_difference_for_SUPPL_EP_TO_TA_DAYS_NUM from vw_uc_az

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - result general
# MAGIC %sql
# MAGIC select results_uc_az.Description_Of_Result, results_uc_az.Qty, concat(round((qty/ (select count(*) from vw_uc_fact_ship_enqr_supp_cum_snapshot))*100,2), '%') as Percent_of_rows_about_general_qty
# MAGIC from(
# MAGIC   select 'qty_rows_general' as Description_Of_Result, count(*) as Qty from vw_uc_fact_ship_enqr_supp_cum_snapshot
# MAGIC   union
# MAGIC   select 'qty_rows_without_SUPPL_EP_TO_TA_DAYS_NUM' as description_of_result, count(*) as qty from vw_uc_fact_ship_enqr_supp_cum_snapshot where SUPPL_EP_TO_TA_DAYS_NUM is null
# MAGIC   union
# MAGIC   select 'qty_rows_with_SUPPL_EP_TO_TA_DAYS_NUM' as description_of_result, count(*) as qty from vw_uc_az
# MAGIC   union
# MAGIC   select 'qty_negative_values' as description_of_result, count(*) as qty_negative_values from vw_uc_az WHERE sub<0.0
# MAGIC   union
# MAGIC   select 'qty_with_first_decimal' as description_of_result, count(*) as first_decimal_values from vw_uc_az WHERE sub>=0.1000
# MAGIC   union
# MAGIC   select 'qty_with_second_decimal' as description_of_result, count(*) as second_decimal_values from vw_uc_az WHERE sub>=0.0100 and sub < 0.1000
# MAGIC   union
# MAGIC   select 'qty_with_third_decimal' as description_of_result, count(*) as third_decimal_values from vw_uc_az WHERE sub>=0.0010 and sub < 0.0100
# MAGIC   union
# MAGIC   select 'qty_with_fourth_decimal' as description_of_result, count(*) as fourth_decimal_values from vw_uc_az WHERE sub>=0.0001 and sub < 0.0010
# MAGIC   union
# MAGIC   select 'qty_with_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az WHERE sub>=0.00001 and sub < 0.00010
# MAGIC   union
# MAGIC   select 'qty_after_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az WHERE sub<0.00001
# MAGIC ) results_uc_az

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_ENQR_SUPPL_CUMUL_SNAPSHOT - result not null field
# MAGIC %sql
# MAGIC select results_uc_az.Description_of_result, results_uc_az.Qty, concat(round((qty/(select count(*) from vw_uc_az))*100,2), '%') as percent_of_rows_about_not_null_qty
# MAGIC from(
# MAGIC   select 'qty_rows_with_SUPPL_EP_TO_TA_DAYS_NUM' as Description_of_result, count(*) as Qty from vw_uc_az
# MAGIC   union
# MAGIC   select 'qty_negative_values' as description_of_result, count(*) as qty_negative_values from vw_uc_az WHERE sub<0.0
# MAGIC   union
# MAGIC   select 'qty_with_first_decimal' as description_of_result, count(*) as first_decimal_values from vw_uc_az WHERE sub>=0.1000
# MAGIC   union
# MAGIC   select 'qty_with_second_decimal' as description_of_result, count(*) as second_decimal_values from vw_uc_az WHERE sub>=0.0100 and sub < 0.1000
# MAGIC   union
# MAGIC   select 'qty_with_third_decimal' as description_of_result, count(*) as third_decimal_values from vw_uc_az WHERE sub>=0.0010 and sub < 0.0100
# MAGIC   union
# MAGIC   select 'qty_with_fourth_decimal' as description_of_result, count(*) as fourth_decimal_values from vw_uc_az WHERE sub>=0.0001 and sub < 0.0010
# MAGIC   union
# MAGIC   select 'qty_with_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az WHERE sub>=0.00001 and sub < 0.00010
# MAGIC   union
# MAGIC   select 'qty_after_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az WHERE sub<0.00001
# MAGIC ) results_uc_az

# COMMAND ----------

# DBTITLE 1,FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT - Azure SQL
from library.database.azure_sql import AzureSQL
from pyspark.sql.functions import col

loader = AzureSQL()
db_catalog_name = 'XPTODW'

queryAzure = f'SELECT * FROM dw.FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT'

df_az_fspcs = AzureSQL.get_df_from_sql(spark=spark, catalog_azure_name=db_catalog_name, query=queryAzure)
df_az_fspcs.createOrReplaceTempView('vw_asql_fact_ship_procurement_cumul_snapshot')
df_az_fspcs.display()
count_asql_notnull = df_az_fspcs.where(col('FVPCS_AUTH_ENQR_TO_PO_DAYS_NUM').isNotNull()).count()
count_asql = df_az_fspcs.count()
print(f'Total General Rows on AzureSQL: {count_asql}')
print(f'Total Not NULL "FVPCS_AUTH_ENQR_TO_PO_DAYS_NUM"  Rows on AzureSQL: {count_asql_notnull}')

# COMMAND ----------

# DBTITLE 1,FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT - Unity Catalog

# preparing reference UC Data
df_uc_fspcs = spark.sql('''
    SELECT * FROM test_gold.dw_procurement.fact_ship_procurement_cumul_snapshot
''')

df_uc_fspcs.createOrReplaceTempView('vw_uc_fact_ship_procurement_cumul_snapshot')
df_uc_fspcs.display()

count_uc_notnull = df_uc_fspcs.where(col('auth_enqr_to_po_days_num').isNotNull()).count()
count_uc = df_uc_fspcs.count()
print(f'Total General Rows on Unity Catalog: {count_uc}')
print(f'Total Not NULL "auth_enqr_to_po_days_num" Rows on Unity Catalog: {count_uc_notnull}')



# COMMAND ----------

# DBTITLE 1,FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT
# preparing reference output data
df_uc_fspcs_result = spark.sql('''
    select
        uc.reqs_first_status_date,
        az.FVPCS_REQS_FIRST_STATUS_DATE,
        uc.requested_delvr_date,
        az.FVPCS_REQUESTED_DELVR_DATE,
        uc.received_delvr_date,
        az.FVPCS_RECEIVED_DELVR_DATE,
        az.FVPCS_AUTH_ENQR_FIRST_STATUS_DATE,
        uc.auth_enqr_first_status_date,
        az.FVPCS_AUTH_ENQR_TO_PO_DAYS_NUM,
        uc.auth_enqr_to_po_days_num, round((ifnull(az.FVPCS_AUTH_ENQR_TO_PO_DAYS_NUM,0)-ifnull(uc.auth_enqr_to_po_days_num,0)), 6) as sub_auth_enqr_to_po_days_num,
        az.FVPCS_ENQR_EO_TO_EP_DAYS_NUM,
        uc.enqr_eo_to_ep_days_num, round((ifnull(az.FVPCS_ENQR_EO_TO_EP_DAYS_NUM,0)-ifnull(uc.enqr_eo_to_ep_days_num,0)), 6) as sub_enqr_eo_to_ep_days_num,
        az.FVPCS_ENQR_EP_TO_TA_DAYS_NUM,
        uc.enqr_ep_to_ta_days_num, round((ifnull(az.FVPCS_ENQR_EP_TO_TA_DAYS_NUM,0)-ifnull(uc.enqr_ep_to_ta_days_num,0)), 6) as sub_enqr_ep_to_ta_days_num,
        az.FVPCS_ENQR_TA_TO_TR_DAYS_NUM,
        uc.enqr_ta_to_tr_days_num, round((ifnull(az.FVPCS_ENQR_TA_TO_TR_DAYS_NUM,0)-ifnull(uc.enqr_ta_to_tr_days_num,0)), 6) as sub_enqr_ta_to_tr_days_num,
        az.FVPCS_ENQR_TO_AUTH_ENQR_DAYS_NUM,
        uc.enqr_to_auth_enqr_days_num, round((ifnull(az.FVPCS_ENQR_TO_AUTH_ENQR_DAYS_NUM,0)-ifnull(uc.enqr_to_auth_enqr_days_num,0)), 6) as sub_enqr_to_auth_enqr_days_num,
        az.FVPCS_PO_DELVR_DAYS_NUM,
        uc.po_delvr_days_num, round((ifnull(az.FVPCS_PO_DELVR_DAYS_NUM,0)-ifnull(uc.po_delvr_days_num,0)), 6) as sub_po_delvr_days_num,
        az.FVPCS_PO_DW_TO_PD_DAYS_NUM,
        uc.po_dw_to_pd_days_num, round((ifnull(az.FVPCS_PO_DW_TO_PD_DAYS_NUM,0)-ifnull(uc.po_dw_to_pd_days_num,0)), 6) as sub_po_dw_to_pd_days_num,
        az.FVPCS_PO_DW_TO_PD_DAYS_NUM,
        uc.po_dw_to_rt_days_num, round((ifnull(az.FVPCS_PO_DW_TO_RT_DAYS_NUM,0)-ifnull(uc.po_dw_to_rt_days_num,0)), 6) as sub_po_dw_to_rt_days_num,
        az.FVPCS_PO_DW_TO_RV_DAYS_NUM,
        uc.po_dw_to_rv_days_num, round((ifnull(az.FVPCS_PO_DW_TO_RV_DAYS_NUM,0)-ifnull(uc.po_dw_to_rv_days_num,0)), 6) as sub_po_dw_to_rv_days_num,
        az.FVPCS_PO_DW_TO_SW_DAYS_NUM,
        uc.po_dw_to_sw_days_num, round((ifnull(az.FVPCS_PO_DW_TO_SW_DAYS_NUM,0)-ifnull(uc.po_dw_to_sw_days_num,0)), 6) as sub_po_dw_to_sw_days_num,
        az.FVPCS_PO_O_TO_PD_DAYS_NUM,
        uc.po_o_to_pd_days_num, round((ifnull(az.FVPCS_PO_O_TO_PD_DAYS_NUM,0)-ifnull(uc.po_o_to_pd_days_num,0)), 6) as sub_po_o_to_pd_days_num,
        az.FVPCS_PO_O_TO_RT_DAYS_NUM,
        uc.po_o_to_rt_days_num, round((ifnull(az.FVPCS_PO_O_TO_RT_DAYS_NUM,0)-ifnull(uc.po_o_to_rt_days_num,0)), 6) as sub_po_o_to_rt_days_num,
        az.FVPCS_PO_O_TO_RV_DAYS_NUM,
        uc.po_o_to_rv_days_num, round((ifnull(az.FVPCS_PO_O_TO_RV_DAYS_NUM,0)-ifnull(uc.po_o_to_rv_days_num,0)), 6) as sub_po_o_to_rv_days_num,
        az.FVPCS_PO_O_TO_SW_DAYS_NUM,
        uc.po_o_to_sw_days_num, round((ifnull(az.FVPCS_PO_O_TO_SW_DAYS_NUM,0)-ifnull(uc.po_o_to_sw_days_num,0)), 6) as sub_po_o_to_sw_days_num,
        az.FVPCS_PO_RECV_DAYS_NUM,
        uc.po_recv_days_num, round((ifnull(az.FVPCS_PO_RECV_DAYS_NUM,0)-ifnull(uc.po_recv_days_num,0)), 6) as sub_po_recv_days_num,
        az.FVPCS_PO_SW_TO_PD_DAYS_NUM,
        uc.po_sw_to_pd_days_num, round((ifnull(az.FVPCS_PO_SW_TO_PD_DAYS_NUM,0)-ifnull(uc.po_sw_to_pd_days_num,0)), 6) as sub_po_sw_to_pd_days_num,
        az.FVPCS_PO_SW_TO_RT_DAYS_NUM,
        uc.po_sw_to_rt_days_num, round((ifnull(az.FVPCS_PO_SW_TO_RT_DAYS_NUM,0)-ifnull(uc.po_sw_to_rt_days_num,0)), 6) as sub_po_sw_to_rt_days_num,
        az.FVPCS_PO_SW_TO_RV_DAYS_NUM,
        uc.po_sw_to_rv_days_num, round((ifnull(az.FVPCS_PO_SW_TO_RV_DAYS_NUM,0)-ifnull(uc.po_sw_to_rv_days_num,0)), 6) as sub_po_sw_to_rv_days_num,
        az.FVPCS_REQS_PL_TO_RE_DAYS_NUM,
        uc.reqs_pl_to_re_days_num, round((ifnull(az.FVPCS_REQS_PL_TO_RE_DAYS_NUM,0)-ifnull(uc.reqs_pl_to_re_days_num,0)), 6) as sub_reqs_pl_to_re_days_num,
        az.FVPCS_REQS_TO_ENQR_DAYS_NUM,
        uc.reqs_to_enqr_days_num, round((ifnull(az.FVPCS_REQS_TO_ENQR_DAYS_NUM,0)-ifnull(uc.reqs_to_enqr_days_num,0)), 6) as sub_reqs_to_enqr_days_num
    from vw_uc_fact_ship_procurement_cumul_snapshot uc
        LEFT join vw_asql_fact_ship_procurement_cumul_snapshot az on az.FVPCS_REQS_FIRST_STATUS_DATE = uc.reqs_first_status_date
            and az.FVPCS_REQUESTED_DELVR_DATE = uc.requested_delvr_date
            and az.FVPCS_RECEIVED_DELVR_DATE = uc.received_delvr_date
            and az.FVPCS_AUTH_ENQR_FIRST_STATUS_DATE = uc.auth_enqr_first_status_date
            and az.FVPCS_EXPECTED_DELVR_DATE = uc.EXPECTED_DELVR_DATE
            and az.FVPCS_ENQR_FIRST_STATUS_DATE = uc.ENQR_FIRST_STATUS_DATE
            and az.FVPCS_ENQR_EO_STATUS_DATE = uc.ENQR_EO_STATUS_DATE
            and az.FVPCS_ENQR_EP_STATUS_DATE = uc.ENQR_EP_STATUS_DATE
            and az.FVPCS_ENQR_TA_STATUS_DATE = uc.ENQR_TA_STATUS_DATE
            and az.FVPCS_ENQR_TR_STATUS_DATE = uc.ENQR_TR_STATUS_DATE
            and az.FVPCS_PO_FIRST_STATUS_DATE = uc.PO_FIRST_STATUS_DATE
            and az.FVPCS_PO_DW_STATUS_DATE = uc.PO_DW_STATUS_DATE
            and az.FVPCS_PO_O_STATUS_DATE = uc.PO_O_STATUS_DATE
            and az.FVPCS_PO_SW_STATUS_DATE = uc.PO_SW_STATUS_DATE
            and az.FVPCS_PO_RV_STATUS_DATE = uc.PO_RV_STATUS_DATE
            and az.FVPCS_PO_RT_STATUS_DATE = uc.PO_RT_STATUS_DATE
            and az.FVPCS_PO_PD_STATUS_DATE = uc.PO_PD_STATUS_DATE
    order by 2 asc, 4 asc
''')

df_uc_fspcs_result.createOrReplaceTempView('vw_uc_az_fspcs')
df_uc_fspcs_result.display()
count_uc_result = df_uc_fspcs_result.count()
print(f'Total Not NULL "auth_enqr_to_po_days_num" Rows on Unity Catalog: {count_uc_result}')

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT - Max and Min differences
# MAGIC %sql
# MAGIC select 'AUTH_ENQR_TO_PO_DAYS_NUM' as FieldDescription, max(sub_auth_enqr_to_po_days_num) as MAX_difference_for_Field, min(sub_auth_enqr_to_po_days_num) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'ENQR_EO_TO_EP_DAYS_NUM' as FieldDescription, max(sub_ENQR_EO_TO_EP_DAYS_NUM) as MAX_difference_for_Field, min(sub_ENQR_EO_TO_EP_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'ENQR_EP_TO_TA_DAYS_NUM' as FieldDescription, max(sub_ENQR_EP_TO_TA_DAYS_NUM) as MAX_difference_for_Field, min(sub_ENQR_EP_TO_TA_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'ENQR_TA_TO_TR_DAYS_NUM' as FieldDescription, max(sub_ENQR_TA_TO_TR_DAYS_NUM) as MAX_difference_for_Field, min(sub_ENQR_TA_TO_TR_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'ENQR_TO_AUTH_ENQR_DAYS_NUM' as FieldDescription, max(sub_ENQR_TO_AUTH_ENQR_DAYS_NUM) as MAX_difference_for_Field, min(sub_ENQR_TO_AUTH_ENQR_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_DELVR_DAYS_NUM' as FieldDescription, max(sub_PO_DELVR_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_DELVR_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_DW_TO_PD_DAYS_NUM' as FieldDescription, max(sub_PO_DW_TO_PD_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_DW_TO_PD_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_DW_TO_RT_DAYS_NUM' as FieldDescription, max(sub_PO_DW_TO_RT_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_DW_TO_RT_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_DW_TO_RV_DAYS_NUM' as FieldDescription, max(sub_PO_DW_TO_RV_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_DW_TO_RV_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_DW_TO_SW_DAYS_NUM' as FieldDescription, max(sub_PO_DW_TO_SW_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_DW_TO_SW_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_O_TO_PD_DAYS_NUM' as FieldDescription, max(sub_PO_O_TO_PD_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_O_TO_PD_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_O_TO_RT_DAYS_NUM' as FieldDescription, max(sub_PO_O_TO_RT_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_O_TO_RT_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_O_TO_RV_DAYS_NUM' as FieldDescription, max(sub_PO_O_TO_RV_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_O_TO_RV_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_O_TO_SW_DAYS_NUM' as FieldDescription, max(sub_PO_O_TO_SW_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_O_TO_SW_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_RECV_DAYS_NUM' as FieldDescription, max(sub_PO_RECV_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_RECV_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_SW_TO_PD_DAYS_NUM' as FieldDescription, max(sub_PO_SW_TO_PD_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_SW_TO_PD_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_SW_TO_RT_DAYS_NUM' as FieldDescription, max(sub_PO_SW_TO_RT_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_SW_TO_RT_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'PO_SW_TO_RV_DAYS_NUM' as FieldDescription, max(sub_PO_SW_TO_RV_DAYS_NUM) as MAX_difference_for_Field, min(sub_PO_SW_TO_RV_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'REQS_PL_TO_RE_DAYS_NUM' as FieldDescription, max(sub_REQS_PL_TO_RE_DAYS_NUM) as MAX_difference_for_Field, min(sub_REQS_PL_TO_RE_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC union
# MAGIC select 'REQS_TO_ENQR_DAYS_NUM' as FieldDescription, max(sub_REQS_TO_ENQR_DAYS_NUM) as MAX_difference_for_Field, min(sub_REQS_TO_ENQR_DAYS_NUM) as MIN_difference_for_Field from vw_uc_az_fspcs
# MAGIC

# COMMAND ----------

# DBTITLE 1,Validation for FACT_VESSEL_PROCUREMENT_CUMUL_SNAPSHOT - result general
# MAGIC %sql
# MAGIC select results_uc_az.Description_Of_Result, results_uc_az.Qty, concat(round((qty/ (select count(*) from vw_asql_fact_ship_procurement_cumul_snapshot))*100,2), '%') as Percent_of_rows_about_general_qty
# MAGIC from(
# MAGIC   select 'qty_rows_general' as Description_Of_Result, count(*) as Qty from vw_asql_fact_ship_procurement_cumul_snapshot
# MAGIC   union
# MAGIC   select 'qty_rows_without_SUPPL_EP_TO_TA_DAYS_NUM' as description_of_result, count(*) as qty from vw_uc_fact_ship_procurement_cumul_snapshot where auth_enqr_to_po_days_num is null
# MAGIC   union
# MAGIC   select 'qty_rows_with_SUPPL_EP_TO_TA_DAYS_NUM' as description_of_result, count(*) as qty from vw_uc_az_fspcs
# MAGIC   union
# MAGIC   select 'qty_negative_values' as description_of_result, count(*) as qty_negative_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num<0.0
# MAGIC   union
# MAGIC   select 'qty_before_first_decimal' as description_of_result, count(*) as first_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=1.0000
# MAGIC   union
# MAGIC   select 'qty_with_first_decimal' as description_of_result, count(*) as first_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=0.1000 AND sub_auth_enqr_to_po_days_num < 1.00000
# MAGIC   union
# MAGIC   select 'qty_with_second_decimal' as description_of_result, count(*) as second_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=0.0100 and sub_auth_enqr_to_po_days_num < 0.1000
# MAGIC   union
# MAGIC   select 'qty_with_third_decimal' as description_of_result, count(*) as third_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=0.0010 and sub_auth_enqr_to_po_days_num < 0.0100
# MAGIC   union
# MAGIC   select 'qty_with_fourth_decimal' as description_of_result, count(*) as fourth_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=0.0001 and sub_auth_enqr_to_po_days_num < 0.0010
# MAGIC   union
# MAGIC   select 'qty_with_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num>=0.00001 and sub_auth_enqr_to_po_days_num < 0.00010
# MAGIC   union
# MAGIC   select 'qty_after_fifth_decimal' as description_of_result, count(*) as fifth_decimal_values from vw_uc_az_fspcs WHERE sub_auth_enqr_to_po_days_num<0.00001
# MAGIC ) results_uc_az
