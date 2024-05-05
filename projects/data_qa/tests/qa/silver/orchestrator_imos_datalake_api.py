# Databricks notebook source
# MAGIC %md 
# MAGIC ###Defining the pre-requisites for all the steps

# COMMAND ----------

# DBTITLE 1,Pre-requisites for all the steps
core_notebook = "bronze_to_silver"
notebook_execution_timeout = 60 * 60
results = []

# COMMAND ----------

# MAGIC %md ### Run Tests for `actcateg`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "actcateg",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "actcateg",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/actcateg.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `basecurr`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "basecurr",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "basecurr",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/basecurr.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `berth`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "berth",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "berth",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/berth.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `bill`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "bill",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "bill",
    "list_pk_cols": "transno",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bill.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `billdet`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "billdet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "billdet",
    "list_pk_cols": "transno, seq",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/billdet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `bnklift`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "bnklift",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "bnklift",
    "list_pk_cols": "liftingid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bnklift.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `brkvest`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "brkvest",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "brkvest",
    "list_pk_cols": "vsl_voy_id, cargoseq, seq",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/brkvest.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `bsrule`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "bsrule",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "bsrule",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bsrule.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `cabv`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "cabv",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "cabv",
    "list_pk_cols": "vtype, short_name",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cabv.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `carbrk`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "carbrk",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "carbrk",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/carbrk.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `cargo`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "cargo",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "cargo",
    "list_pk_cols": "short_name",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cargo.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `cargotp`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "cargotp",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "cargotp",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cargotp.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `coaperiod`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "coaperiod",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "coaperiod",
    "list_pk_cols": "coaid, seqno",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/coaperiod.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `company` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "company",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "company",
    "list_pk_cols": "code, major",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/company.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `cpmfuel`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "cpmfuel",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "cpmfuel",
    "list_pk_cols": "vslcode, voyno, portcallseq, gmt, fueltype",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cpmfuel.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `cpmnoon` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "cpmnoon",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "cpmnoon",
    "list_pk_cols": "vslcode, voyno, portcallseq, gmt, reporttype",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cpmnoon.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `demitin` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "demitin",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "demitin",
    "list_pk_cols": "calcid, itinseq",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/demitin.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `demlayti` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "demlayti",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "demlayti",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/demlayti.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `dept` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "dept",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "dept",
    "list_pk_cols": "deptno",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/dept.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `expvest` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "expvest",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "expvest",
    "list_pk_cols": "vsl_voy_id, vtype, seq",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/expvest.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `fixrmkw` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "fixrmkw",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "fixrmkw",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/fixrmkw.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `frtinv` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "frtinv",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "frtinv",
    "list_pk_cols": "vslcode, voyno, transno",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/frtinv.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `fueltype` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "fueltype",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "fueltype",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/fueltype.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gcargo` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gcargo",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gcargo",
    "list_pk_cols": "cargoid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gcargo.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gconitn`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gconitn",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gconitn",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gconitn.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gcontrct`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gcontrct",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gcontrct",
    "list_pk_cols": "coaid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gcontrct.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gvsl` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gvsl",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gvsl",
    "list_pk_cols": "vsl_code",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsl.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gvsldft`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gvsldft",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gvsldft",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsldft.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gvsldtl`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gvsldtl",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gvsldtl",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsldtl.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `gvslpcons`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "gvslpcons",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "gvslpcons",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvslpcons.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `indexrate` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "indexrate",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "indexrate",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/indexrate.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `indexstruct` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "indexstruct",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "indexstruct",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/indexstruct.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `inqdtl` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "inqdtl",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "inqdtl",
    "list_pk_cols": "requestid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/inqdtl.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `invdet`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "invdet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "invdet",
    "list_pk_cols": "trans_no, seqno",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/invdet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `invoice` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "invoice",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "invoice",
    "list_pk_cols": "trans_no",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/invoice.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `itinvest` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "itinvest",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "itinvest",
    "list_pk_cols": "vsl_voy_id, num",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/itinvest.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `loadlinezonedet` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "loadlinezonedet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "loadlinezonedet",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/loadlinezonedet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `lob` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "lob",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "lob",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/lob.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `mledger` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "mledger",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "mledger",
    "list_pk_cols": "code",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/mledger.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `nc` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "nc",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "nc",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/nc.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `noonmov1` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "noonmov1",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "noonmov1",
    "list_pk_cols": "vslcode, voyno, seq_no_int, gmt, vfrom, delayseq",
    "list_order_by_cols": "lastupdategmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/noonmov1.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `paydet` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "paydet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "paydet",
    "list_pk_cols": "pay_trans, trans_no, seqno",
    "list_order_by_cols": "last_update_gmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/paydet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `payment` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "payment",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "payment",
    "list_pk_cols": "pay_trans",
    "list_order_by_cols": "last_update_gmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/payment.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `payterms` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "payterms",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "payterms",
    "list_pk_cols": "code",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/payterms.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `portcar` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "portcar",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "portcar",
    "list_pk_cols": "vsl_code, voyage, seq_no, berth_seq_no, seq",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portcar.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `portexpstd` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "portexpstd",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "portexpstd",
    "list_pk_cols": "id",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portexpstd.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `portexpstddet` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "portexpstddet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "portexpstddet",
    "list_pk_cols": "portexpstdid, orderno",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portexpstddet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `portfunc` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "portfunc",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "portfunc",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portfunc.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `preason`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "preason",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "preason",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/preason.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `purunit`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "purunit",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "purunit",
    "list_pk_cols": "code",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/purunit.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `rnports` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "rnports",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "rnports",
    "list_pk_cols": "no",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/rnports.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `s4member` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "s4member",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "s4member",
    "list_pk_cols": "userno, groupno",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4member.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `s4objacc` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "s4objacc",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "s4objacc",
    "list_pk_cols": "userno, objid, objtype",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4objacc.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `s4user` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "s4user",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "s4user",
    "list_pk_cols": "userno",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4user.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `schrmkw` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "schrmkw",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "schrmkw",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/schrmkw.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `ssdlist`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "ssdlist",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "ssdlist",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/ssdlist.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `ssdrsn` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "ssdrsn",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "ssdrsn",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/ssdrsn.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `tccomm` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "tccomm",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "tccomm",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdategmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tccomm.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `tcform` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "tcform",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "tcform",
    "list_pk_cols": "tccode",
    "list_order_by_cols": "lastupdategmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tcform.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `tchire`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "tchire",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "tchire",
    "list_pk_cols": "tccode",
    "list_order_by_cols": "lastupdategmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tchire.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `tuitin` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "tuitin",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "tuitin",
    "list_pk_cols": "cargoid, seqno",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tuitin.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `type`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "type",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "type",
    "list_pk_cols": "_sqlid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/type.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `userdata`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "userdata",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "userdata",
    "list_pk_cols": "objtype, objid, fieldid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/userdata.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `userfield`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "userfield",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "userfield",
    "list_pk_cols": "fieldid",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/userfield.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vcarvest`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vcarvest",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vcarvest",
    "list_pk_cols": "vsl_voy_id, cargoseq, status",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vcarvest.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vdeppact`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vdeppact",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vdeppact",
    "list_pk_cols": "vslcode, voyagenum, portcallnum, vlineno, activitytime, equcode",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vdeppact.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vest` 

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vest",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vest",
    "list_pk_cols": "vsl_voy_id",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vest.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vestportexp`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vestportexp",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vestportexp",
    "list_pk_cols": "estimateid, itinseq, seq",
    "list_order_by_cols": "lastupdategmt",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vestportexp.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voyage`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voyage",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voyage",
    "list_pk_cols": "vslcode, voynum",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voyage.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voypnl`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voypnl",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voypnl",
    "list_pk_cols": "vslcode, voyno, snapshottype, snapshotrefdate, data_type",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnl.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voypnlbnkr`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voypnlbnkr",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voypnlbnkr",
    "list_pk_cols": "vslcode, voyno, seq, snapshottype, snapshotrefdate",
    "list_order_by_cols": "",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnlbnkr.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voypnldet`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voypnldet",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voypnldet",
    "list_pk_cols": "vslcode, voyno, snapshottype, snapshotrefdate, data_type, seq",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnldet.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voypnldrill`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voypnldrill",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voypnldrill",
    "list_pk_cols": "vslcode, voyno, snapshottype, snapshotrefdate, data_type, seq",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnldrill.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `voypnlitin`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "voypnlitin",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "voypnlitin",
    "list_pk_cols": "vslcode, snapshottype, snapshotrefdate, data_type, voyno, portcallseq",
    "list_order_by_cols": "vupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnlitin.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vpcost`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vpcost",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vpcost",
    "list_pk_cols": "vsl_code, voy_no_int, seq_no, agent, port_no",
    "list_order_by_cols": "last_update",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vpcost.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vschbth`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vschbth",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vschbth",
    "list_pk_cols": "vsl_code, voy_no_int, seq_no_int, berth_seq_no",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vschbth.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for `vsched`

# COMMAND ----------

execution_parameters = {
    "schema_bronze_name": "imos_datalake_api",
    "table_bronze_name": "vsched",
    "schema_silver_name": "imos_datalake_api",
    "table_silver_name": "vsched",
    "list_pk_cols": "ves_code, voy_no_int, seq_no_int",
    "list_order_by_cols": "lastupdate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vsched.json"
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

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
# MAGIC ---
