# Databricks notebook source
# MAGIC %md ### The main notebook where the tests are implemented

# COMMAND ----------

# Defining the pre-requisites for all the steps
core_notebook = "landing_to_bronze"
notebook_execution_timeout = 60 * 180
results = []

# COMMAND ----------

# MAGIC %md ### Run Tests for actcateg

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/actcateg",
    "bronze_data": "imos_datalake_api/actcateg/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/actcateg.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_actcateg",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for basecurr

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/basecurr",
    "bronze_data": "imos_datalake_api/basecurr/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/basecurr.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_basecurr",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for bill

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/bill",
    "bronze_data": "imos_datalake_api/bill/delta",
    "primary_keys": "transNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bill.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_bill",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for billdet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/billdet",
    "bronze_data": "imos_datalake_api/billdet/delta",
    "primary_keys": "transNo, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/billdet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_billdet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for bnklift

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/bnklift",
    "bronze_data": "imos_datalake_api/bnklift/delta",
    "primary_keys": "liftingId",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bnklift.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_bnklift",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for brkvest

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/brkvest",
    "bronze_data": "imos_datalake_api/brkvest/delta",
    "primary_keys": "vsl_voy_id, cargoSeq, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/brkvest.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_brkvest",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for bsrule

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/bsrule",
    "bronze_data": "imos_datalake_api/bsrule/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/bsrule.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_bsrule",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for cabv

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/cabv",
    "bronze_data": "imos_datalake_api/cabv/delta",
    "primary_keys": "vtype, short_name",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cabv.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_cabv",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for carbrk

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/carbrk",
    "bronze_data": "imos_datalake_api/carbrk/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/carbrk.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_carbrk",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for cargo

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/cargo",
    "bronze_data": "imos_datalake_api/cargo/delta",
    "primary_keys": "short_name",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cargo.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_cargo",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for cargotp

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/cargotp",
    "bronze_data": "imos_datalake_api/cargotp/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cargotp.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_cargotp",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for coaperiod

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/coaperiod",
    "bronze_data": "imos_datalake_api/coaperiod/delta",
    "primary_keys": "coaId, seqNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/coaperiod.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_coaperiod",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for company

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/company",
    "bronze_data": "imos_datalake_api/company/delta",
    "primary_keys": "code, major",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/company.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_company",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for cpmfuel

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/cpmfuel",
    "bronze_data": "imos_datalake_api/cpmfuel/delta",
    "primary_keys": "vslCode, voyNo, portCallSeq, gmt, fuelType",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cpmfuel.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_cpmfuel",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for cpmnoon

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/cpmnoon",
    "bronze_data": "imos_datalake_api/cpmnoon/delta",
    "primary_keys": "vslCode, voyNo, portCallSeq, gmt, reportType",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/cpmnoon.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_cpmnoon",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for demitin

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/demitin",
    "bronze_data": "imos_datalake_api/demitin/delta",
    "primary_keys": "calcId, ItinSeq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/demitin.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_demitin",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for demlayti

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/demlayti",
    "bronze_data": "imos_datalake_api/demlayti/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/demlayti.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_demlayti",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for dept

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/dept",
    "bronze_data": "imos_datalake_api/dept/delta",
    "primary_keys": "deptNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/dept.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_dept",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for expvest

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/expvest",
    "bronze_data": "imos_datalake_api/expvest/delta",
    "primary_keys": "vsl_voy_id, vtype, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/expvest.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_expvest",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for fixrmkw

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/fixrmkw",
    "bronze_data": "imos_datalake_api/fixrmkw/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/fixrmkw.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_fixrmkw",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for frtinv

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/frtinv",
    "bronze_data": "imos_datalake_api/frtinv/delta",
    "primary_keys": "vslCode, voyNo, transNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/frtinv.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_frtinv",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for fueltype

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/fueltype",
    "bronze_data": "imos_datalake_api/fueltype/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/fueltype.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_fueltype",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gcargo

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gcargo",
    "bronze_data": "imos_datalake_api/gcargo/delta",
    "primary_keys": "cargoId",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gcargo.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gcargo",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gconitn

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gconitn",
    "bronze_data": "imos_datalake_api/gconitn/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gconitn.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gconitn",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gcontrct

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gcontrct",
    "bronze_data": "imos_datalake_api/gcontrct/delta",
    "primary_keys": "coaId",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gcontrct.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gcontrct",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gvsl

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gvsl",
    "bronze_data": "imos_datalake_api/gvsl/delta",
    "primary_keys": "vsl_code",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsl.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gvsl",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gvsldtl

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gvsldtl",
    "bronze_data": "imos_datalake_api/gvsldtl/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsldtl.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gvsldtl",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gvslpcons

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gvslpcons",
    "bronze_data": "imos_datalake_api/gvslpcons/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvslpcons.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gvslpcons",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for indexrate

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/indexrate",
    "bronze_data": "imos_datalake_api/indexrate/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/indexrate.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_indexrate",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for indexstruct

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/indexstruct",
    "bronze_data": "imos_datalake_api/indexstruct/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/indexstruct.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_indexstruct",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for inqdtl

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/inqdtl",
    "bronze_data": "imos_datalake_api/inqdtl/delta",
    "primary_keys": "requestId",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/inqdtl.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_inqdtl",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for invdet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/invdet",
    "bronze_data": "imos_datalake_api/invdet/delta",
    "primary_keys": "trans_no, seqNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/invdet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_invdet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for invoice

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/invoice",
    "bronze_data": "imos_datalake_api/invoice/delta",
    "primary_keys": "trans_no",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/invoice.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_invoice",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for itinvest

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/itinvest",
    "bronze_data": "imos_datalake_api/itinvest/delta",
    "primary_keys": "vsl_voy_id, num",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/itinvest.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_itinvest",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for lob

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/lob",
    "bronze_data": "imos_datalake_api/lob/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/lob.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_lob",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for mledger

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/mledger",
    "bronze_data": "imos_datalake_api/mledger/delta",
    "primary_keys": "code",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/mledger.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_mledger",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for noonmov1

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/noonmov1",
    "bronze_data": "imos_datalake_api/noonmov1/delta",
    "primary_keys": "vslCode, voyNo, seq_no_int, gmt, vfrom, delaySeq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/noonmov1.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_noonmov1",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for paydet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/paydet",
    "bronze_data": "imos_datalake_api/paydet/delta",
    "primary_keys": "pay_trans, trans_no, seqNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/paydet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_paydet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for payment

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/payment",
    "bronze_data": "imos_datalake_api/payment/delta",
    "primary_keys": "pay_trans",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/payment.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_payment",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for payterms

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/payterms",
    "bronze_data": "imos_datalake_api/payterms/delta",
    "primary_keys": "code",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/payterms.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_payterms",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for portcar

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/portcar",
    "bronze_data": "imos_datalake_api/portcar/delta",
    "primary_keys": "vsl_code, voyage, seq_no, berth_seq_no, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portcar.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_portcar",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for portexpstd

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/portexpstd",
    "bronze_data": "imos_datalake_api/portexpstd/delta",
    "primary_keys": "id",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portexpstd.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_portexpstd",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for portexpstddet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/portexpstddet",
    "bronze_data": "imos_datalake_api/portexpstddet/delta",
    "primary_keys": "portExpStdId, orderNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portexpstddet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_portexpstddet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for portfunc

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/portfunc",
    "bronze_data": "imos_datalake_api/portfunc/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/portfunc.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_portfunc",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for preason

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/preason",
    "bronze_data": "imos_datalake_api/preason/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/preason.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_preason",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for purunit

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/purunit",
    "bronze_data": "imos_datalake_api/purunit/delta",
    "primary_keys": "code",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/purunit.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_purunit",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for rnports

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/rnports",
    "bronze_data": "imos_datalake_api/rnports/delta",
    "primary_keys": "no",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/rnports.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_rnports",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for s4member

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/s4member",
    "bronze_data": "imos_datalake_api/s4member/delta",
    "primary_keys": "userNo, groupNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4member.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_s4member",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for s4objacc

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/s4objacc",
    "bronze_data": "imos_datalake_api/s4objacc/delta",
    "primary_keys": "userNo, objId, objType",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4objacc.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_s4objacc",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for s4user

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/s4user",
    "bronze_data": "imos_datalake_api/s4user/delta",
    "primary_keys": "userNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/s4user.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_s4user",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for schrmkw

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/schrmkw",
    "bronze_data": "imos_datalake_api/schrmkw/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/schrmkw.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_schrmkw",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for ssdlist

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/ssdlist",
    "bronze_data": "imos_datalake_api/ssdlist/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/ssdlist.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_ssdlist",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for ssdrsn

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/ssdrsn",
    "bronze_data": "imos_datalake_api/ssdrsn/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/ssdrsn.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_ssdrsn",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for tccomm

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/tccomm",
    "bronze_data": "imos_datalake_api/tccomm/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tccomm.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_tccomm",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for tcform

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/tcform",
    "bronze_data": "imos_datalake_api/tcform/delta",
    "primary_keys": "tcCode",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tcform.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_tcform",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for tchire

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/tchire",
    "bronze_data": "imos_datalake_api/tchire/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tchire.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_tchire",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for tuitin

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/tuitin",
    "bronze_data": "imos_datalake_api/tuitin/delta",
    "primary_keys": "cargoId, seqNo",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/tuitin.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_tuitin",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for type

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/type",
    "bronze_data": "imos_datalake_api/type/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/type.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_type",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for userdata

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/userdata",
    "bronze_data": "imos_datalake_api/userdata/delta",
    "primary_keys": "objType, objID, fieldID",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/userdata.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_userdata",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for userfield

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/userfield",
    "bronze_data": "imos_datalake_api/userfield/delta",
    "primary_keys": "fieldID",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/userfield.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_userfield",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vcarvest

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vcarvest",
    "bronze_data": "imos_datalake_api/vcarvest/delta",
    "primary_keys": "vsl_voy_id, cargoSeq, status",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vcarvest.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vcarvest",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vdeppact

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vdeppact",
    "bronze_data": "imos_datalake_api/vdeppact/delta",
    "primary_keys": "vslCode, voyageNum, portCallNum, vlineNo, activityTime, equCode",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vdeppact.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vdeppact",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vest

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vest",
    "bronze_data": "imos_datalake_api/vest/delta",
    "primary_keys": "vsl_voy_id",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vest.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vest",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vestportexp

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vestportexp",
    "bronze_data": "imos_datalake_api/vestportexp/delta",
    "primary_keys": "estimateId, itinSeq, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vestportexp.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vestportexp",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voyage

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voyage",
    "bronze_data": "imos_datalake_api/voyage/delta",
    "primary_keys": "vslCode, voyNum",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voyage.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voyage",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voypnl

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voypnl",
    "bronze_data": "imos_datalake_api/voypnl/delta",
    "primary_keys": "vslCode, voyNo, snapshotType, snapshotRefDate, data_type",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnl.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voypnl",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voypnlbnkr

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voypnlbnkr",
    "bronze_data": "imos_datalake_api/voypnlbnkr/delta",
    "primary_keys": "vslCode, voyNo, seq, snapshotType, snapshotRefDate",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnlbnkr.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voypnlbnkr",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voypnldet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voypnldet",
    "bronze_data": "imos_datalake_api/voypnldet/delta",
    "primary_keys": "vslCode, voyNo, snapshotType, snapshotRefDate, data_type, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnldet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voypnldet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voypnldrill

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voypnldrill",
    "bronze_data": "imos_datalake_api/voypnldrill/delta",
    "primary_keys": "vslCode, voyNo, snapshotType, snapshotRefDate, data_type, seq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnldrill.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voypnldrill",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for voypnlitin

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/voypnlitin",
    "bronze_data": "imos_datalake_api/voypnlitin/delta",
    "primary_keys": "vslCode, snapshotType, snapshotRefDate, data_type, voyNo, portCallSeq",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/voypnlitin.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_voypnlitin",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vpcost

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vpcost",
    "bronze_data": "imos_datalake_api/vpcost/delta",
    "primary_keys": "vsl_code, voy_no_int, seq_no, agent, port_no",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vpcost.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vpcost",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vschbth

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vschbth",
    "bronze_data": "imos_datalake_api/vschbth/delta",
    "primary_keys": "vsl_code, voy_no_int, seq_no_int, berth_seq_no",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vschbth.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vschbth",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for vsched

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/vsched",
    "bronze_data": "imos_datalake_api/vsched/delta",
    "primary_keys": "ves_code, voy_no_int, seq_no_int",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/vsched.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_vsched",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for berth

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/berth",
    "bronze_data": "imos_datalake_api/berth/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/berth.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_berth",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for gvsldft

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/gvsldft",
    "bronze_data": "imos_datalake_api/gvsldft/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/gvsldft.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_gvsldft",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for loadlinezonedet

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/loadlinezonedet",
    "bronze_data": "imos_datalake_api/loadlinezonedet/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/loadlinezonedet.json",
    "format": "json",
    "options": '{"mode": "PERMISSIVE"}',
    "source_identifier": "imos_loadlinezonedet",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

# MAGIC %md ### Run Tests for nc

# COMMAND ----------

execution_parameters = {
    "landing_data": "imos_datalake_api/nc",
    "bronze_data": "imos_datalake_api/nc/delta",
    "primary_keys": "_sqlid",
    "datatypes_definition_file": "/resources/schemas/imos_datalake_api/nc.json",
    "format": "json",
    "options": '{"multiline": "true", "mode": "PERMISSIVE"}',
    "source_identifier": "imos_nc",
}

results.append(dbutils.notebook.run(core_notebook, notebook_execution_timeout, execution_parameters))
print(results[-1])

# COMMAND ----------

final_text = f"{len([True for x in results if 'SUCCEEDED' in x])}/{len(results)} of the GE tests succeeded"
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]

print(final_text)
dbutils.notebook.exit(f'"{notebook_name} --> {final_text}"')
