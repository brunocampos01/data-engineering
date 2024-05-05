# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC %rehashx # clear cache
# MAGIC import os
# MAGIC from data_catalog.src.catalog_creator.creator import Creator

# COMMAND ----------

# MAGIC %md
# MAGIC ## Debug

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("layer_name", "gold")
dbutils.widgets.text("owner", "bruno.campos@cslships.com")
dbutils.widgets.text("create_catalog_and_dbs", "True")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Parameters

# COMMAND ----------

layer_name = dbutils.widgets.get("layer_name")
owner = dbutils.widgets.get("owner")
storage_account = os.getenv("magellanADLS").lower()
create_catalog_and_dbs = dbutils.widgets.get("create_catalog_and_dbs")

catalog_creator = Creator(
    spark=spark,
    layer_name=layer_name,
    storage_account=storage_account,
    owner=owner,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

if create_catalog_and_dbs == "True":
    catalog_creator.catalog_execute()
else:
    import time
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create DBs

# COMMAND ----------

catalog_creator.db_execute(owner)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Tables

# COMMAND ----------

catalog_creator = Creator(
    spark=spark,
    layer_name=layer_name,
    storage_account=storage_account,
    owner=owner,
)
catalog_creator.table_execute(owner=owner)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
