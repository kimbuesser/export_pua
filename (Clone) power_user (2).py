# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.functions import regexp_replace
import json

# COMMAND ----------

# %run ../config/pipeline_config
# pipeline_config = json.loads(pipeline_config)
# db = (pipeline_config['merchant_batch']['db_filter'][env])
# display(spark.table(f'{db}.themeparks_API_park_mix_{env}'))

# COMMAND ----------

#  new_filtered_data = (
#   table('default.pluto_PETCO_COM_refined')
#   .filter(F.col('description').rlike('PETCO|PETCO( |\\.|CO)'))
#   .filter(~F.col('description').rlike('PARK'))
#    .filter(F.col('ir_date') >= '2023-01-01')
#    .filter(F.col('ir_date') <= '2023-10-01')
# )
# # allMetrics
# display(new_filtered_data)

# COMMAND ----------

# %sql
# SELECT *
# FROM tagged.smt_deduped_full_sf_dse_geo_v2


# COMMAND ----------

# top_cus = (
#   new_filtered_data
#   .groupBy(['customer', 'institution_code'])
#   .agg(F.sum("amount").alias("amount"))
#   .orderBy('amount', ascending=False)
# )
# display(top_cus)

# COMMAND ----------

#dbutils.notebook.exit("hello")

# COMMAND ----------

## institution #2
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
  .filter(F.col("customer") ==  "080aa41c152141f0a7f37fc20d6a6ab2")
  .filter(F.col('ir_date') >= '2023-11-01')
  .sort(F.desc('ir_date'))
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
 # .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

## institution #13
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
  .filter(F.col("customer") ==  "84b42ce41dfff6adfc1d5aab8a747f9c")
  .filter(F.col('ir_date') >= '2023-11-01')
  .sort(F.desc('ir_date'))
#  .orderBy('ir_date', descending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
 #  .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

##inst 13
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
  .filter(F.col("customer") ==  "324b572a5caa7a65efe840a26e4b0752")
  .filter(F.col('ir_date') >= '2023-10-01')
  .sort(F.desc('ir_date'))
#   .filter(F.col("ir_date") > "2021-03-01")
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
#   .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

## institution #2
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
  .filter(F.col("customer") ==  "decb6fd8f788767122326d56c71d5ba6")
  .filter(F.col('ir_date') >= '2023-11-01')
  # .filter(F.col('description').rlike('WIX'))
  .sort(F.desc('ir_date'))
#   .filter(F.col("ir_date") > "2021-03-01")
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
#   .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

## inst 1
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
  .filter(F.col("customer") ==  "7c90d2dd3305ba60f88b2844c11d7cf2")
  .filter(F.col('ir_date') >= '2023-11-01')
  .sort(F.desc('ir_date'))
#   .filter(F.col("ir_date") > "2021-03-01")
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
 #  .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

##int 2
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
    .filter(F.col("customer") ==  "67e8819b05a627de46c348136fe90c54")
  .filter(F.col('ir_date') >= '2023-11-01')
#  .filter(F.col("ir_date") > "2021-03-01")
  .sort(F.desc('ir_date'))
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
#   .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)

# COMMAND ----------

##int 143
txns_cust = (
  table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
    .filter(F.col("customer") ==  "5b9f10240474ccf47af1721eaf45cf6a")
  .filter(F.col('ir_date') >= '2023-11-01')
  .sort(F.desc('ir_date'))
#   .filter(F.col("ir_date") > "2021-03-01")
#   .orderBy('ir_date', ascending=True)
#   .filter(F.col("ir_date") < "2021-11-01")
#   .filter(F.col("description").rlike('88'))
#   .sort(F.desc("ir_date"))
#   .filter(F.col("description").rlike("V ?I ?P|P ?E ?T|V ?E ?T"))
)
display(txns_cust)
