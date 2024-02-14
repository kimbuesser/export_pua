# Databricks notebook source
from pyspark.sql import functions as F, Window

# COMMAND ----------

merchant = ''
# filter_group = ''
user_var = 'unique_mem_id'
date_var = 'ir_date'
additional_groups = ['cobrand_id']

# "Good" period before or after dropoff
good_start = '2023-01-01'
good_end = '2023-06-14'

# Period of dropoff
bad_start = '2023-06-15'
bad_end = '2023-09-30'

# Power user needs this many txns in "good" period
min_txn_threshold = 5 

current_filter = r"""
description RLIKE 'CRYPTO\\W*COM|FORISUS'
AND description NOT RLIKE "CRYPTO\\W*COMP|\\b2\\s?CRYPTO|GOOGLE|GIBRALTAR|(^|\\W)ZELLE"
"""

# amount_filter = 'amount>0'
FI_FILTER = F.expr('true') #F.col('institution_code').isin('2') #F.expr('true')

run_raw = True
run_deduped_full = True

# COMMAND ----------

power_user_tablename = f'power_users__deduped_{merchant}'
raw_panel_tablename = f'power_users_raw_panel_{merchant}'
deduped_panel_tablename =  f'power_users_raw_deduped_{merchant}'

# COMMAND ----------

# DBTITLE 1,Existing Tagged table
# Input txn table
txn_table = (
  spark.table('nile_v7.nile_combined_filters_csj_v7')# spark.table('tagged.smt_deduped_full_sf_dse_geo_v2')
    # .filter(F.col('filter_group')==filter_group)
    .filter(F.upper('primary_merchant_name')==merchant)
    .filter(FI_FILTER)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end)))
    # .filter(amount_filter)
)
if txn_table.limit(1).count() ==0:
  (
    spark.table('nile_v7.nile_combined_filters_csj_v7')
    .filter(F.expr(current_filter))
    .filter(FI_FILTER)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end)))
    # .filter(amount_filter)
    .write.saveAsTable(f'power_user_tag_{merchant}', mode='overwrite')
  )
  txn_table= spark.table(f'power_user_tag_{merchant}')

# COMMAND ----------

table('nile_v7.nile_combined_filters_csj_v7').display()

# COMMAND ----------

# DBTITLE 1,Grab Power Users
(
  txn_table
    .withColumn(
      'period_type', 
      F.when(F.col(date_var).between(good_start, good_end), F.lit('GOOD_PERIOD'))
       .when(F.col(date_var).between(bad_start, bad_end), F.lit('BAD_PERIOD'))
    )
    .filter(F.col('period_type').isNotNull())
    .withColumn('ir_week', F.date_trunc('week', date_var).cast('date'))
    .groupBy(user_var, *additional_groups)
    .agg(
      F.count(F.when(F.col('period_type')=='GOOD_PERIOD', F.lit(1))).alias('txns_good'),
      F.count(F.when(F.col('period_type')=='BAD_PERIOD', F.lit(1))).alias('txns_bad'),
      F.countDistinct(F.when(F.col('period_type')=='GOOD_PERIOD', F.col('ir_week'))).alias('weeks_good'),
      F.countDistinct(F.when(F.col('period_type')=='BAD_PERIOD', F.col('ir_week'))).alias('weeks_bad'),
      F.mean(F.when(F.col('period_type')=='GOOD_PERIOD', F.col('amount'))).alias('dpt_good'),
      F.mean(F.when(F.col('period_type')=='BAD_PERIOD', F.col('amount'))).alias('dpt_bad'),
    )
    .write.saveAsTable(power_user_tablename, format='delta', mode='overwrite')
)

# COMMAND ----------

# DBTITLE 1,Subset to customers of interest
power_users_dropoff = (
  spark.table(power_user_tablename)
    .filter((F.col('txns_bad') + F.col('txns_good')) >= min_txn_threshold)
    .filter(F.col('txns_bad') / F.col('txns_good') < 0.1) # Little txns in bad period
    .filter(F.col('weeks_bad') / F.col('weeks_good') <= 0.33) # No more than a third of weeks "bad"
)

print(f'User count: {power_users_dropoff.count()}')

# COMMAND ----------

power_users_dropoff.display()

# COMMAND ----------

dbutils.notebook.exit('FINISHED')

# COMMAND ----------

txns_cust = (
  table('nile_v7.nile_combined_filters_csj_v7')
  .filter(F.col("unique_mem_id") ==  "1053209796162071968751646")
  .filter(F.col('ir_date') >= '2023-01-01')
  .sort(F.desc('ir_date'))
)
display(txns_cust)

# COMMAND ----------

txns_cust = (
  table('nile_v7.nile_combined_filters_csj_v7')
  .filter(F.col("unique_mem_id") ==  "")
  .filter(F.col('ir_date') >= '')
  .sort(F.desc('ir_date'))
)
display(txns_cust)
