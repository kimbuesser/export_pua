# Databricks notebook source
from pyspark.sql import functions as F, Window

# COMMAND ----------

merchant = 'WIX'
filter_group = 'SOFTWARE'
user_var = 'customer'
date_var = 'ir_date'
additional_groups = ['institution_code']

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

amount_filter = 'amount>0'
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
  spark.table('tagged.smt_deduped_full_sf_combined_ir')# spark.table('tagged.smt_deduped_full_sf_dse_geo_v2')
    .filter(F.col('filter_group')==filter_group)
    .filter(F.upper('merchant')==merchant)
    .filter(FI_FILTER)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end)))
    .filter(amount_filter)
)
if txn_table.limit(1).count() ==0:
  (
    spark.table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
    .filter(F.expr(current_filter))
    .filter(FI_FILTER)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end)))
    .filter(amount_filter)
    .write.saveAsTable(f'power_user_tag_{merchant}', mode='overwrite')
  )
  txn_table= spark.table(f'power_user_tag_{merchant}')

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

# DBTITLE 1,Run Power Users on Raw
# # Grab customers from raw data
if run_raw:
  accs_refined = spark.table('accounts.fidwh_accounts_sf_refined_v2')
  max_ndd = accs_refined.agg(F.max('neptune_delivered_date')).head()[0]

  customer_acc_key = (
    accs_refined
      .filter(F.col('neptune_delivered_date')==max_ndd)
      .filter(F.col('status')=='A')
      .filter(F.col('aggr_status_code')==0) # Optional to remove potential dropoffs in recent period
      .filter(FI_FILTER)
      .select('customer', 'account', 'institution_code', 'tp_account_type', 'tp_institution_id')
  )

  duped_cols = list(set(customer_acc_key.columns).intersection(power_users_dropoff.columns) - {user_var, })
  power_user_with_account = (
    power_users_dropoff
    .withColumn(user_var, F.regexp_replace(user_var,'_deduped_id', ''))
    .join(customer_acc_key.drop(*duped_cols), on=user_var, how='left')
  )

  (
    spark.table('transactions.fidwh_txn_sf_raw')
    .filter(F.col('neptune_delivered_date')>=min(good_start, bad_start)) # For speed
    .filter(F.col('status')=='A')
    .filter(amount_filter)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end))) # Filter to include both good / bad periods
    .withColumn('tagged', F.expr(current_filter.replace('description', 'description_with_memo')))
    .join(F.broadcast(power_user_with_account), on='account', how='inner')
    .write.saveAsTable(raw_panel_tablename, format='delta', mode='overwrite', overwriteSchema='true')
  )

  spark.sql(f"optimize {raw_panel_tablename}")

# COMMAND ----------

# DBTITLE 1,Run Power Users on Deduped
if run_deduped_full:
  (
    spark.table('transactions.fidwh_txns_w_accounts_sf_toggled_deduped_full')
    .filter(amount_filter)
    .filter(FI_FILTER)
    .filter(F.col(date_var).between(min(good_start, bad_start), max(good_end, bad_end)))
    .join(F.broadcast(power_users_dropoff), on='customer', how='inner')
    .select('customer', 'account_plus_txn_id', 'fidwh_txns_w_accounts_sf_toggled_deduped_full.institution_code', 'tp_account_type', 'ir_date', 'description', 'amount', 'neptune_delivered_date' ,'txns_good', 'txns_bad', 'weeks_good', 'weeks_bad', 'dpt_good', 'dpt_bad')
    .withColumn('tagged', F.expr(current_filter))
    .write.saveAsTable(deduped_panel_tablename, format='delta', mode='overwrite', overwriteSchema='true')
  )

spark.table(deduped_panel_tablename).filter(F.col('weeks_bad')==0).sort('customer', 'ir_date', 'description').limit(100000).display()

# COMMAND ----------

dbutils.notebook.exit('FINISHED')

# COMMAND ----------

# # Dedupe raw data - Look at panel transactions
(
  spark.table(raw_panel_tablename)
  .withColumn('max_v', F.row_number().over(Window.partitionBy('account_plus_txn_id').orderBy('neptune_delivered_date')))
  .filter(F.col('max_v')==1)
  .sort('customer', 'ir_date', 'description_with_memo')
  .select('customer','account_plus_txn_id', 'ir_date', 'description_with_memo', 'amount', 'txns_good', 'txns_bad', 'tagged')
  .filter('customer=="8f6a3f1104a6bd30245c8a547333bc35"')# cus seems to have trx only as M1 THIS IS ITTTT!! NOT BEEN TAGGED
  #.filter(F.col('description_with_memo').rlike('8327952000')) ##this is to proof m1 it is an m1 finance trx
  #.filter('customer=="012230c2c9fe5b82f8898af4f17d163c"') #cus dissapear after 08/12
  #.filter('customer=="157476a96ae0b2e23269f63ebeec7975"')#cus seems to start buying at coin base 
  #.filter(F.col('description_with_memo').rlike('888\\-8248817')) # Check for similar clauses
  .sort('customer', 'ir_date')
).display()

# COMMAND ----------

spark.table(raw_panel_tablename).display()

# COMMAND ----------

(
  spark.table(raw_panel_tablename)
  .withColumn('max_v', F.row_number().over(Window.partitionBy('account_plus_txn_id').orderBy('neptune_delivered_date')))
  .filter(F.col('max_v')==1)
  .filter('amount>0')
  .filter('tagged')
  .withColumn('discretionary_removal', F.col('description_with_memo').rlike('NSF.*?REMIT'))
  .withColumn('week', F.date_trunc('week','ir_date').cast('date'))
  .filter(F.col('week')>='2023-04-01')
  .groupBy('week', 'discretionary_removal')
  .agg(F.count('*'), F.sum('amount'))
  .sort('week')
).display()


# COMMAND ----------

accs_refined = spark.table('accounts.fidwh_accounts_sf_refined_v2')
max_ndd = accs_refined.agg(F.max('neptune_delivered_date')).head()[0]

accounts = (
  accs_refined
    .filter(F.col('neptune_delivered_date')==max_ndd)
    .filter(F.col('status')=='A')
    .filter(F.col('aggr_status_code')==0) # Optional to remove potential dropoffs in recent period
    .filter(F.col('institution_code').isin(1,2,13))
    .select('account')
)

(
  spark.table('transactions.fidwh_txn_sf_raw_w_metadata')
  .filter(F.col('neptune_delivered_date')>='2023-04-01')
  .filter(F.col('institution_code').isin(1,2,13))
  .filter(F.col('ir_date')>='2023-04-01')
  .filter('(description_with_memo RLIKE "REMITLY") and (description_with_memo not RLIKE "RETRY")')
  .join(accounts, on='account', how='inner') # Get rid of dead accounts
  .withColumn('max_v', F.row_number().over(Window.partitionBy('account_plus_txn_id').orderBy('neptune_delivered_date')))
  .filter(F.col('max_v')==1)
  .filter(F.col('status')=='A')
  .filter(F.col('amount').between(0, 50000))
  .withColumn('discretionary_removal', F.col('description_with_memo').rlike('NSF.*?REMIT'))
  .groupBy('institution_code','ir_date', 'discretionary_removal')
  .agg(
    F.count('*').alias('txns'),
    F.sum('amount').alias('DOLLARS'),
  )
  .write.saveAsTable('remitly_trends', format='delta')

)
