# Databricks notebook source
# DBTITLE 1,Define Helper Functions
# MAGIC %run "/GROUPS/DSE/Data Products/Production/Tagging QA/Nile/Function_Library_Final"

# COMMAND ----------

# DBTITLE 1,Configure Tagging Template
filter_id = 'CHIPOTLE_ONLINE'
tag_column = 'description'
tfd_start = datetime.date(2018, 1, 1)
filters_table_name = 'nile_v7.nile_combined_filters_csj_v7'
refresh_wide_cond = False
txn_includes = {
  'debit': True,
  'credit': True
}

# COMMAND ----------

# DBTITLE 1,Wide Capture Condition
wide_capture_cond = r"""
DESCRIPTION RLIKE 'CHIP ?O'

"""

# COMMAND ----------

# DBTITLE 1,Define Old Filter
old_condition = r"""
(
description rlike 'CHIPOT|CHIP ?OTLE|CHIPO$|(GRUBHUB|GRUBHUBFOOD|DOORDASH|SEAMLSS|POSTMATES)\\W*CHIPO'

AND description not rlike '\\bEL ?CHIP|CASA ?CHIP|SENOR|TAQUERIA|YOU BOUGHT|SUBWAY|TIPS? |TACO\\s|PIZZ|RETRY|GRINGO|PUBLISH|CHICHIPOTTS|MOCHI|CHIPOTEKA|ZECCHIPOTOMAC|AMAZON|CHIPOTE|OTCHIPOTCHI|URCHIPOTEAU|AMZN|MARIACHIPOT|BBQ|S2HOT|CHIPOLA|SURGICAL|SMOKE|LIQUOR|CHIPOLO|CHIPOBO|ZELLE ' 
)
AND amount <= 1500
AND DESCRIPTION RLIKE "PAYPAL|(?<!BANK|SQ)\\.CO|\\wCOM(\\W|$)|WWW|MOBIL(E|$)|\\WAPP(\\W|$)|ONLINE|\\WONLIN?E?(\\W|\\d|$)|\\WONL\\W|(E|M)COM(M|\\W)|WINGSTOP.*\\WMCO\\W|\\bESTORE|(( |^)[0-9]{4}( |$))?XXX-XXX-XXXX"
and DESCRIPTION NOT RLIKE "TS(T|P)|SQ\\s?\\*|\\WSTORE(\\W|$)|MOBILE(.*\\WAL(\\W|$|ABAM)|AL)"
and DESCRIPTION NOT RLIKE "CASH\\s?APP|IBOTTA|^HONEY|HONEY\\.CO|(WWW|^|\\W)RITUAL"
"""

# COMMAND ----------

# DBTITLE 1,Suggested Condition
suggested_condition = r"""
(DESCRIPTION RLIKE 'CHIPOTLE ?ON'
AND DESCRIPTION NOT RLIKE 'CHIPOTLE ?ONTARIO')
"""

# COMMAND ----------

display(table('nile_v7.nile_v7_mr_restaurants_secondary_tagged_csj_1')
        .filter(F.col('secondary_merchant').rlike('FIRST_PARTY_RESTAURANT_ONLINE'))
        .filter(F.col('merchant').isNotNull())
        .groupBy('merchant', 'ir_date')
        .agg(
            F.sum('amount').alias('corp_dollars'),
            F.count('*').alias('corp_transactions')
        ))

corporate = (table('nile_v7.nile_v7_mr_restaurants_secondary_tagged_csj_1')
        .filter(F.col('secondary_merchant').rlike('FIRST_PARTY_RESTAURANT_ONLINE'))
        .filter(F.col('merchant').isNotNull())
        .groupBy('merchant', 'ir_date')
        .agg(
            F.sum('amount').alias('corp_dollars'),
            F.count('*').alias('corp_transactions')
        ))

# COMMAND ----------

from pyspark.sql.functions import split
display(table('nile_v7.nile_v7_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('1st_party'))
        .groupBy('merchant', 'ir_date')
        .agg(
            F.sum('amount').alias('restaurant_dollars'),
            F.count('*').alias('restaurant_transactions')
        )
        .withColumn("restaurant_merchant", split(F.col("merchant"), "_").getItem(0)
)
        .drop('merchant'))

restaurant = (table('nile_v7.nile_v7_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('1st_party'))
        .groupBy('merchant', 'ir_date')
        .agg(
            F.sum('amount').alias('restaurant_dollars'),
            F.count('*').alias('restaurant_transactions')
        )
        .withColumn("restaurant_merchant", split(F.col("merchant"), "_").getItem(0)
)
        .drop('merchant')
        .withColumnRenamed('ir_date', 'ir_date_restaurant')
        )

# COMMAND ----------

join_corp_rest = restaurant.join(corporate, (restaurant.ir_date_restaurant == corporate.ir_date) & (restaurant.restaurant_merchant == corporate.merchant), 'outer')
display(join_corp_rest)

def save_and_overwrite_table(output, save_table):
    output.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(save_table) 
save_and_overwrite_table(join_corp_rest, 'first_party_corp_to_restaurants')

# COMMAND ----------

display(table('first_party_corp_to_restaurants'))

# COMMAND ----------

spark.sql("SHOW TABLE EXTENDED LIKE 'first_party_corp_to_restaurants'").show(10, False)

# COMMAND ----------

display(table('nile_v7.nile_v7_mr_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('dpz'))
        .groupBy(F.col('secondary_merchant'))
        .agg(F.sum('amount')))

corp = (table('nile_v7.nile_v7_mr_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('dpz'))
        .groupBy(F.col('secondary_merchant'))
        .agg(F.sum('amount')))

# COMMAND ----------

display(table('nile_v7.nile_v7_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('dpz'))
        .groupBy(F.col('secondary_merchant'))
        .agg(F.sum('amount')))

rest = (table('nile_v7.nile_v7_restaurants_secondary_tagged_csj_1')
        .filter(F.lower('merchant').rlike('dpz'))
        .groupBy(F.col('secondary_merchant'))
        .agg(F.sum('amount'))
        .filter(F.col('secondary_merchant').isNotNull()))

# COMMAND ----------

rest_corp = corp.join(rest, corp.secondary_merchant == rest.secondary_merchant, 'outer')
display(rest_corp)

# COMMAND ----------

display(corp
        .agg(F.sum('sum(amount)')))

display(rest
        .agg(F.sum('sum(amount)')))

# COMMAND ----------


1294898203.12 - 2620087294.17

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Define Sets of Data
filters_data = clean_data(filters_table_name, tfd_start, tag_column)

# list comp to capture above defined txn_includes (debit or credit)
txn_includes = F.col('type').isin(*[k for k, v in txn_includes.items() if v])
# Saves future object (wide capture table) named nilev7_{filter_id}_filters
with ThreadPoolExecutor() as e:
  filters_wide_capture = e.submit(
    save_wide_table,
    filters_data.filter(wide_capture_cond), f'nilev7_{filter_id}_filters', refresh_wide_cond
  )
  e.shutdown()

# Runs previous function and filters on debit or credit
  filters_wide_capture = filters_wide_capture.result().filter(
    wide_capture_cond
  ).filter(
    txn_includes
  ).cache()

possible_matches = filters_wide_capture.filter(f'NOT ({suggested_condition})').cache()
captured_data = filters_wide_capture.filter(old_condition).cache()
suggested_captured_data = filters_wide_capture.filter(suggested_condition).cache()
additions = filters_wide_capture.filter(f'NOT ({old_condition}) AND ({suggested_condition})').cache()
removals = captured_data.filter(f'NOT({suggested_condition})').cache()

# COMMAND ----------

# MAGIC %md # Suggested and Possible By Cobrand ID
# MAGIC

# COMMAND ----------

# DBTITLE 1,Suggested Captured Data by Cobrand ID
suggested_captures = string_analysis(suggested_captured_data, tag_column).cache()
display(suggested_captures)

# COMMAND ----------

# DBTITLE 1,Possible Captured Data by Cobrand ID
possible_captures = string_analysis(possible_matches, tag_column).cache()
display(possible_captures)

# COMMAND ----------

# MAGIC %md # Display Cleaned Condition Tables
# MAGIC

# COMMAND ----------

# DBTITLE 1,Suggested Grouped
suggested_grouped_desc_matches = dtu_metrics(suggested_captured_data, ['type', 'cobrand_id', tag_column], True).cache()
display(suggested_grouped_desc_matches) 

# COMMAND ----------

# DBTITLE 1,Possible Grouped
grouped_desc_possible_matches = dtu_metrics(possible_matches, ['type', 'cobrand_id', tag_column], True).cache()
display(grouped_desc_possible_matches)

# COMMAND ----------

# DBTITLE 1,Additions
add_captures = dtu_metrics(additions, ['type', 'cobrand_id', tag_column], True).cache()
display(add_captures) 

# COMMAND ----------

# DBTITLE 1,Removals
removal_diff = dtu_metrics(removals, ['type', 'cobrand_id', tag_column], True).cache()
display(removal_diff
) 

# COMMAND ----------

# MAGIC %md # Graph

# COMMAND ----------

# DBTITLE 0,Graph
import seaborn as sns
import matplotlib.pyplot as plt
period = 'week'

captured_data_gr = graph_setup(captured_data.filter(F.col('ir_date')>= '2023-01-01'), period, debits_only = False, credits_only = False)
suggested_captured_data_gr = graph_setup(suggested_captured_data.filter(F.col('ir_date')>= '2023-01-01'), period, debits_only = False, credits_only = False)

fig,ax = plt.subplots()
fig.set_size_inches(40,15)
sns.lineplot(x=period, y='transactions', data = suggested_captured_data_gr, color = 'blue', label = 'new')
sns.lineplot(x=period, y='transactions', data = captured_data_gr, color = 'orange', label = 'old')

# COMMAND ----------

# MAGIC %md # DTU analysis

# COMMAND ----------

# DBTITLE 1,Build DTU Tables
# Build DTU Metrics for old filter under filters table
old_dtu = stack_dtu_metrics(
  dtu_metrics(captured_data, ['type', 'cobrand_id', F.next_day('ir_date', 'sunday').alias('week_enddate')], prefix_col='Old'),
  ['type', 'cobrand_id', 'week_enddate'],
  'Old'
)
# Build DTU Metrics for new filter under filters table
new_dtu = stack_dtu_metrics(
  dtu_metrics(
    suggested_captured_data, ['type', 'cobrand_id', F.next_day('ir_date', 'sunday').alias('week_enddate')], prefix_col='New',
  ),
  ['type', 'cobrand_id', 'week_enddate'],
  'New'
)
# Build DTU comparison (old filter vs new filter) under filters table
dtu = old_dtu.join(
  new_dtu, ['type', 'cobrand_id', 'week_enddate', 'Metric_Name'], 'full'
).fillna(0).withColumn(
  'pct_delta', F.round((F.col('Old_Metric') / F.col('New_Metric')) - 1, 4)
).fillna(0).orderBy(
  'cobrand_id', 'week_enddate'
)

# Long to wide on Metric Name
df_wide = old_dtu.groupBy('week_enddate').pivot('Metric_Name').sum('Old_Metric')

renamed_df_wide = df_wide.select(F.col('week_enddate'), F.col('DPT'), F.col('DPU'), F.col('TPU'), F.col('Dollars').alias('Old_Dollars'), F.col('Transactions').alias('Old_Transactions'), F.col('Users').alias('Old_Users'))

new_df_wide = new_dtu.groupBy('week_enddate').pivot('Metric_Name').sum('New_Metric')

renamed_new_df_wide = new_df_wide.select(F.col('week_enddate'), F.col('DPT'), F.col('DPU'), F.col('TPU'), F.col('Dollars').alias('New_Dollars'), F.col('Transactions').alias('New_Transactions'), F.col('Users').alias('New_Users')).withColumnRenamed('week_enddate', 'new_week_enddate')

# Join new and old
wide_df = renamed_df_wide.join(renamed_new_df_wide, renamed_df_wide.week_enddate == renamed_new_df_wide.new_week_enddate, 'outer')

# COMMAND ----------

display(wide_df.filter((F.col('week_enddate')>='2018-01-01')))

# COMMAND ----------

dbutils.notebook.exit('DONE BUT DONT FORGET TO RUN THE EXCEL FILE BELOW')

# COMMAND ----------

# DBTITLE 1,Save Report to Excel
download_link = saveExcel(
  {
    'dtu_comparison': dtu,
    'suggested_pivot_desc': suggested_captures,
    'suggested_grouped_desc': suggested_grouped_desc_matches,
    'desc_additions': add_captures,
    'desc_removals': removal_diff,
    'pivot_potential_desc': possible_captures,
    'grouped_potential_desc': grouped_desc_possible_matches,
  },
  name=f'nilev7_{filter_id}'
)

displayHTML(download_link)

# COMMAND ----------

# DBTITLE 1,Exit Notebook
nb_info = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
dbutils.notebook.exit(
  json.dumps(
    {
      'file_link': download_link, 
      'template_link': f"https://mscience-e2.cloud.databricks.com/#job/{get_job_id(nb_info)}/run/1",
      'old_filter': old_condition,
      'suggested_filter': suggested_condition
    }
  )
)
