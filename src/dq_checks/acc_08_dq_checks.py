# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# Import table with both indicators
acc08_df = spark.table('iif_indicators_collab.indicators_for_cqrs_acc08')
#display(acc08_df)

# Import table with numerator only
acc08_num_df = spark.table('iif_indicators_collab.indicators_for_cqrs_acc08').filter(F.col('ATTRIBUTE_IDENTIFIER')=='767390F9-FD7E-4B01-A8AE-82B41DE00A02')
#display(acc08_num_df)

# Import table with denominator only
acc08_den_df = spark.table('iif_indicators_collab.indicators_for_cqrs_acc08').filter(F.col('ATTRIBUTE_IDENTIFIER')=='FB4D2185-044E-44A4-9C11-02A0A4034A02')
#display(acc08_den_df)

# COMMAND ----------

# Check each practice has both denominator and numerator each month
acc08_ind_per_prac = (
  acc08_df
  .groupBy('ACHIEVEMENT_DATE', 'ORG_ID')
  .agg(F.count('ATTRIBUTE_IDENTIFIER').alias('INDICATOR_COUNT'))
)
#display(acc08_ind_per_prac)

# Assert each prac has 2 atrtribute IDs per month
try:
  assert acc08_ind_per_prac.filter(F.col('INDICATOR_COUNT') != 2).count() == 0
except:
  print('Practice(s) with more/less than 2 indicators in a month:')
  display(acc08_ind_per_prac.filter(F.col('INDICATOR_COUNT') != 2))

# COMMAND ----------

# Check that each practice has a higher denominator than numerator 
acc08_pivoted_perc_df = (
  acc08_df
  .groupBy('ORG_ID','ACHIEVEMENT_DATE')
  .pivot('ATTRIBUTE_IDENTIFIER')
  .agg(F.sum(F.col('ATTRIBUTE_VALUE').cast('int')))
  .withColumn('perc_of_appts', (F.col('767390F9-FD7E-4B01-A8AE-82B41DE00A02')/F.col('FB4D2185-044E-44A4-9C11-02A0A4034A02'))*100)
)
#display(acc08_pivoted_perc_df)
  
# Assert each numerator is no higher than the denominator
try:
  assert acc08_pivoted_perc_df.filter(F.col('767390F9-FD7E-4B01-A8AE-82B41DE00A02') > F.col('FB4D2185-044E-44A4-9C11-02A0A4034A02')).count() == 0
except:
  print('Practice(s) with a higher numerator than denominator:')
  display(acc08_pivoted_perc_df.filter(F.col('767390F9-FD7E-4B01-A8AE-82B41DE00A02') > F.col('FB4D2185-044E-44A4-9C11-02A0A4034A02')))

# COMMAND ----------

# Check the month on month total change in count of GP practices
date_window = Window.partitionBy().orderBy('ACHIEVEMENT_DATE')
acc08_prac_count_change_df = (
  acc08_df
  .groupBy('ACHIEVEMENT_DATE')
  .agg(F.countDistinct('ORG_ID').alias('TOTAL_PRAC_COUNT'))
  .withColumn('prev_month_value', F.lag('TOTAL_PRAC_COUNT').over(date_window))
  .withColumn(
    'perc_change', 
    F.when(F.isnull(F.col('TOTAL_PRAC_COUNT') - F.col('prev_month_value')), 0)
    .otherwise(((F.col('TOTAL_PRAC_COUNT') - F.col('prev_month_value'))/F.col('prev_month_value'))*100)
  )
  .orderBy('ACHIEVEMENT_DATE')
)
#display(acc08_prac_count_change_df)

w = Window.partitionBy()
perc_change = (
  acc08_prac_count_change_df
  .withColumn('MAX_DATE', F.max(F.col('ACHIEVEMENT_DATE')).over(w))
  .filter(F.col('ACHIEVEMENT_DATE') == F.col('MAX_DATE'))
  .collect()[0][3]
)

# Assert most recent month has not shown a large swing in included practices
try:
  assert ((perc_change > -1) & (perc_change < 1))
except:
  print(f'Swing of more than 1% in GP practices: {perc_change}%')

# COMMAND ----------

# Check the month on month total change in ATTRIBUTE VALUE
date_window = Window.partitionBy().orderBy('ACHIEVEMENT_DATE')
acc08_month_total_change_df = (
  acc08_num_df
  .groupBy('ACHIEVEMENT_DATE')
  .agg(F.sum('ATTRIBUTE_VALUE').alias('TOTAL_ATTRIBUTE_VALUE'))
  .withColumn('prev_month_value', F.lag('TOTAL_ATTRIBUTE_VALUE').over(date_window))
  .withColumn(
    'perc_change', 
    F.when(F.isnull(F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value')), 0)
    .otherwise(((F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value'))/F.col('prev_month_value'))*100)
  )
  .orderBy('ACHIEVEMENT_DATE')
)
#display(acc08_month_total_change_df)

# Assert that each month total increments (except April)
try:
  assert acc08_month_total_change_df.filter((F.month(F.col('ACHIEVEMENT_DATE')) != 4) & (F.col('perc_change') <= 0)).count() == 0
except:
  print('Month(s) with total(s) showing no or negaive change:')
  display(acc08_month_total_change_df.filter(F.col('perc_change') <= 0))

# COMMAND ----------

# Check the month on month total change in ATTRIBUTE VALUE per practice
date_prac_window = Window.partitionBy('ORG_ID').orderBy('ACHIEVEMENT_DATE')
acc08_prac_total_change_df = (
  acc08_num_df
  .groupBy('ORG_ID', 'ACHIEVEMENT_DATE')
  .agg(F.sum('ATTRIBUTE_VALUE').alias('TOTAL_ATTRIBUTE_VALUE'))
  .withColumn('prev_month_value', F.lag('TOTAL_ATTRIBUTE_VALUE').over(date_prac_window))
  .withColumn(
    'perc_change', 
    F.when(F.isnull(F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value')) | ((F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value')) == 0), 0)
    .otherwise(((F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value'))/F.col('prev_month_value'))*100)
  )
  .fillna(0)
)
#display(acc08_prac_total_change_df)

# Assert that no practice total decreases
try:
  assert acc08_prac_total_change_df.filter(F.col('perc_change') < 0).count() == 0
except:
  print('Practice(s) with total(s) showing negaive change:')
  display(acc08_prac_total_change_df.filter(F.col('perc_change') < 0))