# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

cur_year_num_id = 'D3C64D53-20CF-43E7-B15B-1AA8613F7B74'
cur_year_den_id = 'D388FDB3-E3D7-44F2-80C7-0832B448C9B4'
bsln_year_num_id = '0871C126-B4C3-4054-A0C0-2481525A940F'
bsln_year_den_id = '88337490-8A59-4C40-8C13-FF0D9D01F48D'
cur_year_non_stndrd_id = 'd4909566-9bc8-11ed-a8fc-0242ac120002'
bsln_year_non_stndrd_id = 'd4909836-9bc8-11ed-a8fc-0242ac120002'

# COMMAND ----------

# Import table with both years
ac02_cqrs_df = spark.table('iif_indicators_collab.indicators_for_cqrs_ac02')
ac02_udal_df = spark.table('iif_indicators_collab.indicators_for_udal_ac02')

# Import table with current year only
ac02_cur_year_df = (
  spark.table('iif_indicators_collab.indicators_for_cqrs_ac02')
  .filter((F.col('ATTRIBUTE_IDENTIFIER')==cur_year_num_id)|(F.col('ATTRIBUTE_IDENTIFIER')==cur_year_den_id))
)


# Import table with baseline year only
ac02_bsln_year_df = (
  spark.table('iif_indicators_collab.indicators_for_cqrs_ac02')
  .filter((F.col('ATTRIBUTE_IDENTIFIER')==bsln_year_num_id)|(F.col('ATTRIBUTE_IDENTIFIER')==bsln_year_den_id))
)

# Import table with current year non standardised data 
ac02_cur_year_non_stndrd_df = (
  spark.table('iif_indicators_collab.indicators_for_udal_ac02')
  .filter((F.col('ATTRIBUTE_IDENTIFIER')==cur_year_non_stndrd_id)|(F.col('ATTRIBUTE_IDENTIFIER')==cur_year_den_id))
)

# Import table with baseline year non standardised data 
ac02_bsln_year_non_stndrd_df = (
  spark.table('iif_indicators_collab.indicators_for_udal_ac02')
  .filter((F.col('ATTRIBUTE_IDENTIFIER')==bsln_year_non_stndrd_id)|(F.col('ATTRIBUTE_IDENTIFIER')==bsln_year_den_id))
)

# Import table with numerators only
ac02_nums_df = (
  spark.table('iif_indicators_collab.indicators_for_cqrs_ac02')
  .filter((F.col('ATTRIBUTE_IDENTIFIER')==cur_year_num_id)|(F.col('ATTRIBUTE_IDENTIFIER')==bsln_year_num_id))
)

pcn_mapping = spark.table('iif_indicators_collab.pcn_mapping')

# COMMAND ----------

def pcn_mapping_practices_are_unique(pcn_df):
  
    practice_count = pcn_df.select('PRACTICE_CODE').distinct().count()
    pcn_mappings = pcn_df.count()
    
    try:
      assert practice_count == pcn_mappings
    except:
      print(f"There are {practice_count} unique practice codes in the pcn mapping file, and {pcn_mappings} GP-PCN mappings")
    

pcn_mapping_practices_are_unique(pcn_mapping)  

# COMMAND ----------

def check_indicators_per_month(df, expected_count):  
  # Check each practice has correct number of indicators per month
  ind_per_org = (
    df
    .groupBy('ACHIEVEMENT_DATE', 'ORG_ID')
    .agg(F.count('ATTRIBUTE_IDENTIFIER').alias('INDICATOR_COUNT'))
  )

  # Assert each org has however many atrtribute IDs per month
  try:
    assert ind_per_org.filter(F.col('INDICATOR_COUNT') != expected_count).count() == 0
  except:
    print(f'Org(s) with more/less than {expected_count} indicators in a month:')
    display(ind_per_org.filter(F.col('INDICATOR_COUNT') != expected_count))
    
check_indicators_per_month(ac02_cqrs_df, 4)
check_indicators_per_month(ac02_udal_df, 6)

# COMMAND ----------

def check_numerator_higher_than_denominator(df, numerator_id: str, denominator_id: str): 
  # Check that each practice has a higher denominator than numerator 
  pivoted_perc_df = (
    df
    .filter((F.col('ATTRIBUTE_IDENTIFIER')==numerator_id)|(F.col('ATTRIBUTE_IDENTIFIER')==denominator_id))
    .groupBy('ORG_ID','ACHIEVEMENT_DATE')
    .pivot('ATTRIBUTE_IDENTIFIER')
    .agg(F.sum(F.col('ATTRIBUTE_VALUE').cast('int')))
    .withColumn('perc_of_denominator', (F.col(numerator_id)/F.col(denominator_id))*100)
  )
  #display(pivoted_perc_df)

  # Assert each numerator is no higher than the denominator
  try:
    assert pivoted_perc_df.filter(F.col(numerator_id) > F.col(denominator_id)).count() == 0
  except:
    print('Org(s) with a higher numerator than denominator:')
    display(pivoted_perc_df.filter(F.col(numerator_id) > F.col(denominator_id)))
    
check_numerator_higher_than_denominator(ac02_cqrs_df, cur_year_num_id, cur_year_den_id)
check_numerator_higher_than_denominator(ac02_cqrs_df, bsln_year_num_id, bsln_year_den_id)
check_numerator_higher_than_denominator(ac02_udal_df, cur_year_non_stndrd_id, cur_year_den_id)
check_numerator_higher_than_denominator(ac02_udal_df, bsln_year_non_stndrd_id, bsln_year_den_id)

# COMMAND ----------

def check_monthly_org_count(df):
  # Check the month on month total change in count of GP practices
  date_window = Window.partitionBy().orderBy('ACHIEVEMENT_DATE')
  org_count_change_df = (
    df
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
  #display(org_count_change_df)

  w = Window.partitionBy()
  perc_change = (
    org_count_change_df
    .withColumn('MAX_DATE', F.max(F.col('ACHIEVEMENT_DATE')).over(w))
    .filter(F.col('ACHIEVEMENT_DATE') == F.col('MAX_DATE'))
    .collect()[0][3]
  )

  # Assert most recent month has not shown a large swing in included practices
  try:
    assert ((perc_change > -1) & (perc_change < 1))
  except:
    print(f'Swing of more than 1% in org count: {perc_change}%')
    
check_monthly_org_count(ac02_cqrs_df)
check_monthly_org_count(ac02_udal_df)

# COMMAND ----------

def check_monthly_total(df, numerator_id):
  # Check the month on month total change in ATTRIBUTE VALUE
  date_window = Window.partitionBy().orderBy('ACHIEVEMENT_DATE')
  month_total_change_df = (
    df
    .filter(F.col('ATTRIBUTE_IDENTIFIER')==numerator_id)
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
  #display(month_total_change_df)

  # Assert that each month total increments (except April)
  try:
    assert month_total_change_df.filter((F.month(F.col('ACHIEVEMENT_DATE')) != 4) & (F.col('perc_change') <= 0)).count() == 0
  except:
    print('Month(s) with total(s) showing no or negaive change:')
    display(month_total_change_df.filter(F.col('perc_change') <= 0))

check_monthly_total(ac02_cqrs_df, cur_year_num_id)
check_monthly_total(ac02_cqrs_df, bsln_year_num_id)
check_monthly_total(ac02_udal_df, cur_year_num_id)
check_monthly_total(ac02_udal_df, bsln_year_num_id)
check_monthly_total(ac02_udal_df, cur_year_non_stndrd_id)
check_monthly_total(ac02_udal_df, bsln_year_non_stndrd_id)

# COMMAND ----------

def check_monthly_org_totals(df, numerator_id, denominator_id):  
  # Check the month on month total change in ATTRIBUTE VALUE per org
  date_org_window = Window.partitionBy('ORG_ID').orderBy('ACHIEVEMENT_DATE')
  org_total_change_df = (
    df
    .filter(F.col('ATTRIBUTE_IDENTIFIER')==numerator_id)
    .groupBy('ORG_ID', 'ACHIEVEMENT_DATE')
    .agg(F.sum('ATTRIBUTE_VALUE').alias('TOTAL_ATTRIBUTE_VALUE'))
    .withColumn('prev_month_value', F.lag('TOTAL_ATTRIBUTE_VALUE').over(date_org_window))
    .withColumn(
      'perc_change', 
      F.when(F.isnull(F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value')) | ((F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value')) == 0), 0)
      .otherwise(((F.col('TOTAL_ATTRIBUTE_VALUE') - F.col('prev_month_value'))/F.col('prev_month_value'))*100)
    )
    .fillna(0)
  )
  
  org_pop_change_df = (
    df
    .filter(F.col('ATTRIBUTE_IDENTIFIER')==denominator_id)
    .groupBy('ORG_ID', 'ACHIEVEMENT_DATE')
    .agg(F.sum('ATTRIBUTE_VALUE').alias('TOTAL_POPULATION'))
    .withColumn('prev_month_value', F.lag('TOTAL_POPULATION').over(date_org_window))
    .withColumn(
      'pop_perc_change', 
      F.when(F.isnull(F.col('TOTAL_POPULATION') - F.col('prev_month_value')) | ((F.col('TOTAL_POPULATION') - F.col('prev_month_value')) == 0), 0)
      .otherwise(((F.col('TOTAL_POPULATION') - F.col('prev_month_value'))/F.col('prev_month_value'))*100)
    )
    .fillna(0)
  )
  
  org_total_w_pop_df  = org_total_change_df.join(org_pop_change_df, on= ['ORG_ID','ACHIEVEMENT_DATE'], how='inner')

  # Assert that no practice total decreases
  try:
    assert org_total_w_pop_df.filter((F.col('perc_change') < 0) & (F.col('pop_perc_change') >= 5)).count() == 0
  except:
    print('Org(s) with total(s) showing negaive monthly change without a population drop:')
    display(org_total_w_pop_df.filter((F.col('perc_change') < 0) & (F.col('pop_perc_change') >= 5)))
    
check_monthly_org_totals(ac02_cqrs_df, cur_year_num_id, cur_year_den_id)
check_monthly_org_totals(ac02_cqrs_df, bsln_year_num_id, bsln_year_den_id)
check_monthly_org_totals(ac02_udal_df, cur_year_num_id, cur_year_den_id)
check_monthly_org_totals(ac02_udal_df, bsln_year_num_id, bsln_year_den_id)
check_monthly_org_totals(ac02_udal_df, cur_year_non_stndrd_id, cur_year_den_id)
check_monthly_org_totals(ac02_udal_df, bsln_year_non_stndrd_id, bsln_year_den_id)

# COMMAND ----------

def check_cqrs_and_udal_contain_same_data(cqrs_df, udal_df):
  
  cqrs_df = cqrs_df.drop('CREATED_AT').orderBy(cqrs_df.columns)
  
  udal_df = (
    udal_df
    .filter((F.col('ATTRIBUTE_IDENTIFIER')!=cur_year_non_stndrd_id)&(F.col('ATTRIBUTE_IDENTIFIER')!=bsln_year_non_stndrd_id))
    .drop('CREATED_AT')
    .orderBy(udal_df.columns)
  )

  try:
    assert cqrs_df.collect() == udal_df.collect()
    
  except:
    print('CQRS and UDAL tables contain different data')
    
check_cqrs_and_udal_contain_same_data(ac02_cqrs_df, ac02_udal_df)