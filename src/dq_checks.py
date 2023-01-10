# Databricks notebook source
import pyspark.sql.functions as F
import datetime
from dateutil.relativedelta import relativedelta

# COMMAND ----------

achievement_date = '2022-04-30'

# COMMAND ----------

# DBTITLE 1,Data Tables
list_size = spark.table('iif_indicators_collab.list_size')
list_size_england = spark.table('iif_indicators_collab.list_size_england')

pcn_mapping = spark.table('iif_indicators_collab.pcn_mapping')

cqrs_data = spark.table('iif_indicators_collab.indicators_for_cqrs').filter(F.col('ACHIEVEMENT_DATE') == achievement_date)


# COMMAND ----------

# DBTITLE 1,List Size 
def list_size_used_in_run(achievement_date):
  
    """Check dates in list size are equal to start of current acheivement month and baseline achievement month."""
  
    current_year_date = datetime.datetime.strptime(achievement_date, '%Y-%m-%d').date().replace(day = 1)
    baseline_year_date = current_year_date - relativedelta(years=1)
    
    expected_dates = set([current_year_date, baseline_year_date]) 
    
    list_size_dates = ( 
      spark.table('iif_indicators_collab.list_size')
      .select('EXTRACT_DATE')
    )
    
    actual_dates = set([row['EXTRACT_DATE'] for row in list_size_dates.collect()])
    
    assert expected_dates == actual_dates
    
list_size_used_in_run(achievement_date)

# COMMAND ----------

def check_four_indicators_for_each_pcn(df):
    
    "Check there are four indicators for each PCN"
    
    df = df.groupBy('ORG_ID').count()
    
    indicator_counts = ([row['count'] for row in df.collect()])
    
    assert all(x == 4 for x in indicator_counts), 'There is one or more PCNs that does not have 4 indicators'
    
check_four_indicators_for_each_pcn(cqrs_data)

# COMMAND ----------

def pcn_mapping_practices_are_unique(pcn_df):
  
    practice_count = pcn_df.select('PRACTICE_CODE').distinct().count()
    pcn_mappings = pcn_df.count()
    
    print(f"There are {practice_count} unique practice codes in the pcn mapping file, and {pcn_mappings} GP-PCN mappings")
    assert practice_count == pcn_mappings
    

pcn_mapping_practices_are_unique(pcn_mapping)  
    
  
  

# COMMAND ----------

def monthly_change_in_admissions(achv_date: str): 
  
    achv_month = datetime.datetime.strptime(achv_date, '%Y-%m-%d').month
  
    if achv_month == 4:
        
        print("New FY, no previous data")
        
    else: 
      
        print("Okay")
        
monthly_change_in_admissions(achievement_date)