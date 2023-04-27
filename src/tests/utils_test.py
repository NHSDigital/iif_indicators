# Databricks notebook source
# MAGIC %run ./init_tests

# COMMAND ----------

# MAGIC %run ./lib/nutter/runtime

# COMMAND ----------

# MAGIC %run ./lib/chispa

# COMMAND ----------

# MAGIC %run ../functions/utils

# COMMAND ----------

import pandas as pd
import datetime 

# COMMAND ----------

test_utils_exec_results, run_test_fixture, assert_all = init_tests()

# COMMAND ----------

@run_test_fixture
class TestReportDateFunctionality(NutterFixture):
  
  
  def assertion_financial_year_start_is_of_datetime_type(self): 
    
      assertion_case = reportDate('2022-01-31')
      
      assert isinstance(assertion_case.financial_year_start, datetime.date)
      
  
  def assertion_baseline_acheivement_date_is_one_year_prior_to_parsed_date(self):
    
      assertion_case = reportDate('2022-01-31')

      assert assertion_case.get_baseline_acheivement_date() == datetime.date(2021, 1, 31)
      
  
  def assertion_correctly_calculates_financial_year_that_started_in_previous_year(self):
    
      assertion_case = reportDate('2022-01-31')
      
      assert assertion_case.baseline_financial_year_start() == datetime.date(2020, 4, 1)
      

  def assertion_correctly_returns_first_day_of_month(self):
    
      assertion_case = reportDate('2022-01-31')
      
      assert assertion_case.reporting_month_01 == datetime.date(2022, 1, 1)
      
  
  def assertion_correctly_calculates_financial_year_that_started_in_same_year(self):
    
      assertion_case = reportDate('2022-03-31')
      
      assert assertion_case.financial_year_start == datetime.date(2021, 4, 1)    