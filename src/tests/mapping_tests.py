# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %run ./init_tests

# COMMAND ----------

# MAGIC %run ./lib/chispa

# COMMAND ----------

# MAGIC %run ./lib/nutter/runtime

# COMMAND ----------

# MAGIC %run ../functions/01_create_denominator

# COMMAND ----------

# MAGIC %run ../functions/utils

# COMMAND ----------

test_utils_exec_results, run_test_fixture, assert_all = init_tests()

# COMMAND ----------

@run_test_fixture
class TestMappingFunctions(NutterFixture):
  
  
  def assertion_string_type_column_to_datetime(self):
      df = pd.DataFrame(
      {
        'PRACTICE_CODE': ['A12345'],    
        'START_DATE': ['2022-01-01'],
        'END_DATE': ['2022-01-01']
      }).astype(str)

      expected_df = pd.DataFrame(
        {
        'PRACTICE_CODE': ['A12345'],    
        'START_DATE': ['2022-01-01'],
        'END_DATE':['2022-01-01']
        }
      ).astype({
        'PRACTICE_CODE': 'str',
        'START_DATE': 'datetime64[ns]',
        'END_DATE': 'datetime64[ns]'
      })

      actual_df = parse_date_columns_to_datetime(df, ['START_DATE', 'END_DATE'])

      pd.testing.assert_frame_equal(actual_df, expected_df)
  
  def assertion_int_type_column_to_datetime(self):
      df = pd.DataFrame(
      {
        'PRACTICE_CODE': ['A12345'],    
        'START_DATE': [20220101],
        'END_DATE': [20220101]
      }).astype(str)

      expected_df = pd.DataFrame(
        {
        'PRACTICE_CODE': ['A12345'],    
        'START_DATE': ['2022-01-01'],
        'END_DATE':['2022-01-01']
        }
      ).astype({
        'PRACTICE_CODE': 'str',
        'START_DATE': 'datetime64[ns]',
        'END_DATE': 'datetime64[ns]'
      })

      actual_df = parse_date_columns_to_datetime(df, ['START_DATE', 'END_DATE'])

      pd.testing.assert_frame_equal(actual_df, expected_df)


@run_test_fixture
class TestDataFrameDataChecks(NutterFixture):
  
  def assertion_inclusion_list_exclusion_practices_do_not_share_common_practices(self):
  
      inclusion_list = spark.sql("SELECT PRACTICE_CODE FROM iif_indicators_collab.included_practices")
      inclusion_list = [row.PRACTICE_CODE for row in inclusion_list.collect()]
    
      exclusion_list = spark.sql("SELECT PRACTICE_CODE FROM iif_indicators_collab.excluded_practices")
      exclusion_list = [row.PRACTICE_CODE for row in exclusion_list.collect()]
      
      assert set(inclusion_list).symmetric_difference(set(exclusion_list))


# COMMAND ----------

assert_all()

# COMMAND ----------

