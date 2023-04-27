# Databricks notebook source
import pandas as pd 
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ./init_tests

# COMMAND ----------

# MAGIC %run ./lib/chispa

# COMMAND ----------

# MAGIC %run ./lib/nutter/runtime

# COMMAND ----------

# MAGIC %run ../functions/01_create_denominator

# COMMAND ----------

test_utils_exec_results, run_test_fixture, assert_all = init_tests()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iif_indicators_collab.test_dsr_df

# COMMAND ----------

def test(df):
  df = df.groupBy('PCN_CODE', 'eng_population').agg(F.sum('stan_sum')).withColumnRenamed('sum(stan_sum)','stan_sum').withColumn('DSR', F.col('stan_sum') * (1 / F.col('eng_population')))
  return df

# COMMAND ----------

@run_test_fixture
class TestDSRCalculation(NutterFixture):
  
  
  def assertion_dsr_calculation(self):
    
    df = spark.sql("""
                   SELECT * 
                   FROM iif_indicators_collab.test_dsr_df
                   """)
    
    # Calculated using  formula in excel
    expected_df = spark.createDataFrame(pd.DataFrame(
      {
      'PCN_CODE': ['U78157','U26379'],    
      'eng_population':['60744002','60744002'],
      'stan_sum':['1400783.897','1240751.946'],
      'DSR':['0.02306','0.02043'],
      }
     ).astype({
      'PCN_CODE': 'str',    
      'eng_population':'int',
      'stan_sum':'float',
      'DSR':'float',
     }))

    actual_df = test(df)
    actual_df = actual_df.withColumn('DSR',F.round(F.col('DSR'),5))
    actual_df = actual_df.withColumn('stan_sum',F.round(F.col('stan_sum'),3))

    actual = actual_df.collect()
    expected = expected_df.collect()

    assert actual == expected, f"dsr_test expected to find {expected} but found {actual}"

# COMMAND ----------

assert_all()