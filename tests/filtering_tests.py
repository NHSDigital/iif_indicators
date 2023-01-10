# Databricks notebook source
import pandas as pd
import unittest
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %run ./init_tests

# COMMAND ----------

# MAGIC %run ./lib/chispa

# COMMAND ----------

# MAGIC %run ./lib/nutter/runtime

# COMMAND ----------

# MAGIC %run ../functions/02_create_numerator

# COMMAND ----------

# MAGIC %run ../functions/utils

# COMMAND ----------

# MAGIC %run ../constants/parameters

# COMMAND ----------

# MAGIC %run ../constants/field_definitions

# COMMAND ----------

test_utils_exec_results, run_test_fixture, assert_all = init_tests()

# COMMAND ----------

@run_test_fixture
class HesInitialFilterTest(NutterFixture):
      
  
  def assertion_check_invalid_dob_filtered_out(self):
  
      df = spark.createDataFrame(
            pd.DataFrame({'MYDOB': ['012022', '011999', '021987', None, '011900', '011901', '011800']})
      )

      expected_df = spark.createDataFrame(
            pd.DataFrame({'MYDOB': ['012022', '011999', '021987']})
      )

      actual_df = exclude_records_in_criteria(df, 'MYDOB', invalid_DOBs)

      assert_df_equality(actual_df, expected_df)
      
      
  def assertion_check_age_outside_range_0_120_filtered_out(self):
    
      df = spark.createDataFrame(
            pd.DataFrame({'STARTAGE': [1, 4, 1666, 12, 15, 12, 7, 10, -1, 0.1, None]})
      )
      
      expected_df = spark.createDataFrame(
            pd.DataFrame({'STARTAGE': [1, 4, 12, 15, 12, 7, 10, 0.1]})
      )
      
      actual_df = include_records_between_criteria_range(df, "STARTAGE", valid_age)
      
      assert_df_equality(actual_df, expected_df)
      
      
  def assertion_check_filter_integer_type_gender_is_male_or_female(self):
    
      df = spark.createDataFrame(
              pd.DataFrame({'SEX': [1, 2, 1, 1, 1, 0, 9, 1]})
        ) 

      expected_df = spark.createDataFrame(
              pd.DataFrame({'SEX': [1, 2, 1, 1, 1,1]})
        )

      actual_df = include_records_in_criteria(df, "SEX", valid_sex)

      assert_df_equality(actual_df, expected_df)

  

  def assertion_check_transform_95_and_over_patients_to_95_plus(self):

      df = (spark.createDataFrame(
            pd.DataFrame({
              'ID': ['1', '2', '3', '4'],
              'STARTAGE': [1, 95, 100, -1]
            })
           )
           )
      
      
      actual_df = transform_95_and_over(df, 'STARTAGE')

      expected_df = (spark.createDataFrame(
            pd.DataFrame({
              'ID': ['1', '2', '3', '4'],
              'STARTAGE': ['1', '95+', '95+', '-1']
            })
      ))

      assert_basic_rows_equality(actual_df, expected_df)

      
  def assertion_check_male_female_name_transformation(self):

      df = spark.createDataFrame(
              pd.DataFrame({
                'ID': ['1', '2', '3'],
                'SEX': ['1', '2', None ]})
        ) 

      expected_df = spark.createDataFrame(
              pd.DataFrame({
                'ID': ['1', '2', '3'],
                'SEX': ['MALE', 'FEMALE', 'UNKNOWN']})
        )

      actual_df = remap_sex_column_with_name(df, "SEX")

      assert_basic_rows_equality(actual_df, expected_df)



# COMMAND ----------

@run_test_fixture
class CodeFilterTests(NutterFixture):
  
    "Test suite for testing the cohort filtering functionand logic for HES data"
  
    def assertion_no_exclusion_code_functioning(self):

        df = (spark.createDataFrame(
            pd.DataFrame({
              'EPIKEY' : ['a', 'b', 'c', 'd','e', 'f'],
              'DIAG_3_01': ['J45', 'X99', 'E14', 'G40','N13', None],
              'DIAG_4_01': ['J456', 'X999', 'E148', 'G401','N134', None],
              'DIAG_3_CONCAT': [['J45'], ['X99' , 'J45'], ['E14'], ['G40', 'G60'],['N13'], [None]],
              'DIAG_4_CONCAT': [['J456'], ['X999', 'J456'], ['E999', 'E678', 'E148'], ['G40', 'G60'],['N134'], [None]]})))

        actual_df = filter_data_for_condition(df, acsc_logic, 'no_exclusions_group')

        expected_df = (spark.createDataFrame(
          pd.DataFrame(
            {
              'EPIKEY' : ['a', 'c', 'd'],
              'DIAG_3_01': ['J45', 'E14', 'G40'],
              'DIAG_4_01': ['J456', 'E148', 'G401'],
              'DIAG_3_CONCAT': [['J45'], ['E14'], ['G40', 'G60']],
              'DIAG_4_CONCAT': [['J456'], ['E999', 'E678', 'E148'], ['G40', 'G60']]
            }
          )
        )
       )
        assert_df_equality(actual_df, expected_df)
      
      
    def assertion_influenza_patient_filter_functioning(self): 
        
        df = (spark.createDataFrame(
                  pd.DataFrame({
                    'EPIKEY' : ['a', 'b', 'c', 'd','e','f', 'g'],
                    'DIAG_3_01': ['J10', 'J10', 'D57', 'J18','J15','J15', None],
                    'DIAG_4_01': ['J100', 'J100', 'D570', 'J188','J157','J157', None],
                    'DIAG_3_CONCAT': [['J10'], ['J10', 'D57'], ['D57', 'J10'], ['J18'],['J15', 'L19', 'K23'], ['J15', 'L19', 'K23', 'D57'], [None]],
                    'DIAG_4_CONCAT': [['J100'], ['J100', 'D570'], ['D5070', 'J100'], ['J188'],['J152', 'L192', 'K232'],['J152', 'L192', 'K232', 'D572'], [None]]})))
        
        actual_df = filter_data_for_condition(df, acsc_logic, 'influenza_pneumonia')
        
        expected_df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : ['a', 'd','e'],
                'DIAG_3_01': ['J10', 'J18','J15'],
                'DIAG_4_01': ['J100', 'J188','J157'],
                'DIAG_3_CONCAT': [['J10'], ['J18'],['J15', 'L19', 'K23']],
                'DIAG_4_CONCAT': [['J100'], ['J188'],['J152', 'L192', 'K232']]})))
        
        assert_df_equality(actual_df, expected_df)
    
    def assertion_copd_patient_filter_functioning(self):
        
        df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : ['a', 'b', 'c', 'd','e', 'f', 'g'],
                'DIAG_3_01': ['J45', 'J20', 'J20', 'G40','J20', 'J47', 'None'],
                'DIAG_3_CONCAT': [['J45'], ['X99', 'J45'], ['J20', 'J11', 'J43'], ['J20', 'J11', 'J43'],['J20', 'K81', 'J47', 'Z76'], ['J47'], [None]]})))
        
        actual_df = filter_data_for_condition(df, acsc_logic, 'copd')
        
        expected_df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : [ 'c','e', 'f'],
                'DIAG_3_01': ['J20', 'J20', 'J47'],
                'DIAG_3_CONCAT': [['J20', 'J11', 'J43'],['J20', 'K81', 'J47', 'Z76'], ['J47']]})))
        
        assert_df_equality(actual_df, expected_df)
        
    
    def assertion_chf_patients_filter_functioning(self):
      
        df = (spark.createDataFrame(
                  pd.DataFrame({
                    'EPIKEY' : ['a', 'b', 'c', 'd', 'e'],
                    'DIAG_3_01': ['I11', 'I50', 'J81', 'J81', None],
                    'DIAG_4_01': ['I110', 'I506', 'J816', 'J816', None],
                    'OPERTN_3_01': ['F23', 'K01', 'K71', 'V19', None]})))
        
        actual_df = filter_data_for_condition(df, acsc_logic, 'congestive_heart_failure')
        
        expected_df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : ['a', 'd'],
                'DIAG_3_01': ['I11', 'J81'],
                'DIAG_4_01': ['I110' , 'J816'],
                'OPERTN_3_01': ['F23', 'V19']})))
        
        assert_df_equality(actual_df, expected_df)
        
    def assertion_hypertension_patients_function_functioning(self):
      
        df = (spark.createDataFrame(
                pd.DataFrame({
                  'EPIKEY' : ['a', 'b', 'c', 'd','e', 'f'],
                  'DIAG_3_01': ['I10', 'I50', 'J81', 'I11','I10', None],
                  'DIAG_4_01': ['I100', 'I506', 'J816', 'I119','I108', None],
                  'OPERTN_3_01': ['F23', 'K44', 'K50', 'V19','K69', None]})))

        actual_df = filter_data_for_condition(df, acsc_logic, 'hypertension')

        expected_df = (spark.createDataFrame(
                pd.DataFrame({
                  'EPIKEY' : ['a', 'd'],
                  'DIAG_3_01': ['I10', 'I11'],
                  'DIAG_4_01': ['I100' , 'I119'],
                  'OPERTN_3_01': ['F23', 'V19']})))

        assert_df_equality(actual_df, expected_df)
        
    
    def assertion_cellulitis_patients_function_functioning(self):
        
        df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : ['a', 'b', 'c', 'd','e', 'f', 'g', 'h', 'i', 'j'],
                'DIAG_3_01': ['L03', 'L03', 'L08', 'L08','L08', 'L03', 'B11', None, 'L08', 'L08'],
                'DIAG_4_01': ['L034', 'L034', 'L080', 'L085','L088', 'L088', 'L980', None, 'L081', 'L081'],
                'OPERTN_3_01': ['Z23', 'A12', 'U50', 'V19', 'X67', 'S12', 'U40', None, 'X01', 'S43']})))
        
        actual_df = filter_data_for_condition(df, acsc_logic, 'cellulitis')
        
        expected_df = (spark.createDataFrame(
              pd.DataFrame({
                'EPIKEY' : ['a', 'c','e', 'g'],
                'DIAG_3_01': ['L03', 'L08','L08', 'B11'],
                'DIAG_4_01': ['L034', 'L080','L088', 'L980'],
                'OPERTN_3_01': ['Z23', 'U50','X67', 'U40']}))) 
        
        assert_df_equality(actual_df, expected_df)
        
        
    
      


# COMMAND ----------

assert_all()

# COMMAND ----------

