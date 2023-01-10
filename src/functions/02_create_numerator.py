# Databricks notebook source
from pyspark.sql import functions as F, DataFrame
from functools import reduce, partial
import operator
from typing import List

# COMMAND ----------

def query_hes_database(database_name: str, table_name: str) -> DataFrame: 
  
  """
  Return required patient data and columns from HES database.

  Args: 
    database_name: Name of Database containing HES data. 
    table_name: Name of table containing HES data. 

  Returns: 
    df: DataFrame containing required data from HES database. 
  """

  columns = [
    F.col('GPPRAC')          .cast('string')  .alias('GPPRAC'),
    F.col('EPIKEY')          .cast('bigint')  .alias('EPIKEY'), 
    F.col('ADMIDATE')        .cast('date')    .alias('ADMIDATE'),
    F.col('ADMIMETH')        .cast('string')  .alias('ADMIMETH'),
    F.col('EPISTAT')         .cast('string')  .alias('EPISTAT'),
    F.col('EPIORDER')        .cast('int')     .alias('EPIORDER'),
    F.col('EPITYPE')         .cast('string')  .alias('EPITYPE'),
    F.col('STARTAGE')        .cast('int')     .alias('STARTAGE'),
    F.col('MYDOB')           .cast('string')  .alias('MYDOB'),
    F.col('SEX')             .cast('string')  .alias('SEX'),
    F.col('MAINSPEF')        .cast('string')  .alias('MAINSPEF'),
    F.col('FAE_EMERGENCY')   .cast('int')     .alias('FAE_EMERGENCY'),
    F.col('DIAG_3_01')       .cast('string')  .alias('DIAG_3_01'),
    F.col('DIAG_3_CONCAT')   .cast('string')  .alias('DIAG_3_CONCAT'),
    F.col('DIAG_4_01')       .cast('string')  .alias('DIAG_4_01'),
    F.col('DIAG_4_CONCAT')   .cast('string')  .alias('DIAG_4_CONCAT'),
    F.col('OPERTN_3_01')     .cast('string')  .alias('OPERTN_3_01')
  ]

  df = (
    spark.table(f'{database_name}.{table_name}')
    .select(*columns)
  )

  return df 

  
def convert_diagnosis_columns_to_array(df: DataFrame) -> DataFrame:
    
  """
  Converts values of diagnosis columns into an array using convert_column_to_array().

  Args: 
    df: Dataframe with columns to transform.

  Returns: 
    df_transformed: DataFrame containing columns transformed by convert_column_to_array() function. 
  """

  df_transformed = (df
                     .transform(lambda df: convert_column_to_array(df, 'DIAG_3_CONCAT', 'DIAG_3_CONCAT'))
                     .transform(lambda df: convert_column_to_array(df, 'DIAG_4_CONCAT', 'DIAG_4_CONCAT'))
                   )

  return df_transformed



def filter_valid_acsc_patients(df: DataFrame, start_date: str, end_date: str) -> DataFrame: 
  
  """
  Applies inclusion logic of valid acsc patients.

  Args: 
    df: Dataframe of HES data.
    start_date: Start date of report period
    end_date: End date of report period
      
  Returns: 
    hes_transformed: DataFrame containing valid acsc patients. 
  """
  
  hes_transformed = (df
                     .transform(lambda df: include_records_within_report_period(df, start_date, end_date))
                     .transform(lambda df: include_records_between_criteria_range(df, 'STARTAGE', valid_age))
                     .transform(lambda df: include_records_in_criteria(df, 'SEX', valid_sex))
                     .transform(lambda df: include_records_in_criteria(df, 'EPISTAT', valid_epistat))
                     .transform(lambda df: include_records_in_criteria(df, 'EPITYPE', valid_epitype))
                     .transform(lambda df: include_records_in_criteria(df, 'EPIORDER', valid_epiorder))
                     .transform(lambda df: include_records_in_criteria(df, 'ADMIMETH', valid_admission_methods))
                     .transform(lambda df: exclude_records_in_criteria(df, 'MYDOB', invalid_DOBs))
                     .transform(lambda df: exclude_records_in_criteria(df, 'MAINSPEF', excluded_specialties))
                     .transform(lambda df: remap_sex_column_with_name(df, 'SEX'))
                     .transform(lambda df: transform_95_and_over(df, 'STARTAGE'))

                      )

  return hes_transformed


def include_records_within_report_period(df: DataFrame, start_date: str, end_date: str) -> DataFrame:
  
  """
  Function to filter out records that do not fall within the reporting period.

  Args: 
    df: Dataframe of HES data.
    start_date: Start date of report period.
    end_date: End date of report period.
      
  Returns: 
    df: DataFrame containing data within reporting period. 
  """
  
  record_within_report_period = F.col('ADMIDATE').between(start_date, end_date)

  return df.filter(record_within_report_period)
  


def exclude_records_in_criteria(df: DataFrame , column: str, criteria_list: List) -> DataFrame:

  """
  Removes records which match provided criteria list.
  
  Args: 
    df: Dataframe of HES data.
    column: Column to check.
    criteria_list: List by which to exclude.
      
  Returns: 
    df: DataFrame containing data post exclusion. 
            
  Ex:
    INVALID_DOB = ['190001', '190101']
    exclude_records_in_criteria(df, 'MYDOB', INVALID_DOB)
  """

  return  df.filter(~(F.col(column).isin(criteria_list)))
  

def exclude_records_starting_with_letter(df: DataFrame, column: str, letter: List) -> DataFrame:
  
  """
  Filters out records that start with letter.
  
  Args: 
    df: Dataframe of HES data.
    column: Column to check.
    letter: Letter by which to exclude.
      
  Returns: 
    df: DataFrame containing data post exclusion. 
  
  Ex: 
    exclude_records_starting_with_letter(df, 'DIAG_03_1', 'O')

  """

  return df.filter(~(F.col(column).startswith(*letter)))
    
    
def include_records_in_criteria(df: DataFrame, column: str, criteria_list: List) -> DataFrame:
  
  """
  Includes records which match provided criteria list.
  
  Args: 
    df: Dataframe of HES data.
    column: Column to check.
    criteria_list: List by which to include.
      
  Returns: 
    df: DataFrame containing data post exclusion. 
 
  Ex: 
    VALID_GENDER = [1, 2]
    include_records_in_criteria(df, "SEX", VALID_GENDER)
  """

  return df.filter(F.col(column).isin(criteria_list))
    

def include_records_between_criteria_range(df: DataFrame, column: str, criteria_list: List) -> DataFrame:
  
  """
  Includes records within range of provided criteria list.
  
  Args: 
    df: Dataframe of HES data.
    column: Column to check.
    criteria_list: List by which to include.
      
  Returns: 
    df: DataFrame containing data post exclusion. 
  
  Ex:
    VALID_AGE = [0, 120]
    include_records_between_criteria_range(df, "STARTAGE", VALID_AGE)

  """

  if len(criteria_list) != 2: 
      ValueError("Criteria list must contain only two items.")

  return df.filter(F.col(column).between(*criteria_list))
  
  
def remap_sex_column_with_name(df: DataFrame, column: str) -> DataFrame:
    
  """
  Replaces sex value with name. Shouldn't return UNKNOWN if valid_sex filter is working
  
  Args: 
    df: Dataframe of HES data.
    column: Name of column containing sex value.

  Returns: 
    df: DataFrame containing remapped SEX column. 
  """ 

  return (df.withColumn('SEX', F.when(F.col(column) == '1', 'MALE')
                       .when(F.col(column) == '2', 'FEMALE')
                       .when(F.col(column).isNull(), 'UNKNOWN').otherwise('UNKNOWN')))


def transform_95_and_over(df: DataFrame, column: str):
  
  """
  Replaces patients with ages over 95 to 95+
  
  Args: 
    df: Dataframe of HES data.
    column: Name of column containing age value.
      
  Returns: 
    df: DataFrame containing remapped age column. 
  """

  df = df.withColumn(column, F.when(F.col(column) >= 95, '95+').otherwise(F.col(column)))

  return df 

# COMMAND ----------

# Logic for condition encoded here

def filter_data_for_condition(df: DataFrame, dictionary: dict, condition: str) -> DataFrame:
  
  """
  Creates a cohort based on logic parsed in dictionary
  
  Args: 
    df: dataframe containing HES records.
    dictionary: dictionary of logic to be applied.
    condition: name of condition.
  
  Returns: 
    df: DataFrame containing data filtered for condition passed. 
  """
  
  if dictionary[condition]['additional_criteria']: 
      df = df.where(
        (dictionary[condition]['and_or'])(
          (dictionary[condition]['inclusion_criteria']),
          (dictionary[condition]['exclusion_criteria'])
        )
      )
  
  else:
       df = df.where(dictionary[condition]['inclusion_criteria'])
    
  return df

# COMMAND ----------

def create_numerator(database: str, table: str, start_date: str, end_date: str, report_period_month_start: str) -> DataFrame:
    
  """
  Runs sequence of functions neccessary to generate the numerator (number of acsc patients)
  
  Args: 
    database: Name of database HES data is stored in.
    table: Name of table HES data is stored in.
    start_date: Start date of report period.
    end_date: End date of report period.
    report_period_month_start: Date of first day of report period month.
  
  Returns: 
    acsc_patients: DataFrame containing list of valid acsc patients filtered for indicator specific conditions. 
  """
    
  hes_ahas = query_hes_database(database, table)
  hes_ahas = convert_diagnosis_columns_to_array(hes_ahas)
  hes_filtered = filter_valid_acsc_patients(hes_ahas, start_date, end_date)

  acsc_patients = spark.createDataFrame(data = [], schema = hes_filtered.schema)
  df_list = ['no_exclusions_group', 'cellulitis', 'influenza_pneumonia', 'hypertension','congestive_heart_failure', 'copd']

  for condition in df_list: 
      df = filter_data_for_condition(hes_filtered, acsc_logic, condition)
      acsc_patients = acsc_patients.union(df)

  acsc_patients = (acsc_patients.drop_duplicates()
                   .withColumn('RPSD', F.lit(start_date))
                   .withColumn('RPED', F.lit(end_date))
                   .withColumn('RP_month_start', F.lit(report_period_month_start))
                  )



  return acsc_patients