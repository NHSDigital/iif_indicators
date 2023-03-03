# Databricks notebook source
from pyspark.sql import functions as F, DataFrame

# COMMAND ----------

def get_attribute_codes(database_name: str = 'iif_indicators_collab', table_name: str = 'attribute_mapping') -> DataFrame:
  
    """
    Read in attribute codes for indicators from database. 
    
    Args:
      database_name: Name of database containing attribute codes. 
      table_name: Name of table containing attribute codes. 
    
    Returns: 
      df: DataFrame containing attribute codes. 
    """
    
    columns = [
      F.col('Indicator')    .cast('string') .alias('DATA_REFERENCE_IDENTIFIER'),
      F.col('Attribute_ID') .cast('string') .alias('ATTRIBUTE_IDENTIFIER')
    ]
    
    df = (
      spark.table(f"{database_name}.{table_name}")
      .select(*columns)
    
    )
    
    return df

# COMMAND ----------

def get_amb_care_pats_df(db_name: str, table_name: str) -> DataFrame: 
    
    """
    Read in amb_care_pats_df, renaming some columns, and join with PCN mapping data. 
    
    Args:
      db_name: Name of database containing amb_care_pats_df. 
      table_name: Name of table containing amb_care_pats_df. 
    
    Returns: 
      df: DataFrame containing amb care pats joined to PCN mapping. 
    """
    
    df = spark.sql(
      f"""
      Select DISTINCT 
      amb.GPPRAC AS PRACTICE_CODE, 
      amb.EPIKEY, 
      amb.STARTAGE AS AGE, 
      amb.SEX, 
      amb.FAE_EMERGENCY, 
      amb.RP_month_start AS EXTRACT_DATE,
      amb.RPSD, 
      pcn.PCN_CODE,
      pcn.PCN_NAME
      from iif_indicators_collab.pcn_mapping pcn
      LEFT JOIN {db_name}.{table_name} amb
      ON (amb.GPPRAC = pcn.PRACTICE_CODE) 
      """
    )
    
 
    return df

# COMMAND ----------

def find_admissions_by_age_sex(pcn_mapping: DataFrame, age_sex_ref_df: DataFrame, admissions_data: DataFrame) -> DataFrame:

  """
  Returns admissions by age band and sex.

  Args:
    pcn_mapping: Dataframe containing PCN mapping data. 
    age_sex_ref_df: Dataframe containing list size age and sex grouping reference columns.
    admissions_data: Dataframe containing admissions data.

  Returns: 
    admissions_by_age_sex_df: DataFrame containing admissions by age and sex. 
  """

  age_band_pcn_joined_df = pcn_mapping.crossJoin(age_sex_ref_df).distinct()
  admissions_by_age_sex_df = (
    age_band_pcn_joined_df
    .join(admissions_data, ['PRACTICE_CODE', 'AGE', 'SEX', 'PCN_CODE'], how = 'left')
    .groupBy('PCN_CODE', 'AGE_FIVE_YEAR').sum('FAE_EMERGENCY')
    .withColumnRenamed('sum(FAE_EMERGENCY)','admissions')
    .fillna(0, 'admissions')
    .orderBy('PCN_CODE', 'AGE_FIVE_YEAR'))
  return admissions_by_age_sex_df

  
def find_admissions_by_pcn(pcn_map_df: DataFrame, admissions_data_df: DataFrame) -> DataFrame:
  
  """
  Returns admissions by PCN.

  Args:
    pcn_map_df: Dataframe containing PCN mapping data. 
    admissions_data_df: Dataframe containing admissions data.

  Returns: 
    df: DataFrame containing admissions by PCN. 
  """
  df = (
    pcn_map_df
    .join(admissions_data_df, on = ['PRACTICE_CODE','PCN_CODE'], how = 'left')
    .groupBy('PCN_CODE')
    .sum('FAE_EMERGENCY')
    .withColumnRenamed('sum(FAE_EMERGENCY)','admissions')
    .fillna(0, 'admissions')
    .orderBy('PCN_CODE')
  )
  return df 


def generate_standardised_numerator(db_name: str, table_name: str, acsc_indicator_code: str, date: str) -> DataFrame:
  
  """
  Runs sequence of functions neccessary to apply standardisation to the numerator (number of acsc patients).

  Args:
    db_name: Name of database HES data is stored in.
    table_name: Name of table HES data is stored in.
    acsc_indicator_code: Indicator specific code with which to fill the ATTRIBUTE_ID col.
    date: First day of report peiod month for list size extraction.

  Returns: 
    standardised_numerator: DataFrame containing standardised admissions. 
  """

  # Generate admissions by pcn and pcn/age/gender for calculation
  amb_care_pats = get_amb_care_pats_df(db_name, table_name)
  current_pcn_mapping = spark.table('iif_indicators_collab.pcn_mapping').select('PRACTICE_CODE', 'PCN_CODE', 'PCN_NAME')
  ref_age_columns = spark.sql("""Select SEX, AGE, AGE_FIVE_YEAR, 'EXTRACT_DATE' from iif_indicators_collab.ref_age_columns""")

  admissions_by_pcn = find_admissions_by_pcn(current_pcn_mapping, amb_care_pats)
  admissions_by_pcn_age_sex = find_admissions_by_age_sex(current_pcn_mapping, ref_age_columns, amb_care_pats)

  # Gathering standard population data for calculation
  england_list_size_by_age_sex = spark.sql('SELECT AGE_FIVE_YEAR, Count AS eng_population_by_age_sex FROM iif_indicators_collab.list_size_england')
  england_list_size = england_list_size_by_age_sex.groupBy().agg(F.sum('eng_population_by_age_sex')).collect()[0][0]
  pcn_list_size = spark.sql("""Select * from iif_indicators_collab.list_size""").withColumnRenamed('count','banded_pcn_pop').filter(F.col('EXTRACT_DATE') == f'{date}')

  calculate_dsr_df = (pcn_list_size
                       .join(england_list_size_by_age_sex, ['AGE_FIVE_YEAR'], how = 'left')
                       .join(admissions_by_pcn_age_sex, ['PCN_CODE', 'AGE_FIVE_YEAR'], how = 'left')
                       .withColumn('eng_population', F.lit(england_list_size))
                       .withColumn('stan_sum', (F.col('admissions') * F.col('eng_population_by_age_sex')) / F.col('banded_pcn_pop'))
                       .groupBy('PCN_CODE', 'eng_population').agg(F.sum('stan_sum')).withColumnRenamed('sum(stan_sum)','stan_sum')
                       .withColumn('DSR', F.col('stan_sum') * (1 / F.col('eng_population')))
                      )

  pcn_list_size_totals = pcn_list_size.groupBy('PCN_CODE').sum('banded_pcn_pop').withColumnRenamed('sum(banded_pcn_pop)','pcn_pop')

  standardised_numerator = (admissions_by_pcn
                        .join(calculate_dsr_df, on = ['PCN_CODE'], how = 'left')
                        .join(pcn_list_size_totals, on = ['PCN_CODE'], how = 'left')
                        .withColumn('standardised_admissions', F.round(F.col('pcn_pop')*F.col('DSR')))
                        .withColumn('percentage_change', 100 * (F.col('standardised_admissions')-F.col('admissions'))/F.col('admissions'))
                        .withColumn('ATTRIBUTE_ID', F.lit(acsc_indicator_code))
                        .orderBy('PCN_CODE')
                       )
  
  return standardised_numerator 
