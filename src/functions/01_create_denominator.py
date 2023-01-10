# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

def get_latest_list_size_data(ach_date: str) -> DataFrame:
  
    """
    Extracts raw data from GP Patient List (List size) database for date specified
    
    Args: 
      ach_date: Date to filter list size practices on, usually the 1st day of month of Reporting Period End Date. 
      
    Returns: 
      df: DataFrame containing list size data.
    """ 
    
  
    df = spark.sql(f"""SELECT * FROM dss_corporate.gp_patient_list WHERE EXTRACT_DATE = '{ach_date}' """)
    df = df.drop('GP_PATIENT_LIST_KEY', 'SYSTEM_CREATED_DATE')
    
    return df
        
def get_active_practices(ach_date: str, database: str = 'dss_corporate', practice_table:str = 'ods_practice_v02', list_size_table: str = 'gp_patient_list') -> DataFrame: 
  
    """
    Function which retrieves active practices from ods_practice_v02 table.
    
    Args: 
      ach_date: Date to filter list size practices on, usually the 1st day of month of Reporting Period End Date. 
      database: Name of Database containing list size data. 
      practice_table: Name of Table containing gp practice data. 
      list_size_table: Table containing list size data. 
      
    Returns: 
      df: DataFrame containing open and active GP practices.
    """
  
    df = spark.sql(
      f"""SELECT a.CODE as PRACTICE_CODE 
          FROM {database}.{practice_table} a
          INNER JOIN
          (SELECT DISTINCT PRACTICE_CODE, EXTRACT_DATE FROM {database}.{list_size_table} WHERE EXTRACT_DATE = '{ach_date}') b
          ON a.CODE = b.PRACTICE_CODE 
          WHERE a.OPEN_DATE <= '{ach_date}' AND 
          (a.CLOSE_DATE >= '{ach_date}' OR a.CLOSE_DATE IS NULL)
          AND a.DSS_RECORD_START_DATE <= '{ach_date}' AND 
          (a.DSS_RECORD_END_DATE >= '{ach_date}' OR a.DSS_RECORD_END_DATE IS NULL)
          """
    )
    
    return df

def select_active_practices_only(df: DataFrame, active_practice_df: DataFrame) -> DataFrame:  
  
    """
    Joins main df with active practices dataframe object to subset active practices in main df. 
    
    Args: 
      df: Dataframe containing practice codes where you wish to select only the active ones.
      active_practice_df: Dataframe containing list of open active practice codes. 
      
    Returns: 
      df: DataFrame containing only open active practices. 
    """
  
    return df.join(active_practice_df, on = ['PRACTICE_CODE'], how = 'inner')
  
  
def prepare_data_for_aggregation(df: DataFrame, age_ref_data: DataFrame, pcn_mapping: DataFrame) -> DataFrame:
  
    """
    Prepares data for aggregration by joining with age_band and pcn_mapping reference dataframes.
    
    Args: 
      df: Dataframe containing lst size data.
      age_ref_data: Dataframe containing age reference data for age and gender list size grouping.
      pcn_mapping: Dataframe containing PCN mapping data.
      
    Returns: 
      df: DataFrame containing PCN mappings and list size age bands. 
    """
    
    df = (df.join(age_ref_data, on = [ F.col('key') == F.col('ORIGINAL')], how = 'left')
          .join(pcn_mapping, on = ['PRACTICE_CODE'], how = 'left')
          .dropna(subset = ['PCN_CODE'])
         )
    
    return df 
  
def aggregate_pcn_counts_by_age(df: DataFrame) -> DataFrame:
  
    """
    Aggregate counts by pcn and age band.
    
    Args: 
      df: DataFrame containing PCN codes and age bands. 
      
    Returns: 
      df: DataFrame containing list size by PCN aggregated to 5 year age groups.
    """

    return df.groupBy('PCN_CODE', 'PCN_NAME', 'AGE_FIVE_YEAR').agg(F.sum('val').alias('COUNT')) 

# COMMAND ----------

def retrieve_pcn_list_size(date: str, pcn_df: DataFrame, acsc_indicator_id = str) -> DataFrame:
  
  """
  Retrieves aggregated counts for list size broken down by PCN region and Age/Gender for specified date for purpose of standardisation. 
  
  Args:
    date: date of extract: Must be in format 'YYYY-MM-01'.
    pcn_df: Dataframe of pcn mapping data.
    acsc_indicator_id: Indicator ID to fill 'Attribute_ID' col.
    
  Returns: 
      df: DataFrame containing list size by PCN and age/gender. 
  """
  
  age_ref_data = spark.table('iif_indicators_collab.ref_age_columns')
  
  active_practices = get_active_practices(date)
  
  df = get_latest_list_size_data(date)
  df = select_active_practices_only(df, active_practices)
  df = transpose_columns_wide_to_long(df, ['PRACTICE_CODE', 'EXTRACT_DATE'])
  df = prepare_data_for_aggregation(df, age_ref_data, pcn_df)
  
  df = aggregate_pcn_counts_by_age(df)
  df = df.withColumn('Attribute_ID', F.lit(acsc_indicator_id))
  df = df.withColumn('EXTRACT_DATE', F.lit(date))
   
  return df

# COMMAND ----------

def create_denominator(date: str, pcn_mapping_df: DataFrame) -> DataFrame: 
  
    """
    Creates denominator counts for PCN. 
    
    Args: 
      date: Acheivement Date for current period. 
      pcn_mapping_df: Dataframe of PCN mapping data.
    
    Returns: 
      df: DataFrame containing list size by PCN for current and baseline years. 
    """
    
    dates = reportDate(date)
    
    current_year_date = dates.reporting_month_01
    previous_year_date = dates.get_baseline_month_01()
     
    # List size for current FY 
    current = retrieve_pcn_list_size(current_year_date, pcn_mapping_df, denominator_indicator_id)
    # List size for baseline FY
    previous = retrieve_pcn_list_size(previous_year_date, pcn_mapping_df, baseline_denominator_indicator_id)
    
    list_size_unioned = current.union(previous)
    
    return list_size_unioned

# COMMAND ----------

def retrieve_england_list_size_by_pcn(ach_date: str) -> DataFrame:
    
    """
    Returns dataframe with list size for England on ach_date. 
    
    Args:
      ach_date: Achievement date for which to retrieve list size data.
      
    Returns: 
      df: DataFrame containing list size by PCN. 
    """
    
    if isinstance(ach_date, str):
        assert datetime.datetime.strptime(ach_date, '%Y-%m-%d'), "Date format incorrect - makes sure its 'YYYY-MM-DD'"
        assert datetime.datetime.strptime(ach_date, '%Y-%m-%d').day == 1, "Please ensure day is set to '01'"
        
    if isinstance(ach_date, datetime.date):
        assert ach_date.day == 1, "Please ensure day is set to '01'"
        
    pcn_mapping = spark.table('iif_indicators_collab.pcn_mapping').select('PRACTICE_CODE', 'PCN_CODE')
    age_ref_df = spark.table('iif_indicators_collab.ref_age_columns').select('ORIGINAL', 'AGE_FIVE_YEAR')
    
    list_size = get_latest_list_size_data(ach_date)
    active_practices = get_active_practices(ach_date)
    active_list_size = select_active_practices_only(list_size, active_practices)
    
    df = transpose_columns_wide_to_long(active_list_size, ['PRACTICE_CODE', 'EXTRACT_DATE'])
    df = (
      df
      .join(age_ref_df, on = [F.col('key') == F.col('ORIGINAL')], how = 'left')
      .join(pcn_mapping, on = 'PRACTICE_CODE', how = 'left')
    )

    df = df.groupBy('AGE_FIVE_YEAR', 'EXTRACT_DATE').agg(F.sum('val').alias('Count'))
    
    return df

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL THAT USES THE ODS MAPPING API TO CREATE A PCN MAPPING 'MASTER_TABLE'
# MAGIC CREATE OR REPLACE TEMPORARY VIEW master_table AS 
# MAGIC   select 
# MAGIC   o.Name AS PRACTICE_NAME, 
# MAGIC   o.organisationID AS PRACTICE_CODE, 
# MAGIC   re.StartDate AS RELATIONSHIP_START_DATE, 
# MAGIC   re.EndDate AS RELATIONSHIP_END_DATE, 
# MAGIC   target.name AS PCN_NAME, 
# MAGIC   target.organisationID AS PCN_CODE 
# MAGIC   from dss_corporate.ODSAPIOrganisationDetails o, -- Returns Practice name/coded
# MAGIC        dss_corporate.ODSAPIRoleDetails r, 
# MAGIC           dss_corporate.ODSAPICodeSystemDetails rolename,
# MAGIC           dss_corporate.ODSAPIRelationshipDetails re, 
# MAGIC           dss_corporate.ODSAPICodeSystemDetails relname, -- Used as link between GP and PCN 
# MAGIC           dss_corporate.ODSAPIOrganisationDetails target, -- Used to get PCN name/code
# MAGIC           dss_corporate.ODSAPIRoleDetails targetrole, -- 
# MAGIC           dss_corporate.ODSAPICodeSystemDetails targetrolecode -- 
# MAGIC   where o.datetype ='Operational'
# MAGIC   AND target.datetype = 'Operational'
# MAGIC   AND r.datetype = 'Operational'
# MAGIC   AND targetRole.dateType = 'Operational'
# MAGIC   AND (rolename.displayname = 'GP PRACTICE' OR rolename.displayname = 'PRESCRIBING COST CENTRE') 
# MAGIC   AND o.organisationID = r.organisationID
# MAGIC   AND r.roleID = rolename.ID
# MAGIC   AND targetrole.roleID = 'RO272' and targetrole.primaryRole=1 --RO272 is PCN  
# MAGIC   AND re.organisationID = o.organisationID
# MAGIC   and re.relationshipID = relname.ID
# MAGIC   AND re.targetOrganisationID = target.organisationID 
# MAGIC   AND relname.displayname = 'IS PARTNER TO' -- COVID/nominated option excluded  
# MAGIC   AND target.organisationID = targetrole.organisationID
# MAGIC   AND targetrole.roleID = targetrolecode.ID
# MAGIC   order by o.name, target.name

# COMMAND ----------

def create_pcn_mapping(report_date: str) -> DataFrame:
  
    """
    Returns GP-PCN mappings which are active on the date argument parsed. 
    
    Args: 
      report_date: string of date to check active PCN mappings. 
      
    Returns: 
      df: DataFrame containing active GP-PCN mappings. 
      
    """

    df = spark.sql(f"""SELECT DISTINCT *, 
                      '{report_date}' AS EXTRACT_DATE
                      FROM master_table 
                      WHERE RELATIONSHIP_START_DATE <= '{report_date}' 
                      AND (RELATIONSHIP_END_DATE >= '{report_date}' OR RELATIONSHIP_END_DATE IS NULL)
                      ORDER BY PRACTICE_CODE""")
    
    windowSpec  = Window.partitionBy(F.col("PRACTICE_CODE")).orderBy(F.col("RELATIONSHIP_START_DATE").desc())

    df = df.withColumn("row_number", row_number().over(windowSpec)) 
    df = df.filter(F.col('row_number') == 1).drop('row_number')
    df = df.drop_duplicates()
    
    return df 