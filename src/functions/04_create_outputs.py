# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import datetime

# COMMAND ----------

def prepare_cqrs_udal_data(table: str, org_id_col: str, report_date: datetime, rename_dict: dict, count_column: str) -> DataFrame:
  
  """
  Takes data from indicators table and transforms into correct format for publication.

  Args: 
    table: Name of the table to extract data from.
    org_id_col: Column containing org ID.
    report_date: Reporting date used to fill achievement date col.    
    rename_dict: Dictionary by which to convert CQRS attribute IDs to plain text measures. 
    count_column: Name of the column that contains count of indicator. 

  Returns: 
    df: DataFrame containing correct schema and formatting for ingestion by CQRS. 
  """
  
  df = (
    spark.table(f'iif_indicators_collab.{table}')
    .withColumnRenamed(org_id_col, 'ORG_ID')
    .withColumn('ACHIEVEMENT_DATE', F.lit(report_date))
    .withColumn('ATTRIBUTE_TYPE', F.lit('int'))
    .withColumnRenamed('Attribute_ID', 'ATTRIBUTE_IDENTIFIER')
    .replace(to_replace=rename_dict, subset=['ATTRIBUTE_IDENTIFIER'])
  )
  
  df = (
    df
    .groupBy('ORG_ID', 'ACHIEVEMENT_DATE', 'ATTRIBUTE_IDENTIFIER', 'ATTRIBUTE_TYPE')
    .agg(F.sum(F.col(count_column)).cast('string').alias('ATTRIBUTE_VALUE'))
    .withColumn('ATTRIBUTE_VALUE', F.round(F.col('ATTRIBUTE_VALUE')).cast('integer').cast('string'))
    .join(attribute_codes, 'ATTRIBUTE_IDENTIFIER', 'left')
    .withColumn('CREATED_AT', F.current_timestamp())
    .select('ORG_ID', 'ACHIEVEMENT_DATE', 'DATA_REFERENCE_IDENTIFIER', 'ATTRIBUTE_IDENTIFIER', 'ATTRIBUTE_VALUE', 'ATTRIBUTE_TYPE', 'CREATED_AT')
    .orderBy('ORG_ID', 'ACHIEVEMENT_DATE', 'DATA_REFERENCE_IDENTIFIER', 'ATTRIBUTE_IDENTIFIER')
  )

  return df

# COMMAND ----------

def prepare_publication_data(table: str, org_type: str, org_id_col: str, org_name_col: str, financial_year: str, report_date_str: str, ncd_id: str, rename_dict: dict, count_column: str) -> DataFrame:
  
  """
  Takes data from indicators table and transforms into correct format for publication.

  Args: 
    table: Name of the table to extract data from.
    org_type: Type of organisation e.g. PCN, GP pratice.
    org_id_col: Column containing org ID.
    org_name_col: Column containing org name.
    financial_year: Financial year in format yyyy e.g. 2122.
    report_date_str: String of reporting date used to fill achievement date col.    
    ncd_id: Indicator code to fill IND_CODE column.
    rename_dict: Dictionary by which to convert CQRS attribute IDs to plain text measures. 
    count_column: Name of the column that contains count of indicator. 

  Returns: 
    df: DataFrame containing correct schema and formatting for ingestion by CQRS. 
  """
  
  df = (
    spark.table(f'iif_indicators_collab.{table}')
    .withColumn('ORGANISATION_TYPE', F.lit(org_type))
    .withColumnRenamed(org_id_col, 'ORGANISATION_CODE')
    .withColumnRenamed(org_name_col, 'ORGANISATION_NAME')
    .withColumn('QUALITY_SERVICE', F.lit(f'NCD{financial_year}'))
    .withColumn('ACH_DATE', F.lit(report_date_str))
    .withColumn('IND_CODE', F.lit(ncd_id))
    .withColumnRenamed('Attribute_ID', 'MEASURE')
    .replace(to_replace=rename_dict, subset=['MEASURE'])
  )
  
  df = (
    df
    .groupBy('ORGANISATION_TYPE', 'ORGANISATION_CODE', 'ORGANISATION_NAME', 'QUALITY_SERVICE', 'ACH_DATE', 'IND_CODE', 'MEASURE')
    .agg(F.sum(F.col(count_column)).alias('VALUE'))
    .withColumn('CREATED_AT', F.current_timestamp())
    .select('ORGANISATION_TYPE', 'ORGANISATION_CODE', 'ORGANISATION_NAME', 'QUALITY_SERVICE', 'ACH_DATE', 'IND_CODE', 'MEASURE', 'VALUE', 'CREATED_AT')
    .orderBy('ORGANISATION_CODE', 'ORGANISATION_NAME', 'QUALITY_SERVICE', 'ACH_DATE', 'IND_CODE', 'MEASURE')
  )

  return df

# COMMAND ----------

def archive_landing_table(indicator_landing_table_df: DataFrame, archive_table_name: str):
  """
  Function to archive current landing table into archive table as long as 'CREATED_AT' timestamp is more recent than what is in the archive table (which should be impossible).
  
  Args:
    indicator_landing_table_df: Dataframe containing indicator counts.
    archive_table_name: Name of indicator specific archive table.
  """
  archive_df = spark.table(f'iif_indicators_collab.{archive_table_name}')
  landing_table_created_at = indicator_landing_table_df.select(F.max('CREATED_AT')).collect()[0][0]
  archive_table_most_recent_created_at = archive_df.select(F.max('CREATED_AT')).collect()[0][0]

  if (landing_table_created_at > archive_table_most_recent_created_at):
    appended_archive_df = archive_df.union(indicator_landing_table_df)
    create_table(archive_table_name, 'iif_indicators_collab', appended_archive_df)
    print(f'Table created at {landing_table_created_at} archived')

  else:
    print('This table has already been saved!')

# COMMAND ----------

def update_indicators_for_cqrs_or_udal_table(indicator_landing_table_df: DataFrame, table: str):
  """
  Function to update the cqrs/udal indicators table using the data stored in the landing table. 
  Will delete any matching org-date-indicator data if already present and then append the table with the new data.
  
  Args:
    acsc_indicator_landing_table_df: Dataframe containing indicator counts.
    table: Determines whether data will be saved to cqrs or udal.
  """
  df = spark.table(f'iif_indicators_collab.indicators_for_{table}')

  df_deleted = df.join(indicator_landing_table_df, on=['ORG_ID', 'ACHIEVEMENT_DATE', 'DATA_REFERENCE_IDENTIFIER', 'ATTRIBUTE_IDENTIFIER'], how='left_anti')
  df_deleted = df_deleted.withColumn('ACHIEVEMENT_DATE', F.to_date(F.col("ACHIEVEMENT_DATE"),"yyyy-MM-dd"))
  df_updated = df_deleted.union(indicator_landing_table_df)
  create_table(f'indicators_for_{table}', 'iif_indicators_collab', df_updated)

# COMMAND ----------

def update_indicators_for_publication_table(indicator_landing_table_df: DataFrame):
  """
  Function to update the publication indicators table using the data stored in the landing table. 
  Will delete any matching org-date-indicator data if already present and then append the table with the new data.
  
  Args:
    acsc_indicator_landing_table_df: Dataframe containing indicator counts.
  """
  
  indicator_landing_table_df = indicator_landing_table_df.select('ORGANISATION_TYPE', 'ORGANISATION_CODE', 'ORGANISATION_NAME', 'QUALITY_SERVICE', 'ACH_DATE', 'IND_CODE', 'MEASURE', 'VALUE').drop('CREATED_AT')
  create_table('indicators_for_publication', 'iif_indicators_collab', indicator_landing_table_df)