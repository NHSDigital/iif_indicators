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

@F.udf("string")
def wc_suppress(val:str) -> str:
  """
  A UDF to be supplied as the function for a .withColumn to suppress a given column.
  
  Example usage:
    df.withColumn("test_col", wc_suppress("test_col"))
  
  Args:
    val (int): The value of the column to be suppressed.
    
  Returns:
    str: * for values between [1-7], otherwise rounded to the nearest 5.
  """
  # Convert to int
  val = int(val)
  
  # Convert null to 0 and pass through any other non-numerical values
  if val == None: return '0'
  if type(val) not in (int,float): return str(val)
  
  # Pass 0
  if val == 0: return str(val)
  
  # Small value suppression
  if abs(val) >= 1 and abs(val) <= 7: return "*"

  # Round to nearest 5
  else: return str(5*round(val/5))


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
    print('date clause invoked')
    appended_archive_df = archive_df.union(indicator_landing_table_df)
    display(appended_archive_df)
    display(appended_archive_df.groupBy(F.col('CREATED_AT')).count())
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

# COMMAND ----------

def sum_and_prettify(df: DataFrame, grp_column_name: str, agg_column_name: str, output_label: str) -> DataFrame:
  """
  Sums up a dataframe based on the column_name argument and returns it ready to be appended to another table for acc08 and ehch04
    
    Args:
      df: A spark dataframe representing the filtered table
      grp_column_name: The column name by which to group by. GP Code column name
      agg_column_name: The column name by which to sum up by. Appointments column name
      output_label: The column name that's to be used for the aggregated value
    
    Returns:
      A spark dataframe that has been summed up by the agg_column_name, and grouped by the grp_column_name 
      that follows the schema of the output table. Any GP Practices that would not have a value receive a 
      value of 0 instead.
      
  """
  # Aggregate and add relevant columns
  df = (
    df
      .groupBy(grp_column_name)
      .agg(
        F.sum(agg_column_name)
        .cast('string').alias('SUM')
      )
      .select(
        F.col(grp_column_name).alias('ORG_ID'), 
        F.col('SUM').alias('ATTRIBUTE_VALUE')
      )
      .withColumn('FRIENDLY_IDENTIFIER', F.lit(output_label))
      .withColumn('ACHIEVEMENT_DATE', F.lit(report_end))
  )
  
  # Get all GP Practices that have values
  inserted_practices = df.select('ORG_ID').distinct().collect()
  inserted_practices = [practice[0] for practice in inserted_practices]
  
  # Compare GP practices with no values to original list, and build a row for each with value 0
  zero_practices = []
  for practice in gp_practices:
    if practice not in inserted_practices:
      zero_practices.append([practice, 0, output_label, report_end])
  
  # Add the rows of practices having value = 0 back into the DF
  zero_practices = spark.createDataFrame(zero_practices)
  df = df.union(zero_practices)
  
  # Add static columns
  attribute_details = get_attribute_uuid(output_label)
  df = df.withColumn('CREATED_AT', F.current_timestamp())
  df = df.withColumn('DATA_REFERENCE_IDENTIFIER', F.lit(attribute_details[0]))
  df = df.withColumn('ATTRIBUTE_IDENTIFIER', F.lit(attribute_details[1]))
  df = df.withColumn('ATTRIBUTE_TYPE', F.lit("int"))
  return df

# COMMAND ----------

def create_reduced_df(pcn_mapping_df,indicators_df):
  """
  Function to reduce the size of the df returned for acc08 and ehch04
  """
  reduced_df = indicators_df.join(pcn_mapping_df, indicators_df.ORG_ID == pcn_mapping_df.PRACTICE_CODE, "inner").drop("PRACTICE_CODE")
  reduced_df = reduced_df.select(["ORG_ID", "ACHIEVEMENT_DATE", "DATA_REFERENCE_IDENTIFIER", "ATTRIBUTE_IDENTIFIER", "FRIENDLY_IDENTIFIER", "ATTRIBUTE_VALUE", "ATTRIBUTE_TYPE", "CREATED_AT"]).orderBy(F.col("ORG_ID"))
  return reduced_df