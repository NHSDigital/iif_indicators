# Databricks notebook source
import pyspark.sql.functions as F
from dataclasses import dataclass, field
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame
from pandas.tseries.offsets import MonthEnd
from typing import List

import functools
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def create_table(table_name: str, database_name: str, dataframe_name: DataFrame): 
  
  """
  Writes or overwrites dataframe to database, and sets table ownership to group.

  Args: 
    table_name: Desired table name
    database_name: Name of database to write  
    dataframe_name: Name of dataframe to save
  """
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
 
  dataframe_name.write.mode("overwrite").saveAsTable(f"{database_name}.{table_name}")
  
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
  print(f"Table {database_name}.{table_name} successfully updated.")

  
def transpose_columns_wide_to_long(df: DataFrame, by: List) -> DataFrame:
    
  """
  Transposes columns in a df from wide to long format. 

  Args: 
    df: dataframe to transform 
    by: name of column or list containing names of columns to set as index columns.  
  
  Returns: transposed df
  
  E.g. 
  [('Practice_code','Males_0_to_10','Males_11_to_20'),
   ('A1111','2','4'),
   ('A2222','1','7')
  ]
  ->
  [('Practice_code','key','val'),
   ('A1111','Males_0_to_10','2'),
   ('A1111','Males_11_to_20','4'),
   ('A2222','Males_0_to_10','1'),
   ('A2222','Males_11_to_20','7')
  ]
  """
    
  # Filter dtypes and split into column names and type description
  cols, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c not in by))
  assert len(set(dtypes)) == 1, "All columns have to be of the same type"

  # Create and explode an array of (column_name, column_value) structs
  kvs = F.explode(F.array([
    F.struct(F.lit(c).alias("key"), F.col(c).alias("val")) for c in cols
  ])).alias("kvs")

  return df.select(by + [kvs]).select(by + ["kvs.key", "kvs.val"])
  
  
def parse_date_columns_to_datetime(df: DataFrame, columns: List) -> DataFrame:  
  
  """
  Converts string columns in list to datetime columns. 
  
  Args: 
    df: Dataframe containing columns to parse. 
    columns: List of columns to convert to datetime.
  """
  
  for column in columns: 
    df[column] = pd.to_datetime(df[column])
  
  
  return df 


def convert_column_to_array(df: DataFrame, orig_column: str, new_column:str) -> DataFrame: 
  
    """
    Converts column of single, concatenated string and converts to array.
    Splits items on commas.
    
    Args:
      df: Dataframe to transform.
      orig_column: Name of column to convert.
      new_column: Name of new converted column.
    """
  
    df = df.withColumn(new_column, 
                       F.array_distinct(
                         F.split(
                           F.col(
                             orig_column), ",")
                       )
                      )
    
    return df
  
def convert_list_to_array(lists: List) -> F.array: 
    
  """
  Converts list to array type

  Args:
    lists: List to be converted
  """

  return F.array([F.lit(i) for i in lists])
  

# COMMAND ----------

@dataclass
class reportDate:
  
  """
  A date object that takes in a date string and calculates a variety of dates required for the publication. 
  """
  
  reporting_period_end: str
  baseline_report_period_end: datetime.date = field(init = None) 
  financial_year_start: datetime.date = field(init = None)
  baseline_financial_year_start: datetime.date = field(init = None) 
  reporting_month_01: datetime.date = field(init = None) 
  
  
  def __post_init__(self):
    
      self.validate_date_is_end_of_month()
      self.baseline_report_period_end = self.get_baseline_acheivement_date
      self.financial_year_start = self.calculate_financial_year
      self.reporting_month_01 = self.calculate_first_day_reporting_month
      self.baseline_financial_year_start = self.get_baseline_financial_year_start
      
      
  
  @property
  def calculate_financial_year(self):
      
      "Calculates FY start date based on date attribute"
    
      date = datetime.datetime.strptime(self.reporting_period_end, "%Y-%m-%d").date()
      year_of_date = date.year
      financial_year_start_date = datetime.datetime.strptime(str(year_of_date)+"-04-01","%Y-%m-%d").date()
      
      if date < financial_year_start_date:
            return datetime.datetime.strptime(f'{financial_year_start_date.year-1}-04-01', "%Y-%m-%d").date()
      else:
            return datetime.datetime.strptime(f'{financial_year_start_date.year}-04-01', "%Y-%m-%d").date()
  
  @property
  def calculate_first_day_reporting_month(self): 
    
      "Returns the first date of report end month. Useful for joining HES to list size extract."
      
    
      return datetime.datetime.strptime(self.reporting_period_end, "%Y-%m-%d").date().replace(day=1)
      
  
  def get_baseline_acheivement_date(self):
    
      "Subtracts year from report period end to get baseline acheivements date."
    
      return (pd.to_datetime(self.reporting_period_end) - pd.DateOffset(years=1)).date()
    
  def get_baseline_financial_year_start(self):
    
      "Subtracts year from financial year to get baseline financial year start."
    
      return (pd.to_datetime(self.financial_year_start) - pd.DateOffset(years=1)).date()
    
  
  def get_baseline_month_01(self):
    
      "Subtracts year from report period end to get baseline acheivements date."
    
      return (pd.to_datetime(self.reporting_month_01) - pd.DateOffset(years=1)).date()
    
    
  def validate_date_is_end_of_month(self):

      "Helper function to identify if date entered is valid end of month date "
      date_test = (pd.to_datetime(self.reporting_period_end) + MonthEnd(0)).date()
      assert date_test == pd.to_datetime(self.reporting_period_end).date(), "Date parsed is not end of month date"
      


# COMMAND ----------

def letter_number_generator(start: List, letter: str, numrange: int) -> List:
  
  """
  Returns given list of numbers with a specified letter attached to the start and numbers incrementing from 0 to a specified amount attached to the end.

  Args:
    start: List of numbers that will come after the letter, and before the generated final number(s)
    letter: Letter to begin code with.
    numrange: Range of numbers to generate in the code. 

  Returns: 
    final_list: DataFrame containing attribute codes. 
  """

  final_list= []
  for i in start:
    list_here = [letter + f"{i}" + f"{x}" for x in range(0,numrange)]

    final_list.extend(list_here)

  return final_list

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

def are_dfs_equal(df1, df2):
  df1 = df1.orderBy('ORG_ID', 'ACHIEVEMENT_DATE', 'ATTRIBUTE_IDENTIFIER', 'DATA_REFERENCE_IDENTIFIER')
  df2 = df2.orderBy('ORG_ID', 'ACHIEVEMENT_DATE', 'ATTRIBUTE_IDENTIFIER', 'DATA_REFERENCE_IDENTIFIER')
  
  if df1.schema != df2.schema:
      print('schema mismatch')
      return False
  if df1.collect() != df2.collect():
      print('data mismatch')
      return False
  return True

# COMMAND ----------


# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

# COMMAND ----------

def add_to_table(df: DataFrame, table_name: str) -> None:
  """
  Creates a table if it doesn't exist and will add the provided dataframe to it.
  Will not allow for duplicate entries (protected by timestamp)
  
  Args:
    df: The dataframe that should be saved
    table_name: The full name of the table that should be saved
    
  Returns:
    None
  """
  #try:
    
  #existing_df = spark.table(table_name)
  #df = unionAll([existing_df, df])
  df.write.mode("overwrite").insertInto(table_name)
 # except:
  #  pass
 #  df.write.mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

def get_attribute_uuid(attribute_description : str):
  """
  Gets the Indicator ID and Attribute_ID from the attribute mapping table and returns those values as a tuple
  
  Args:
    attribute_description: The description (key) of the indicator value i.e. "ACC-08 Numerator"
    
  Returns:
    Tuple: 
      [0] = Indicator ID also referred to as DATA_REFERENCE_IDENTIFIER. For example ACC-08 falls under indicator NCD026
      [1] = Attribute_ID. For example referring to the Numerator of ACC-08
  """
  
  df = spark.sql("""
    SELECT Indicator, Attribute_ID
    FROM iif_indicators_collab.attribute_mapping 
    WHERE Attribute_description = '{description}'
    """.format(description=attribute_description)).first()

  if df == None:
    return ("INDICATOR NOT FOUND IN LOOKUP", "INDICATOR NOT FOUND IN LOOKUP")
  return df[0], df[1]

# COMMAND ----------

def get_financial_year():
  """
  Returns the start of the current financial year 
  """
  
  start_financial_year = datetime.date.today().replace(day=1).replace(month=4)
  
  # Say it's Feb 2022, we need to subtract 1 year from our 01/04 date
  if start_financial_year > report_end:
    current_year = start_financial_year.year  
    start_financial_year = start_financial_year.replace(year=current_year-1)
  
  return start_financial_year

# COMMAND ----------

def check_schemas_match(df1: DataFrame,
                        df2: DataFrame,
                        allow_nullable_schema_mismatch=True
                       ) -> bool:
  """
  Returns True if the dataframe schemas match, or False otherwise.
  
  If allow_nullable_schema_mismatch is False then the nullability of the columns must also match.
  If True, nullability isn't included in the check.
  """
  
  if df1.schema == df2.schema:
    return True
  elif not allow_nullable_schema_mismatch:
    print('allow_nullable_schema_mismatch')
    return False
  
  if len(df1.schema) != len(df2.schema):
    print('schema length mismatch')    
    return False
  
  for field_1, field_2 in zip(df1.schema, df2.schema):
    if field_1.name != field_2.name:
      print('name error', field_1, field_2)      
      return False
    if field_1.dataType != field_2.dataType:
      print('datatype error', field_1, field_2)
      
      return False


# COMMAND ----------

def are_dfs_equal(df1, df2):
  df1_cols = sorted(df1.columns)
  df2_cols = sorted(df2.columns)
  
  if df1_cols!=df2_cols:
    print('cols are not equal')
    
  # sort by columns
  df1 = df1.orderBy(df1_cols)
  df2 = df2.orderBy(df2_cols)
  
  #check schemas match
  schema_bool = check_schemas_match(df1, df2)
  if schema_bool==False:
    print('schema mismatch')
    return False
    
  #check data matches
  if df1.collect() != df2.collect():
    for i in range(0,len(df1.collect())):
      if df1.collect()[i]!=df2.collect()[i]:
        print(df1.collect()[i], '\n')
        print(df2.collect()[i], '\n')
        print('data mismatch')
        return False
  return True

# COMMAND ----------

