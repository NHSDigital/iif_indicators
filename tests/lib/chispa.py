# Databricks notebook source
README = """
This notebook reproduces the functionality of the [chispa package v0.8.2](https://github.com/MrPowers/chispa/releases/tag/v0.8.2). This package should be added as a dependency (e.g. via requirements.txt) once it is possible to install these requirements in DAE.

Notes
-----
- All the functionality in chispa is contained except 
  - chispa/prettytable.py, which is in an accompanying notebook that is run in the cells below
  - six, which is imported
"""

# COMMAND ----------

# chispa/bcolors.py


class bcolors:
    NC='\033[0m' # No Color, reset all

    Bold='\033[1m'
    Underlined='\033[4m'
    Blink='\033[5m'
    Inverted='\033[7m'
    Hidden='\033[8m'

    Black='\033[30m'
    Red='\033[31m'
    Green='\033[32m'
    Yellow='\033[33m'
    Blue='\033[34m'
    Purple='\033[35m'
    Cyan='\033[36m'
    LightGray='\033[37m'
    DarkGray='\033[30m'
    LightRed='\033[31m'
    LightGreen='\033[32m'
    LightYellow='\033[93m'
    LightBlue='\033[34m'
    LightPurple='\033[35m'
    LightCyan='\033[36m'
    White='\033[97m'


def blue(s: str) -> str:
  return bcolors.LightBlue + str(s) + bcolors.LightRed


# COMMAND ----------

# MAGIC %run ./prettytable

# COMMAND ----------

# chispa/column_comparer.py

# from chispa.bcolors import *
# from chispa.prettytable import PrettyTable


class ColumnsNotEqualError(Exception):
    """The columns are not equal"""
    pass


def assert_column_equality(df, col_name1, col_name2):
    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
        zipped = list(zip(colName1Elements, colName2Elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if elements[0] == elements[1]:
                first = bcolors.LightBlue + str(elements[0]) + bcolors.LightRed
                second = bcolors.LightBlue + str(elements[1]) + bcolors.LightRed
                t.add_row([first, second])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise ColumnsNotEqualError("\n" + t.get_string())


def assert_approx_column_equality(df, col_name1, col_name2, precision):
    elements = df.select(col_name1, col_name2).collect()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    all_rows_equal = True
    zipped = list(zip(colName1Elements, colName2Elements))
    t = PrettyTable([col_name1, col_name2])
    for elements in zipped:
        first = bcolors.LightBlue + str(elements[0]) + bcolors.LightRed
        second = bcolors.LightBlue + str(elements[1]) + bcolors.LightRed
        # when one is None and the other isn't, they're not equal
        if (elements[0] == None and elements[1] != None) or (elements[0] != None and elements[1] == None):
            all_rows_equal = False
            t.add_row([str(elements[0]), str(elements[1])])
        # when both are None, they're equal
        elif elements[0] == None and elements[1] == None:
            t.add_row([first, second])
        # when the diff is less than the threshhold, they're approximately equal
        elif abs(elements[0] - elements[1]) < precision:
            t.add_row([first, second])
        # otherwise, they're not equal
        else:
            all_rows_equal = False
            t.add_row([str(elements[0]), str(elements[1])])
    if all_rows_equal == False:
        raise ColumnsNotEqualError("\n" + t.get_string())


# COMMAND ----------

# MAGIC %run ./prettytable

# COMMAND ----------

# chispa/dataframe_comparer.py

# from chispa.prettytable import PrettyTable
# from chispa.bcolors import *
# from chispa.schema_comparer import assert_schema_equality
# from chispa.row_comparer import *
# import chispa.six as six
from functools import reduce
import six


class DataFramesNotEqualError(Exception):
    """The DataFrames are not equal"""
    pass


def assert_df_equality(df1, df2, ignore_nullable=False, transforms=None, allow_nan_equality=False, ignore_column_order=False, ignore_row_order=False):
    if transforms is None:
        transforms = []
    if ignore_column_order:
        transforms.append(lambda df: df.select(sorted(df.columns)))
    if ignore_row_order:
        transforms.append(lambda df: df.sort(df.columns))
    df1 = reduce(lambda acc, fn: fn(acc), transforms, df1)
    df2 = reduce(lambda acc, fn: fn(acc), transforms, df2)
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    if allow_nan_equality:
        assert_generic_rows_equality(df1, df2, are_rows_equal_enhanced, [True])
    else:
        assert_basic_rows_equality(df1, df2)


def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    if df1.collect() != df2.collect():
        return False
    return True


def assert_approx_df_equality(df1, df2, precision, ignore_nullable=False):
    assert_schema_equality(df1.schema, df2.schema, ignore_nullable)
    assert_generic_rows_equality(df1, df2, are_rows_approx_equal, [precision])


def assert_generic_rows_equality(df1, df2, row_equality_fun, row_equality_fun_args):
    df1_rows = df1.collect()
    df2_rows = df2.collect()
    zipped = list(six.moves.zip_longest(df1_rows, df2_rows))
    t = PrettyTable(["df1", "df2"])
    allRowsEqual = True
    for r1, r2 in zipped:
        # rows are not equal when one is None and the other isn't
        if (r1 is not None and r2 is None) or (r2 is not None and r1 is None):
            allRowsEqual = False
            t.add_row([r1, r2])
        # rows are equal
        elif row_equality_fun(r1, r2, *row_equality_fun_args):
            first = bcolors.LightBlue + str(r1) + bcolors.LightRed
            second = bcolors.LightBlue + str(r2) + bcolors.LightRed
            t.add_row([first, second])
        # otherwise, rows aren't equal
        else:
            allRowsEqual = False
            t.add_row([r1, r2])
    if allRowsEqual == False:
        raise DataFramesNotEqualError("\n" + t.get_string())


def assert_basic_rows_equality(df1, df2):
    rows1 = df1.collect()
    rows2 = df2.collect()
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise DataFramesNotEqualError("\n" + t.get_string())

# COMMAND ----------

# chispa/number_helpers.py

import math


def isnan(x):
    try:
        return math.isnan(x)
    except TypeError:
        return False


def nan_safe_equality(x, y) -> bool:
    return (x == y) or (isnan(x) and isnan(y))

# COMMAND ----------

# chispa/row_comparer.py

from pyspark.sql import Row
# from chispa.number_helpers import nan_safe_equality


def are_rows_equal(r1: Row, r2: Row) -> bool:
    return r1 == r2


def are_rows_equal_enhanced(r1: Row, r2: Row, allow_nan_equality: bool) -> bool:
    if r1 is None and r2 is None:
        return True
    if (r1 is None and r2 is not None) or (r2 is None and r1 is not None):
        return False
    d1 = r1.asDict()
    d2 = r2.asDict()
    if allow_nan_equality:
        for key in d1.keys() & d2.keys():
            if not(nan_safe_equality(d1[key], d2[key])):
                return False
        return True
    else:
        return r1 == r2


def are_rows_approx_equal(r1: Row, r2: Row, precision: float) -> bool:
    if r1 is None and r2 is None:
        return True
    if (r1 is None and r2 is not None) or (r2 is None and r1 is not None):
        return False
    d1 = r1.asDict()
    d2 = r2.asDict()
    allEqual = True
    for key in d1.keys() & d2.keys():
        if isinstance(d1[key], float) and isinstance(d2[key], float):
            if abs(d1[key] - d2[key]) > precision:
                allEqual = False
        elif d1[key] != d2[key]:
            allEqual = False
    return allEqual


# COMMAND ----------

# MAGIC %run ./prettytable

# COMMAND ----------

# chispa/schema_comparer.py
  
# from chispa.prettytable import PrettyTable
# from chispa.bcolors import *
# import chispa.six as six
# from chispa.structfield_comparer import are_structfields_equal
import six


class SchemasNotEqualError(Exception):
    """The schemas are not equal"""
    pass


def assert_schema_equality(s1, s2, ignore_nullable=False):
    if ignore_nullable:
        assert_schema_equality_ignore_nullable(s1, s2)
    else:
        assert_basic_schema_equality(s1, s2)


def assert_basic_schema_equality(s1, s2):
    """ Modified by AJ since the original - better outputs for nested structs
    
    TODO: modify other schema functions
    """
    def _assert_fields(s1, s2, t):
          zipped = list(six.moves.zip_longest(s1, s2))
          
          for sf1, sf2 in zipped:
              if sf1 == sf2:
                  t.add_row([blue(sf1), blue(sf2)])
              else:
                  if isinstance(sf1.dataType, T.StructType) and isinstance(sf2.dataType, T.StructType):
                      _assert_fields(sf1.dataType, sf2.dataType, t)
                  elif isinstance(sf1.dataType, T.ArrayType) and isinstance(sf2.dataType, T.ArrayType):
                      _assert_fields(sf1.dataType.elementType, sf2.dataType.elementType, t)
                  else:
                      t.add_row([sf1, sf2])
                      
          raise SchemasNotEqualError("\n" + t.get_string())
          
    if s1 != s2:
        t = PrettyTable(["schema1", "schema2"])
        _assert_fields(s1, s2, t)
        
        


def assert_schema_equality_ignore_nullable(s1, s2):
    if are_schemas_equal_ignore_nullable(s1, s2) == False:
        t = PrettyTable(["schema1", "schema2"])
        zipped = list(six.moves.zip_longest(s1, s2))
        for sf1, sf2 in zipped:
            if are_structfields_equal(sf1, sf2, True):
                t.add_row([blue(sf1), blue(sf2)])
            else:
                t.add_row([sf1, sf2])
        raise SchemasNotEqualError("\n" + t.get_string())


def are_schemas_equal_ignore_nullable(s1, s2):
    if len(s1) != len(s2):
        return False
    zipped = list(six.moves.zip_longest(s1, s2))
    for sf1, sf2 in zipped:
        names_equal = sf1.name == sf2.name
        types_equal = check_type_equal_ignore_nullable(sf1, sf2)
        if not names_equal or not types_equal:
          return False
    return True


def check_type_equal_ignore_nullable(sf1, sf2):
    """Checks StructField data types ignoring nullables.

    Handles array element types also.
    """
    dt1, dt2 = sf1.dataType, sf2.dataType
    if dt1.typeName() == dt2.typeName():
        # Account for array types by inspecting elementType.
        if dt1.typeName() == 'array':
            return dt1.elementType == dt2.elementType
        else:
            return True
    else:
        return False

# COMMAND ----------

# MAGIC %run ./prettytable

# COMMAND ----------

# chispa/structfield_comparer.py


def are_structfields_equal(sf1, sf2, ignore_nullability=False):
    if ignore_nullability:
        if sf1 is None and sf2 is not None:
            return False
        elif sf1 is not None and sf2 is None:
            return False
        elif sf1.name != sf2.name or sf1.dataType != sf2.dataType:
            return False
        else:
            return True
    else:
        return sf1 == sf2