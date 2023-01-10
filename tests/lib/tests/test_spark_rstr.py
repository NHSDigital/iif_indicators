# Databricks notebook source
import re
import unittest
import random


class TestContext:
  def __init__(self, nrows=1):
    self.df = spark.createDataFrame([[i] for i in range(nrows)], 'row_number: int')
    
  def assert_matches(self, pattern, column):
    value = self.df.select(column.alias('value')).first()['value']
    errmsg = '{} does not match {}'.format(value, pattern)
    assert re.match(pattern, value), errmsg
    
    
context = TestContext()
assert_matches = context.assert_matches

# COMMAND ----------

# MAGIC %run ../../tests/init_tests

# COMMAND ----------

# MAGIC %run ../spark_rstr

# COMMAND ----------

STAR_PLUS_LIMIT = 10  # Reduced from 100 to limit runtime of tests
MAX_SAMPLES = 10  # Reduced from 100 to limit runtime of tests

# COMMAND ----------

class TestSparkRstr(unittest.TestCase):
  def setUp(self):
    self.rs = SparkRstr()

  def test_specific_length(self):
    assert_matches('^A{5}$', self.rs.rstr('A', 5))

  def test_length_range(self):
    assert_matches('^A{11,20}$', self.rs.rstr('A', 11, 20))

  def test_custom_alphabet(self):
    assert_matches('^A{1,10}$', self.rs.rstr('AA'))

  def test_alphabet_as_list(self):
    assert_matches('^A{1,10}$', self.rs.rstr(['A', 'A']))

  def test_include(self):
    assert_matches('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include='@'))

  def test_include_specific_length(self):
    '''
    Verify including characters doesn't make the string longer than intended.
    '''
    assert_matches('^[ABC@]{5}$', self.rs.rstr('ABC', 5, include='@'))

  def test_exclude(self):
    df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
    col = self.rs.rstr('ABC', exclude='C').alias('value')
    for row in df.select(col).collect():
      assert 'C' not in row['value']

  def test_include_as_list(self):
    assert_matches('^[ABC]*@[ABC]*$', self.rs.rstr('ABC', include=['@']))

  def test_exclude_as_list(self):            
    df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
    col = self.rs.rstr('ABC', exclude=['C']).alias('value')
    for row in df.select(col).collect():
      assert 'C' not in row['value']


class TestDigits(unittest.TestCase):
  def setUp(self):
    self.rs = SparkRstr()

  def test_all_digits(self):
    assert_matches(r'^\d{1,10}$', self.rs.digits())

  def test_digits_include(self):
    assert_matches(r'^\d*@\d*$', self.rs.digits(include='@'))

  def test_digits_exclude(self):
    df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
    col = self.rs.digits(exclude='5').alias('value')
    for row in df.select(col).collect():
      assert '5' not in row['value']


class TestNondigits(unittest.TestCase):
    def setUp(self):
        self.rs = SparkRstr()

    def test_nondigits(self):
        assert_matches(r'^\D{1,10}$', self.rs.nondigits())

    def test_nondigits_include(self):
        assert_matches(r'^\D*@\D*$', self.rs.nondigits(include='@'))

    def test_nondigits_exclude(self):
      df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
      col = self.rs.nondigits(exclude='A').alias('value')
      for row in df.select(col).collect():
        assert 'A' not in row['value']


class TestLetters(unittest.TestCase):
  def setUp(self):
    self.rs = SparkRstr()

  def test_letters(self):
    assert_matches(r'^[a-zA-Z]{1,10}$', self.rs.letters())

  def test_letters_include(self):
    assert_matches(r'^[a-zA-Z]*@[a-zA-Z]*$', self.rs.letters(include='@'))

  def test_letters_exclude(self):
    df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
    col = self.rs.letters(exclude='A').alias('value')
    for row in df.select(col).collect():
      assert 'A' not in row['value']


class TestUnambiguous(unittest.TestCase):
  def setUp(self):
    self.rs = SparkRstr()

  def test_unambiguous(self):
    assert_matches('^[a-km-zA-HJ-NP-Z2-9]{1,10}$', self.rs.unambiguous())

  def test_unambiguous_include(self):
    assert_matches('^[a-km-zA-HJ-NP-Z2-9@]{1,10}$', self.rs.unambiguous(include='@'))

  def test_unambiguous_exclude(self):
    df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
    col = self.rs.letters(exclude='A').alias('value')
    for row in df.select(col).collect():
      assert 'A' not in row['value']


class TestCustomAlphabets(unittest.TestCase):
  def test_alphabet_at_instantiation(self):
    rs = SparkRstr(vowels='AEIOU')
    assert_matches('^[AEIOU]{1,10}$', rs.vowels())

  def test_add_alphabet(self):
    rs = SparkRstr()
    rs.add_alphabet('evens', '02468')
    assert_matches('^[02468]{1,10}$', rs.evens())

# COMMAND ----------

# Load the tests from each test case
load_tests = unittest.defaultTestLoader.loadTestsFromTestCase
test_cases = [
  TestSparkRstr, 
  TestDigits, 
  TestNondigits, 
  TestLetters,
  TestUnambiguous,
  TestCustomAlphabets
]
test_suites = []

# Iteratively create test suites by loading tests
for test_case in test_cases:
  test_suites.append(load_tests(test_case))

# Run the tests
suite = unittest.TestSuite(tests=test_suites)
runner = unittest.TextTestRunner()
runner.run(suite)

# COMMAND ----------

class TestSparkXeger(unittest.TestCase):
    def setUp(self):
        self.rs = SparkRstr()

    def test_literals(self):
        pattern = r'foo'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_dot(self):
        '''
        Verify that the dot character doesn't produce newlines.
        See: https://bitbucket.org/leapfrogdevelopment/rstr/issue/1/
        '''
        pattern = r'.+'
        df = spark.createDataFrame([[i] for i in range(100)], 'x: int')
        df = df.select(self.rs.xeger(pattern).alias('value'))
        for row in df.collect():  
            assert re.match(pattern, row['value'])

    def test_digit(self):
        pattern = r'\d'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_nondigits(self):
        pattern = r'\D'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_literal_with_repeat(self):
        pattern = r'A{3}'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_literal_with_range_repeat(self):
        pattern = r'A{2, 5}'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_word(self):
        pattern = r'\w'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_nonword(self):
        pattern = r'\W'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_or(self):
        pattern = r'foo|bar'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_or_with_subpattern(self):
        pattern = r'(foo|bar)'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_range(self):
        pattern = r'[A-F]'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_character_group(self):
        pattern = r'[ABC]'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_carot(self):
        pattern = r'^foo'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_dollarsign(self):
        pattern = r'foo$'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_not_literal(self):
        pattern = r'[^a]'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_negation_group(self):
        pattern = r'[^AEIOU]'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_lookahead(self):
        pattern = r'foo(?=bar)'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_lookbehind(self):
        pattern = r'(?<=foo)bar'
        df = spark.createDataFrame([[1]], 'x: int')
        value = df.select(self.rs.xeger(pattern).alias('value')).first()['value']
        errmsg = '{} does not match {}'.format(value, pattern)
        assert re.search(pattern, value), errmsg

    def test_backreference(self):
        pattern = r'(foo|bar)baz\1'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_greedy(self):
        pattern = r'a*'
        assert_matches(pattern, self.rs.xeger(pattern))

    def test_zero_or_more_non_greedy(self):
        pattern = r'a*?'
        assert_matches(pattern, self.rs.xeger(pattern))

# COMMAND ----------

# Run the tests
suite = unittest.defaultTestLoader.loadTestsFromTestCase(TestSparkXeger)
runner = unittest.TextTestRunner()
runner.run(suite)

# COMMAND ----------

