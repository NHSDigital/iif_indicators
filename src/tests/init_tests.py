# Databricks notebook source
# MAGIC %run ./lib/chispa

# COMMAND ----------

# MAGIC %run ./lib/nutter/runtime

# COMMAND ----------

from __future__ import annotations
import unittest


assert_raises = unittest.TestCase().assertRaises
assert_dict_equal = unittest.TestCase().assertDictEqual


def init_tests():
  results = {}
  
  def run_test_fixture(cls):
    # Get the results
    exec_result, msg = nutter_exec(cls)
    results.update(exec_result)
    print(msg)
  
  
  is_main = get_widget_is_main(False)
      
  def assert_all():
    if is_main():
      assert_test_results(results)
    
  return results, run_test_fixture, assert_all


def get_widget_is_main(default_value:bool = True) -> bool:
  dbutils.widgets.dropdown('is_main', str(default_value), ['True', 'False'], 'Main notebook?')  # Is this the main notebook in the run?
  options = {'True': True, 'False': False}
  
  def is_main():
    return options.get(dbutils.widgets.get('is_main'))
  
  return is_main


def nutter_exec(cls: type[NutterFixture]) -> tuple[dict[str, TestExecResults], str]:
  fixture = cls()
  exec_result = fixture.execute_tests()
  return {cls.__name__: exec_result}, exec_result.to_string()


def assert_test_results(exec_results: dict[str, TestExecResults]):
  """Check if there are failing tests in a list of execution results. Can be used at the end of a notebook
  to alert user that at least one test failed.
  """
  failing_test_fixtures = [name for name, result in exec_results.items() if not result.test_results.passed()]
  num_failing_tests = sum(map(lambda result: result.test_results.num_failures, exec_results.values()))
  
  if num_failing_tests:
    raise Exception(f"There were {num_failing_tests} failed tests across the following test fixtures: {failing_test_fixtures}")