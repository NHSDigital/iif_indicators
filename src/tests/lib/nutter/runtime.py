# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

# nutter/runtime/testcase.py

import os
import time
import traceback


def get_testcase(test_name):

    tc = TestCase(test_name)

    return tc


class TestCase():
    ERROR_MESSAGE_ASSERTION_MISSING = """ TestCase does not contain an assertion function.
                                            Please pass a function to set_assertion """

    def __init__(self, test_name):
        self.test_name = test_name
        self.before = None
        self.__before_set = False
        self.run = None
        self.__run_set = False
        self.assertion = None
        self.after = None
        self.__after_set = False
        self.invalid_message = ""
        self.tags = []

    def set_before(self, before):
        self.before = before
        self.__before_set = True

    def set_run(self, run):
        self.run = run
        self.__run_set = True

    def set_assertion(self, assertion):
        self.assertion = assertion

    def set_after(self, after):
        self.after = after
        self.__after_set = True

    def execute_test(self):
        start_time = time.perf_counter()
        try:
            if hasattr(self.run, "tag"):
                if isinstance(self.run.tag, list):
                    self.tags.extend(self.run.tag)
                else:
                    self.tags.append(self.run.tag)
            if not self.is_valid():
                raise NoTestCasesFoundError(
                    "Both a run and an assertion are required for every test")
            if self.__before_set and self.before is not None:
                self.before()
            if self.__run_set:
                self.run()
            self.assertion()
            if self.__after_set and self.after is not None:
                self.after()

        except Exception as exc:
            return TestResult(self.test_name, False,
                              self.__get_elapsed_time(start_time), self.tags,
                              exc, traceback.format_exc())

        return TestResult(self.test_name, True,
                          self.__get_elapsed_time(start_time), self.tags, None)

    def is_valid(self):
        is_valid = True

        if self.assertion is None:
            self.__add_message_to_error(self.ERROR_MESSAGE_ASSERTION_MISSING)
            is_valid = False

        return is_valid

    def __get_elapsed_time(self, start_time):
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        return elapsed_time

    def __add_message_to_error(self, message):
        if self.invalid_message:
            self.invalid_message += os.linesep

        self.invalid_message += message

    def get_invalid_message(self):
        self.is_valid()

        return self.invalid_message


class NoTestCasesFoundError(Exception):
    pass

# COMMAND ----------

# nutter/runtime/fixtureloader.py


def get_fixture_loader():
    loader = FixtureLoader()
    return loader


class FixtureLoader():
    def __init__(self):
        self.__test_case_dictionary = {}
        pass

    def load_fixture(self, nutter_fixture):
        if nutter_fixture is None:
            raise ValueError("Must pass NutterFixture")

        all_attributes = dir(nutter_fixture)
        for attribute in all_attributes:
            is_test_method = self.__is_test_method(attribute)
            if is_test_method:
                test_full_name = attribute
                test_name = self.__get_test_name(attribute)
                func = getattr(nutter_fixture, test_full_name)
                if func is None:
                    continue

                if test_name == "before_all" or test_name == "after_all":
                    continue

                test_case = None
                if test_name in self.__test_case_dictionary:
                    test_case = self.__test_case_dictionary[test_name]

                if test_case is None:
                    test_case = TestCase(test_name)

                test_case = self.__set_method(test_case, test_full_name, func)

                self.__test_case_dictionary[test_name] = test_case

        return self.__test_case_dictionary

    def __is_test_method(self, attribute):
        if attribute.startswith("before_") or \
                attribute.startswith("run_") or \
                attribute.startswith("assertion_") or \
                attribute.startswith("after_"):
            return True
        return False

    def __set_method(self, case, name, func):
        if name.startswith("before_"):
            case.set_before(func)
            return case
        if name.startswith("run_"):
            case.set_run(func)
            return case
        if name.startswith("assertion_"):
            case.set_assertion(func)
            return case
        if name.startswith("after_"):
            case.set_after(func)
            return case

        return case

    def __get_test_name(self, full_name):
        if full_name == "before_all" or full_name == "after_all":
            return full_name

        name = self.__remove_prefix(full_name, "before_")
        name = self.__remove_prefix(name, "run_")
        name = self.__remove_prefix(name, "assertion_")
        name = self.__remove_prefix(name, "after_")

        return name

    def __remove_prefix(self, text, prefix):
        if text.startswith(prefix):
            return text[len(prefix):]
        return text

# COMMAND ----------

# nutter/runtime/nutterfixture.py

import logging
from abc import ABCMeta
from collections import OrderedDict


def tag(the_tag):
    def tag_decorator(function):
        if not isinstance(the_tag, list) and not isinstance(the_tag, str):
            raise ValueError("the_tag must be a string or a list")
        if not str.startswith(function.__name__, "run_"):
            raise ValueError("a tag may only decorate a run_ method")

        function.tag = the_tag
        return function
    return tag_decorator


class NutterFixture(object):
    """
    """

    __metaclass__ = ABCMeta

    def __init__(self):
        self.data_loader = FixtureLoader()
        self.test_results = TestResults()
        self._logger = logging.getLogger('NutterRunner')

    def execute_tests(self):
        self.__load_fixture()

        if len(self.__test_case_dict) > 0 and self.__has_method("before_all"):
            logging.debug('Running before_all()')
            self.before_all()

        for key, value in self.__test_case_dict.items():
            logging.debug('Running test: {}'.format(key))
            test_result = value.execute_test()
            logging.debug('Completed running test: {}'.format(key))
            self.test_results.append(test_result)

        if len(self.__test_case_dict) > 0 and self.__has_method("after_all"):
            logging.debug('Running after_all()')
            self.after_all()

        return TestExecResults(self.test_results)

    def __load_fixture(self):
        if hasattr(self, 'data_loader') is False:
            msg = """ If you have an __init__ method in your test class.
                      Make sure you make a call to initialize the parent class.
                      For example: super().__init__() """
            raise InitializationException(msg)

        test_case_dict = self.data_loader.load_fixture(self)
        if test_case_dict is None:
            logging.fatal("Invalid Test Fixture")
            raise InvalidTestFixtureException("Invalid Test Fixture")
        self.__test_case_dict = OrderedDict(sorted(test_case_dict.items(), key=lambda t: t[0]))

        logging.debug("Found {} test cases".format(len(test_case_dict)))
        for key, value in self.__test_case_dict.items():
            logging.debug('Test Case: {}'.format(key))

    def __has_method(self, method_name):
        method = getattr(self, method_name, None)
        if callable(method):
            return True
        return False


class InvalidTestFixtureException(Exception):
    def __init__(self, message):
        super().__init__(message)

class InitializationException(Exception):
    def __init__(self, message):
        super().__init__(message)