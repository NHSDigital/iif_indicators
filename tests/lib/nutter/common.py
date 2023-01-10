# Databricks notebook source
# nutter/common/apiclientresults

from abc import ABCMeta
import logging


class ExecuteNotebookResult(object):
    def __init__(self, life_cycle_state, notebook_path,
                 notebook_result, notebook_run_page_url):
        self.task_result_state = life_cycle_state
        self.notebook_path = notebook_path
        self.notebook_result = notebook_result
        self.notebook_run_page_url = notebook_run_page_url

    @classmethod
    def from_job_output(cls, job_output):
        life_cycle_state = utils.recursive_find(
            job_output, ['metadata', 'state', 'life_cycle_state'])
        notebook_path = utils.recursive_find(
            job_output, ['metadata', 'task', 'notebook_task', 'notebook_path'])
        notebook_run_page_url = utils.recursive_find(
            job_output, ['metadata', 'run_page_url'])
        notebook_result = NotebookOutputResult.from_job_output(job_output)

        return cls(life_cycle_state, notebook_path,
                   notebook_result, notebook_run_page_url)

    @property
    def is_error(self):
        # The assumption is that the task is an terminal state
        # Success state must be TERMINATED all the others are considered failures
        return self.task_result_state != 'TERMINATED'

    @property
    def is_any_error(self):
        if self.is_error:
            return True
        if self.notebook_result.is_error:
            return True
        if self.notebook_result.nutter_test_results is None:
            return True

        for test_case in self.notebook_result.nutter_test_results.results:
            if not test_case.passed:
                return True
        return False

class NotebookOutputResult(object):
    def __init__(self, result_state, exit_output, nutter_test_results):
        self.result_state = result_state
        self.exit_output = exit_output
        self.nutter_test_results = nutter_test_results

    @classmethod
    def from_job_output(cls, job_output):
        exit_output = ''
        nutter_test_results = ''
        notebook_result_state = ''
        if 'error' in job_output:
            exit_output = job_output['error']

        if 'notebook_output' in job_output:
            notebook_result_state = utils.recursive_find(
                job_output, ['metadata', 'state', 'result_state'])

            if 'result' in job_output['notebook_output']:
                exit_output = job_output['notebook_output']['result']
                nutter_test_results = cls._get_nutter_test_results(exit_output)

        return cls(notebook_result_state, exit_output, nutter_test_results)

    @property
    def is_error(self):
        # https://docs.azuredatabricks.net/dev-tools/api/latest/jobs.html#jobsrunresultstate
        return self.result_state != 'SUCCESS' and not self.is_run_from_notebook

    @property
    def is_run_from_notebook(self):
        # https://docs.azuredatabricks.net/dev-tools/api/latest/jobs.html#jobsrunresultstate
        return self.result_state == 'N/A'

    @classmethod
    def _get_nutter_test_results(cls, exit_output):
        nutter_test_results = cls._to_nutter_test_results(exit_output)
        if nutter_test_results is None:
            return None
        return nutter_test_results

    @classmethod
    def _to_nutter_test_results(cls, exit_output):
        if not exit_output:
            return None
        try:
            return TestResults().deserialize(exit_output)
        except Exception as ex:
            error = 'error while creating result from {}. Error: {}'.format(
                ex, exit_output)
            logging.debug(error)
            return None


class WorkspacePath(object):
    def __init__(self, notebooks, directories):
        self.notebooks = notebooks
        self.directories = directories
        self.test_notebooks = self._set_test_notebooks()

    @classmethod
    def from_api_response(cls, objects):
        notebooks = cls._set_notebooks(objects)
        directories = cls._set_directories(objects)
        return cls(notebooks, directories)

    @classmethod
    def _set_notebooks(cls, objects):
        if 'objects' not in objects:
            return []
        return [NotebookObject(object['path']) for object in objects['objects']
                if object['object_type'] == 'NOTEBOOK']

    @classmethod
    def _set_directories(cls, objects):
        if 'objects' not in objects:
            return []
        return [Directory(object['path']) for object in objects['objects']
                if object['object_type'] == 'DIRECTORY']

    def _set_test_notebooks(self):
        return [notebook for notebook in self.notebooks
                if notebook.is_test_notebook]


class WorkspaceObject():
    __metaclass__ = ABCMeta

    def __init__(self, path):
        self.path = path


class NotebookObject(WorkspaceObject):
    def __init__(self, path):
        self.name = self._get_notebook_name_from_path(path)
        super().__init__(path)

    def _get_notebook_name_from_path(self, path):
        segments = path.split('/')
        if len(segments) == 0:
            raise ValueError('Invalid path. Path must start /')
        name = segments[len(segments)-1]
        return name

    @property
    def is_test_notebook(self):
        return utils.contains_test_prefix_or_surfix(self.name)


class Directory(WorkspaceObject):
    def __init__(self, path):
        super().__init__(path)

# COMMAND ----------

# nutter/common/pickserializable.py

from abc import abstractmethod, ABCMeta


class PickleSerializable():
    __metaclass__ = ABCMeta

    @abstractmethod
    def serialize(self):
        pass

    @abstractmethod
    def deserialize(self):
        pass

# COMMAND ----------

# nutter/common/resultsview.py

import logging
from abc import abstractmethod, ABCMeta


def get_run_results_views(exec_results):
    if not isinstance(exec_results, list):
        raise ValueError("Expected a List")

    results_view = RunCommandResultsView()
    for exec_result in exec_results:
        results_view.add_exec_result(exec_result)

    return results_view


# Not implemented due to difficulty importing from nutter.common.api
# def get_list_results_view(list_results):
#     return ListCommandResultsView(list_results)


def print_results_view(results_view):
    if not isinstance(results_view, ResultsView):
        raise ValueError("Expected ResultsView")

    results_view.print()

    print("Total: {} \n".format(results_view.total))


class ResultsView():
    __metaclass__ = ABCMeta

    def print(self):
        print(self.get_view())

    @abstractmethod
    def get_view(self):
        pass

    @abstractmethod
    def total(self):
        pass


# Not implemented due to difficulty importing from nutter.common.api
# class ListCommandResultsView(ResultsView):
#     def __init__(self, listresults):
#         if not isinstance(listresults, list):
#             raise ValueError("Expected a a list of TestNotebook()")
#         self.list_results = [ListCommandResultView.from_test_notebook(test_notebook)
#                              for test_notebook in listresults]

#         super().__init__()

#     def get_view(self):
#         writer = StringWriter()
#         writer.write_line('{}'.format('\nTests Found'))
#         writer.write_line('-' * 55)
#         for list_result in self.list_results:
#             writer.write(list_result.get_view())

#         writer.write_line('-' * 55)

#         return writer.to_string()

#     @property
#     def total(self):
#         return len(self.list_results)


# Not implemented due to difficulty importing from nutter.common.api
# class ListCommandResultView(ResultsView):
#     def __init__(self, name, path):
#         self.name = name
#         self.path = path
#         super().__init__()

#     @classmethod
#     def from_test_notebook(cls, test_notebook):
#         if not isinstance(test_notebook, TestNotebook):
#             raise ValueError('Expected an instance of TestNotebook')
#         return cls(test_notebook.name, test_notebook.path)

#     def get_view(self):
#         return "Name:\t{}\nPath:\t{}\n\n".format(self.name, self.path)

#     @property
#     def total(self):
#         return 1


class RunCommandResultsView(ResultsView):
    def __init__(self):
        self.run_results = []
        super().__init__()

    def add_exec_result(self, result):
        if not isinstance(result, ExecuteNotebookResult):
            raise ValueError("Expected ExecuteNotebookResult")
        self.run_results.append(RunCommandResultView(result))

    def get_view(self):
        writer = StringWriter()
        writer.write('\n')
        for run_result in self.run_results:
            writer.write(run_result.get_view())
            writer.write_line('=' * 60)

        return writer.to_string()

    @property
    def total(self):
        return len(self.run_results)


class RunCommandResultView(ResultsView):
    def __init__(self, result):

        if not isinstance(result, ExecuteNotebookResult):
            raise ValueError("Expected ExecuteNotebookResult")

        self.notebook_path = result.notebook_path
        self.task_result_state = result.task_result_state
        self.notebook_result_state = result.notebook_result.result_state
        self.notebook_run_page_url = result.notebook_run_page_url

        self.raw_notebook_output = result.notebook_result.exit_output
        t_results = self._get_test_results(result)
        self.test_cases_views = []
        if t_results is not None:
            for t_result in t_results.results:
                self.test_cases_views.append(TestCaseResultView(t_result))

        super().__init__()

    def _get_test_results(self, result):
        if result.notebook_result.is_run_from_notebook:
            return result.notebook_result.nutter_test_results

        return self.__to_testresults(result.notebook_result.exit_output)

    def get_view(self):
        sw = StringWriter()
        sw.write_line("Notebook: {} - Lifecycle State: {}, Result: {}".format(
            self.notebook_path, self.task_result_state, self.notebook_result_state))
        sw.write_line('Run Page URL: {}'.format(self.notebook_run_page_url))

        sw.write_line("=" * 60)

        if len(self.test_cases_views) == 0:
            sw.write_line("No test cases were returned.")
            sw.write_line("Notebook output: {}".format(
                self.raw_notebook_output))
            sw.write_line("=" * 60)
            return sw.to_string()

        if len(self.failing_tests) > 0:
            sw.write_line("FAILING TESTS")
            sw.write_line("-" * 60)

            for tc_view in self.failing_tests:
                sw.write(tc_view.get_view())

            sw.write_line("")
            sw.write_line("")

        if len(self.passing_tests) > 0:
            sw.write_line("PASSING TESTS")
            sw.write_line("-" * 60)

            for tc_view in self.passing_tests:
                sw.write(tc_view.get_view())

            sw.write_line("")
            sw.write_line("")

        return sw.to_string()

    def __to_testresults(self, exit_output):
        if not exit_output:
            return None
        try:
            return TestResults().deserialize(exit_output)
        except Exception as ex:
            error = 'error while creating result from {}. Error: {}'.format(
                ex, exit_output)
            logging.debug(error)
            return None

    @property
    def total(self):
        return len(self.test_cases_views)

    @property
    def passing_tests(self):
        return list(filter(lambda x: x.passed, self.test_cases_views))

    @property
    def failing_tests(self):
        return list(filter(lambda x: not x.passed, self.test_cases_views))


class TestCaseResultView(ResultsView):
    def __init__(self, nutter_test_results):

        if not isinstance(nutter_test_results, TestResult):
            raise ValueError("Expected TestResult")

        self.test_case = nutter_test_results.test_name
        self.passed = nutter_test_results.passed
        self.exception = nutter_test_results.exception
        self.stack_trace = nutter_test_results.stack_trace
        self.execution_time = nutter_test_results.execution_time

        super().__init__()

    def get_view(self):
        sw = StringWriter()

        time = '{} seconds'.format(self.execution_time)
        sw.write_line('{} ({})'.format(self.test_case, time))

        if (self.passed):
            return sw.to_string()

        sw.write_line("")
        sw.write_line(self.stack_trace)
        sw.write_line("")
        sw.write_line(self.exception.__class__.__name__ + ": " + str(self.exception))

        return sw.to_string()

    @property
    def total(self):
        return 1

# COMMAND ----------

# nutter/common/stringwriter.py


class StringWriter():
    def __init__(self):
        self.result = ""

    def write(self, string_to_append):
        self.result += string_to_append

    def write_line(self, string_to_append):
        self.write(string_to_append + '\n')

    def to_string(self):
        return self.result

# COMMAND ----------

# nutter/common/testexecresults.py


class TestExecResults():
    def __init__(self, test_results):
        if not isinstance(test_results, TestResults):
            raise TypeError("test_results must be of type TestResults")
        self.test_results = test_results
        self.runcommand_results_view = RunCommandResultsView()

    def to_string(self):
        notebook_path = ""
        notebook_result = self.get_ExecuteNotebookResult(
            notebook_path, self.test_results)
        self.runcommand_results_view.add_exec_result(notebook_result)
        view = self.runcommand_results_view.get_view()
        return view

    def exit(self, dbutils):
        dbutils.notebook.exit(self.test_results.serialize())

    def get_ExecuteNotebookResult(self, notebook_path, test_results):
        notebook_result = NotebookOutputResult(
            'N/A', None, test_results)

        return ExecuteNotebookResult('N/A', 'N/A', notebook_result, 'N/A')

# COMMAND ----------

# nutter/common/testresult.py

import pickle
import base64


def get_test_results():
    return TestResults()

class TestResults(PickleSerializable):
    def __init__(self):
        self.results = []
        self.test_cases = 0
        self.num_failures = 0
        self.total_execution_time = 0

    def append(self, testresult):
        if not isinstance(testresult, TestResult):
            raise TypeError("Can only append TestResult to TestResults")

        self.results.append(testresult)
        self.test_cases = self.test_cases + 1
        if (not testresult.passed):
            self.num_failures = self.num_failures + 1

        total_execution_time = self.total_execution_time + testresult.execution_time
        self.total_execution_time = total_execution_time

    def serialize(self):
        bin_data = pickle.dumps(self)
        return str(base64.encodebytes(bin_data), "utf-8")

    def deserialize(self, pickle_string):
        bin_str = pickle_string.encode("utf-8")
        decoded_bin_data = base64.decodebytes(bin_str)
        return pickle.loads(decoded_bin_data)

    def passed(self):
        for item in self.results:
            if not item.passed:
                return False
        return True

    def __eq__(self, other):
        if not isinstance(self, other.__class__):
            return False
        if len(self.results) != len(other.results):
            return False
        for item in other.results:
            if not self.__item_in_list_equalto(item):
                return False

        return True

    def __item_in_list_equalto(self, expected_item):
        for item in self.results:
            if (item == expected_item):
                return True

        return False

class TestResult:
    def __init__(self, test_name, passed,
                 execution_time, tags, exception=None, stack_trace=""):

        if not isinstance(tags, list):
            raise ValueError("tags must be a list")
        self.passed = passed
        self.exception = exception
        self.stack_trace = stack_trace
        self.test_name = test_name
        self.execution_time = execution_time
        self.tags = tags

    def __eq__(self, other):
        if isinstance(self, other.__class__):
            return self.test_name == other.test_name \
                and self.passed == other.passed \
                and type(self.exception) == type(other.exception) \
                and str(self.exception) == str(other.exception)

        return False

# COMMAND ----------

# nutter/common/utils.py


def recursive_find(dict_instance, keys):
    if not isinstance(keys, list):
        raise ValueError("Expected list of keys")
    if not isinstance(dict_instance, dict):
        return None
    if len(keys) == 0:
        return None
    key = keys[0]
    value = dict_instance.get(key, None)
    if value is None:
        return None
    if len(keys) == 1:
        return value
    return recursive_find(value, keys[1:len(keys)])

def contains_test_prefix_or_surfix(name):
    if name is None:
        return False

    return name.lower().startswith('test_') or name.lower().endswith('_test')