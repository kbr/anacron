"""
test_sql_interface.py

testcode for sql-actions and for decorators
"""

import collections
import datetime
import pathlib
import time
import unittest
import uuid

from anacron import configuration
from anacron import decorators
from anacron import sql_interface
from anacron import worker


TEST_DB_NAME = configuration.configuration.anacron_path / "test.db"


def test_callable(*args, **kwargs):
    return args, kwargs

def test_adder(a, b):
    return a + b

def test_multiply(a, b):
    return a * b


class TestSQLInterface(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink()

    def test_storage(self):
        entries = self.interface.get_tasks_on_due()
        self.assertFalse(list(entries))
        self.interface.register_callable(test_callable)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def test_entry_signature(self):
        self.interface.register_callable(test_callable)
        entries = self.interface.get_tasks_on_due()
        obj = entries[0]
        assert isinstance(obj, sql_interface.HybridNamespace) is True
        assert obj["function_module"] == test_callable.__module__
        assert obj["function_name"] == test_callable.__name__

    def test_arguments(self):
        args = ["pi", 3.141]
        kwargs = {"answer": 41, 10: "ten"}
        crontab = "* 1 * * *"
        self.interface.register_callable(
            test_callable, crontab=crontab, args=args, kwargs=kwargs
        )
        entries = list(self.interface.get_tasks_on_due())
        obj = entries[0]
        assert obj["crontab"] == crontab
        assert obj["args"] == args
        assert obj["kwargs"] == kwargs

    def test_schedules_get_one_of_two(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1

    def test_schedules_get_two_of_two(self):
        # register two callables, both scheduled in the present or past
        schedule = datetime.datetime.now() - datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 2

    def test_delete(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(milliseconds=1)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get the `test_callable` function on due
        # and delete it from the db
        entry = self.interface.get_tasks_on_due()[0]
        assert entry["function_name"] == test_callable.__name__
        self.interface.delete_callable(entry)
        # wait and test to get the remaining single entry
        # and check whether it is the `test_adder` function
        time.sleep(0.001)
        entries = self.interface.get_tasks_on_due()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["function_name"] == test_adder.__name__

    def test_get_task_by_signature(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # find a nonexistent callable should return an empty generator
        entries = self.interface.get_tasks_by_signature(test_multiply)
        assert len(entries) == 0
        # find a callable scheduled for the future:
        entries = self.interface.get_tasks_by_signature(test_adder)
        assert len(entries) == 1

    def test_get_tasks_by_signature(self):
        # it is allowed to register the same callables multiple times.
        # regardless of the schedule `get_tasks_by_signature()` should return
        # all entries.
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_adder)
        entries = list(self.interface.get_tasks_by_signature(test_adder))
        assert len(entries) == 2

    def test_update_schedule(self):
        # entries like cronjobs should not get deleted from the tasks
        # but updated with the next schedule
        schedule = datetime.datetime.now()
        next_schedule = schedule + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        entry = self.interface.get_tasks_by_signature(test_adder)[0]
        assert entry["schedule"] == schedule
        self.interface.update_schedule(entry["rowid"], next_schedule)
        entry = self.interface.get_tasks_by_signature(test_adder)[0]
        assert entry["schedule"] == next_schedule

    def test_result_by_uuid_no_result(self):
        # result should be None if no entry found
        uuid_ = uuid.uuid4().hex
        result = self.interface.get_result_by_uuid(uuid_)
        assert result is None

    def test_result_by_uuid_result_registered(self):
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        result = self.interface.get_result_by_uuid(uuid_)
        # return a TaskResult instance:
        assert result is not None
        assert result.is_waiting is True

    def test_update_result_no_error(self):
        answer = 42
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        self.interface.update_result(uuid_, result=answer)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.is_ready is True
        assert result.function_result == answer
        # test shortcut for function_result:
        assert result.result == answer

    def test_update_result_with_error(self):
        message = "ValueError: more text here ..."
        uuid_ = uuid.uuid4().hex
        self.interface.register_result(test_adder, uuid=uuid_)
        self.interface.update_result(uuid_, error_message=message)
        result = self.interface.get_result_by_uuid(uuid_)
        assert result.has_error is True

    def test_count_tasks(self):
        # register three callables, two as cronjobs.
        # check whether there are three entries in the database
        self.interface.register_callable(test_callable)
        self.interface.register_callable(test_adder, crontab="* * * * *")
        self.interface.register_callable(test_multiply, crontab="* * * * *")
        entries = self.interface.count_tasks()
        assert entries == 3

    def test_delete_cronjobs(self):
        # register three callables, two as cronjobs.
        # delete the cronjobs and check that a single item in left
        # in the database.
        self.interface.register_callable(test_callable)
        self.interface.register_callable(test_adder, crontab="* * * * *")
        self.interface.register_callable(test_multiply, crontab="* * * * *")
        self.interface.delete_cronjobs()
        entries = self.interface.count_tasks()
        assert entries == 1
        # remaining entry should be the test_callable
        entry = self.interface.get_tasks_on_due()[0]
        assert entry.function_module == test_callable.__module__
        assert entry.function_name == test_callable.__name__




# decorator testing includes database access.
# for easier testing decorator tests are included here.

def cron_function():
    pass


class TestCronDecorator(unittest.TestCase):

    def setUp(self):
        self.orig_interface = decorators.interface
        decorators.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_interface

#     def test_cron_no_arguments_inactive(self):
#         # the database should have no entry with the default crontab
#         # if configuration is not active
#         wrapper = decorators.cron()
#         func = wrapper(cron_function)
#         assert func == cron_function
#         entries = list(decorators.interface.get_tasks_by_signature(cron_function))
#         assert len(entries) == 0

    def test_cron_no_arguments_active(self):
        # the database should have one entry with the default crontab
        # if configuration is active
#         configuration.configuration.is_active = True
        wrapper = decorators.cron()
        func = wrapper(cron_function)
        assert func == cron_function
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == decorators.DEFAULT_CRONTAB
#         configuration.configuration.is_active = False

    def test_suppress_identic_cronjobs(self):
        # register multiple cronjobs of a single callable.
        # then add the cronjob again by means of the decorator.
        # the db then should hold just a single entry deleting
        # the other ones.
        # should not happen:
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 2
        # now add the same function with the cron decorator:
        crontab = "10 2 1 * *"
        configuration.configuration.is_active = True
        wrapper = decorators.cron(crontab=crontab)
        func = wrapper(cron_function)
        # just a single entry should no be in the database
        # (the one added by the decorator):
        entries = list(decorators.interface.get_tasks_by_signature(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == crontab
        configuration.configuration.is_active = False


def delegate_function():
    return 42


class TestNewDelegateDecorator(unittest.TestCase):

    def setUp(self):
        self.orig_decorator_interface = decorators.interface
        self.orig_worker_interface = worker.interface
        worker.interface = decorators.interface =\
            sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_decorator_interface
        worker.interface = self.orig_worker_interface

    def test_inactive(self):
        # does not return the original function but calls
        # the original function indirect instead of registering
        # the task in the db.
        wrapper = decorators.delegate(delegate_function)
        assert wrapper() == 42
        entries = decorators.interface.get_tasks_by_signature(delegate_function)
        assert len(entries) == 0
        # call decorator with parameter
        call = decorators.delegate(provide_result=False)
        wrapper = call(delegate_function)
        entries = decorators.interface.get_tasks_by_signature(delegate_function)
        assert len(entries) == 0
        assert wrapper() == 42

    def test_active_no_argument(self):
        configuration.configuration.is_active = True
        wrapper = decorators.delegate(delegate_function)
        assert wrapper() != 42
        entries = decorators.interface.get_tasks_by_signature(delegate_function)
        assert len(entries) == 1
        configuration.configuration.is_active = False

    def test_active_with_argument(self):
        configuration.configuration.is_active = True
        call = decorators.delegate(provide_result=False)
        wrapper = call(delegate_function)
        assert wrapper() is None  # provide_result is False
        entries = decorators.interface.get_tasks_by_signature(delegate_function)
        assert len(entries) == 1
        configuration.configuration.is_active = False

    def test_active_with_argument_get_uuid(self):
        configuration.configuration.is_active = True
        call = decorators.delegate(provide_result=True)
        wrapper = call(delegate_function)
        assert isinstance(wrapper(), str) is True  # provide_result is True
        entries = decorators.interface.get_tasks_by_signature(delegate_function)
        assert len(entries) == 1
        configuration.configuration.is_active = False

    def test_active_with_argument_and_get_result(self):
        """
        Test story:

        1. wrap a function with the delegate decorator and provide
           arguments. This should return a uuid.
        2. Check for task entry in db.
        3. Then call `Worker.handle_tasks` what should return True.
        4. Then call `interface.get_result_by_uuid` which should return
           a TaskResult instance with the correct result.

        """
        # 1: wrap function and call the wrapper with arguments
        configuration.configuration.is_active = True
        call = decorators.delegate(provide_result=True)
        wrapper = call(test_adder)
        uuid_ = wrapper(40, 2)
        assert uuid_ is not None

        # 2: a single entry is now now in both tables:
        time.sleep(0.001)  # have some patience with the db.
        task_entries = decorators.interface.get_tasks_by_signature(test_adder)
        assert len(task_entries) == 1
        result = decorators.interface.get_result_by_uuid(uuid_)
        assert result is not None
        assert result.is_waiting is True

        # 3: let the worker handle the task:
        worker_ = worker.Worker()  # instanciate but don't start the worker
        # return True if at least one task has handled:
        return_value = worker_.handle_tasks()
        assert return_value is True
        time.sleep(0.001)  # have some patience with the db.
        # after handling the task should be removed from the db:
        task_entries = decorators.interface.get_tasks_by_signature(test_adder)
        assert len(task_entries) == 0

        # 4: check whether the worker has updated the result entry in the db:
        result = decorators.interface.get_result_by_uuid(uuid_)
        assert result.is_ready is True
        assert result.result == 42  # 40 + 2
        # shut down:
        configuration.configuration.is_active = False


class TestHybridNamespace(unittest.TestCase):

    def setUp(self):
        self.data = {"pi": 3.141, "answer": 42}
        self.attr_dict = sql_interface.HybridNamespace(self.data)

    def test_dict_access(self):
        self.attr_dict["one"] = 1
        assert self.attr_dict["one"] == 1

    def test_attribute_access(self):
        self.attr_dict.two = 2
        assert self.attr_dict.two == 2

    def test_mixed_access(self):
        self.attr_dict.three = 3
        assert self.attr_dict["three"] == 3
        self.attr_dict["four"] = 4
        assert self.attr_dict.four == 4

    def test_get_init_values(self):
        assert self.attr_dict["pi"] == self.data["pi"]
        assert self.attr_dict.pi == self.data["pi"]
        assert self.attr_dict["answer"] == self.data["answer"]
        assert self.attr_dict.answer == self.data["answer"]
