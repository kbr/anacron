"""
test_sql_interface.py

testcode for sql-actions
"""

import datetime
import pathlib
import time
import unittest

from anacron import configuration
from anacron import decorators
from anacron import sql_interface


TEST_DB_NAME = "test.db"


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
        entries = self.interface.get_callables()
        self.assertFalse(list(entries))
        self.interface.register_callable(test_callable)
        entries = self.interface.get_callables()
        assert len(entries) == 1

    def test_entry_signature(self):
        self.interface.register_callable(test_callable)
        entries = self.interface.get_callables()
        obj = entries[0]
        assert type(obj) is dict
        assert obj["function_module"] == test_callable.__module__
        assert obj["function_name"] == test_callable.__name__

    def test_arguments(self):
        args = ["pi", 3.141]
        kwargs = {"answer": 41, 10: "ten"}
        crontab = "* 1 * * *"
        self.interface.register_callable(
            test_callable, crontab=crontab, args=args, kwargs=kwargs
        )
        entries = list(self.interface.get_callables())
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
        entries = self.interface.get_callables()
        assert len(entries) == 1

    def test_schedules_get_two_of_two(self):
        # register two callables, both scheduled in the present or past
        schedule = datetime.datetime.now() - datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = self.interface.get_callables()
        assert len(entries) == 2

    def test_delete(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(milliseconds=1)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get the `test_callable` function on due
        # and delete it from the db
        entry = self.interface.get_callables()[0]
        assert entry["function_name"] == test_callable.__name__
        self.interface.delete_callable(entry)
        # wait and test to get the remaining single entry
        # and check whether it is the `test_adder` function
        time.sleep(0.001)
        entries = self.interface.get_callables()
        assert len(entries) == 1
        entry = entries[0]
        assert entry["function_name"] == test_adder.__name__

    def test_find_callable(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # find a nonexistent callable should return an empty generator
        entries = self.interface.find_callables(test_multiply)
        assert len(entries) == 0
        # find a callable scheduled for the future:
        entries = self.interface.find_callables(test_adder)
        assert len(entries) == 1

    def test_find_callables(self):
        # it is allowed to register the same callables multiple times.
        # regardless of the schedule `find_callables()` should return
        # all entries.
        schedule = datetime.datetime.now() + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_adder)
        entries = list(self.interface.find_callables(test_adder))
        assert len(entries) == 2

    def test_update_schedule(self):
        # entries like cronjobs should not get deleted from the tasks
        # but updated with the next schedule
        schedule = datetime.datetime.now()
        next_schedule = schedule + datetime.timedelta(seconds=10)
        self.interface.register_callable(test_adder, schedule=schedule)
        entry = self.interface.find_callables(test_adder)[0]
        assert entry["schedule"] == schedule
        self.interface.update_schedule(entry["rowid"], next_schedule)
        entry = self.interface.find_callables(test_adder)[0]
        assert entry["schedule"] == next_schedule




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

    def test_cron_no_arguments_inactive(self):
        # the database should have no entry with the default crontab
        # if configuration is not active
        wrapper = decorators.cron()
        func = wrapper(cron_function)
        assert func == cron_function
        entries = list(decorators.interface.find_callables(cron_function))
        assert len(entries) == 0

    def test_cron_no_arguments_active(self):
        # the database should have one entry with the default crontab
        # if configuration is active
        configuration.configuration.is_active = True
        wrapper = decorators.cron()
        func = wrapper(cron_function)
        assert func == cron_function
        entries = list(decorators.interface.find_callables(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == decorators.DEFAULT_CRONTAB
        configuration.configuration.is_active = False

    def test_suppress_identic_cronjobs(self):
        # register multiple cronjobs of a single callable.
        # then add the cronjob again by means of the decorator.
        # the db then should hold just a single entry deleting
        # the other ones.
        # should not happen:
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        decorators.interface.register_callable(cron_function, crontab=decorators.DEFAULT_CRONTAB)
        entries = list(decorators.interface.find_callables(cron_function))
        assert len(entries) == 2
        # now add the same function with the cron decorator:
        crontab = "10 2 1 * *"
        configuration.configuration.is_active = True
        wrapper = decorators.cron(crontab=crontab)
        func = wrapper(cron_function)
        # just a single entry should no be in the database
        # (the one added by the decorator):
        entries = list(decorators.interface.find_callables(cron_function))
        assert len(entries) == 1
        entry = entries[0]
        assert entry["crontab"] == crontab
        configuration.configuration.is_active = False


def delegate_function():
    pass


class TestDelegateDecorator(unittest.TestCase):

    def setUp(self):
        self.orig_interface = decorators.interface
        decorators.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(decorators.interface.db_name).unlink()
        decorators.interface = self.orig_interface

    def test_inactive(self):
        wrapper = decorators.delegate(delegate_function)
        assert wrapper is delegate_function
        entries = list(decorators.interface.find_callables(delegate_function))
        assert len(entries) == 0

    def test_active(self):
        configuration.configuration.is_active = True
        wrapper = decorators.delegate(delegate_function)
        assert wrapper is not delegate_function
        entries = list(decorators.interface.find_callables(delegate_function))
        assert len(entries) == 0
        # call to wrapper store task in db:
        wrapper()
        entries = list(decorators.interface.find_callables(delegate_function))
        assert len(entries) == 1
        configuration.configuration.is_active = False

