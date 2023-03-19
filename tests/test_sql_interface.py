"""
test_sql_interface.py

testcode for sql-actions
"""

import datetime
import pathlib
import time
import unittest
from anacron import sql_interface


TEST_DB_NAME = "test.db"


def test_callable(*args, **kwargs):
    return args, kwargs

def test_adder(a, b):
    return a + b


class TestSQLInterface(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink()

    def test_storage(self):
        entries = self.interface.get_callables()
        self.assertFalse(list(entries))
        self.interface.register_callable(test_callable)
        entries = list(self.interface.get_callables())
        assert len(entries) == 1

    def test_entry_signature(self):
        self.interface.register_callable(test_callable)
        entries = list(self.interface.get_callables())
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

    def test_schedules(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(milliseconds=1)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get one callable at due
        entries = list(self.interface.get_callables())
        assert len(entries) == 1
        # wait and test to get two callbles on due
        time.sleep(0.001)
        entries = list(self.interface.get_callables())
        assert len(entries) == 2

    def test_delete(self):
        # register two callables, one with a schedule in the future
        schedule = datetime.datetime.now() + datetime.timedelta(milliseconds=1)
        self.interface.register_callable(test_adder, schedule=schedule)
        self.interface.register_callable(test_callable)
        # test to get the `test_callable` function on due
        # and delete it from the db
        entry = list(self.interface.get_callables())[0]
        assert entry["function_name"] == test_callable.__name__
        self.interface.delete_callable(entry)
        # wait and test to get the remaining single entry
        # and check whether it is the `test_adder` function
        time.sleep(0.001)
        entries = list(self.interface.get_callables())
        assert len(entries) == 1
        entry = entries[0]
        assert entry["function_name"] == test_adder.__name__





