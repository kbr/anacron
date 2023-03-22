"""
test_engine.py

tests for the engine and the worker.
"""
import pathlib
import subprocess
import sys
import time
import unittest

from anacron import configuration
from anacron import engine
from anacron import sql_interface
from anacron import worker


TEST_DB_NAME = "test.db"


class TestWorkerStartStop(unittest.TestCase):

    def setUp(self):
        self.cmd = [sys.executable, worker.__file__]
        self.cwd = configuration.configuration.cwd

    def test_start_and_stop_workerprocess(self):
        process = subprocess.Popen(self.cmd, cwd=self.cwd)
        assert process.poll() is None  # subprocess runs
        process.terminate()
        time.sleep(0.1)
        assert process.poll() is not None


class TestWorkerStartStopViaEngine(unittest.TestCase):

    def test_start_worker_via_engine(self):
        configuration.configuration.is_active = True
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is True
        assert engine_.monitor is not None
        engine_.stop()
        configuration.configuration.is_active = False


def add_test(a, b):
    return a + b


class TestWorker(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)
        self.worker = worker.Worker()

    def tearDown(self):
        pathlib.Path(self.interface.db_name).unlink()

    def test_process_task(self):
        self.interface.register_callable(add_test, args=(2, 40))
        task = self.interface.get_callables()[0]
        self.worker.process_task(task)
        assert self.worker.result == 42

