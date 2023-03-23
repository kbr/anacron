"""
test_engine.py

tests for the engine and the worker.

The configuration sets a flag `is_active`. If this flag is True the
engine should start a monitor-thread. The monitor-thread then starts the
worker process. On terminating the engine sets a threading event to
terminate the monitor-thread and the monitor thread should shut down the
worker process.
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


class TestEngine(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)

    def tearDown(self):
        # clean up if tests don't run through
        sf = configuration.configuration.semaphore_file
        if sf.exists():
            sf.unlink()  # nissing_ok parameter needs Python >= 3.8
        pathlib.Path(self.interface.db_name).unlink()

    def test_inactive(self):
        """
        If the `configuration.is_active` flag is not set, the engine
        should not start the monitor-thread.
        """
        # start should return None
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is False

    def test_no_start_on_semaphore(self):
        """
        Don't start more than one monitor-process. A started monitor
        will set a semaphore-file. If this file is present, a new
        monitor will not start.
        """
        configuration.configuration.is_active = True
        sf = configuration.configuration.semaphore_file
        sf.touch()
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is False
        sf.unlink()
        configuration.configuration.is_active = False

    def test_start_subprocess(self):
        process = engine.start_subprocess()
        assert isinstance(process, subprocess.Popen) is True
        assert process.poll() is None
        process.terminate()
        time.sleep(0.02)  # give process some time to terminate
        assert process.poll() is not None


