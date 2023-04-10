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
import time
import unittest

from anacron import configuration
from anacron import engine
from anacron import sql_interface


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

    def test_start_subprocess(self):
        process = engine.start_subprocess()
        assert isinstance(process, subprocess.Popen) is True
        assert process.poll() is None
        process.terminate()
        time.sleep(0.02)  # give process some time to terminate
        assert process.poll() is not None
