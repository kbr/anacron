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


TEST_DB_NAME = configuration.configuration.anacron_path / "test.db"


class TestEngine(unittest.TestCase):

    def setUp(self):
        self.interface = sql_interface.SQLiteInterface(db_name=TEST_DB_NAME)
        self._configuration_is_active = configuration.configuration.is_active

    def tearDown(self):
        # clean up if tests don't run through
        self._unset_semaphore()
        pathlib.Path(self.interface.db_name).unlink()
        configuration.configuration.is_active = self._configuration_is_active

    @staticmethod
    def _set_semaphore():
        configuration.configuration.semaphore_file.touch(exist_ok=True)

    @staticmethod
    def _unset_semaphore():
        sf = configuration.configuration.semaphore_file
        if sf.exists():
            sf.unlink()  # nissing_ok parameter needs Python >= 3.8

    def test_start_subprocess(self):
        process = engine.start_subprocess()
        assert isinstance(process, subprocess.Popen) is True
        assert process.poll() is None
        process.terminate()
        time.sleep(0.02)  # give process some time to terminate
        assert process.poll() is not None

    def test_start_allowed(self):
        # local shortcuts
        ee = engine.engine
        cc = configuration.configuration

        ee._start_allowed = None  # reset in tests to ignore caching
        cc.is_active = False
        assert ee.start_allowed is False
        ee._start_allowed = None
        cc.is_active = True
        self._set_semaphore()
        assert ee.start_allowed is False
        ee._start_allowed = None
        self._unset_semaphore()
        assert ee.start_allowed is True

