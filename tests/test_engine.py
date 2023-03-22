"""
test_engine.py

tests for the engine.
"""
import time
import unittest

from anacron import configuration
from anacron import engine


TEST_DB_NAME = "test.db"


class TestEngine(unittest.TestCase):

    def tearDown(self):
        # clean up if tests don't run through
        sf = configuration.configuration.semaphore_file
        if sf.exists():
            sf.unlink()  # nissin_ok parameter needs Python >= 3.8

    def test_inactive(self):
        # start should return None
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is False

    def test_no_start_on_semaphore(self):
        configuration.configuration.is_active = True
        sf = configuration.configuration.semaphore_file
        sf.touch()
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is False
        sf.unlink()
        configuration.configuration.is_active = False

    def test_monitor_start_and_stop(self):
        configuration.configuration.is_active = True
        configuration.configuration.worker_allowed = False
        sf = configuration.configuration.semaphore_file
        assert sf.exists() is False
        # start engine successfull
        engine_ = engine.Engine()
        result = engine_.start()
        assert result is True
        assert engine_.monitor is not None
        assert engine_.monitor_thread.is_alive() is True
        # shut down
        engine_.stop()
        time.sleep(0.001)  # give thread some time to exit
        assert sf.exists() is False
        assert engine_.monitor_thread.is_alive() is False
        configuration.configuration.is_active = False
        configuration.configuration.worker_allowed = True
