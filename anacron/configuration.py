"""
configuration.py

Module to get the configuration from the standard (default) settings or adapt them from web-frameworks (currently django).
"""

import pathlib

try:
    from django.conf import settings
except ImportError:
    DJANGO_IS_INSTALLED = False
else:
    DJANGO_IS_INSTALLED = True

DB_FILE_NAME = "anacron.db"
SEMAPHORE_FILE_NAME = "anacron.flag"
MONITOR_IDLE_TIME = 1  # seconds
WORKER_IDLE_TIME = 1  # seconds


class Configuration:
    """
    Configuration as instance attributes
    for better testing.
    """
    def __init__(self, db_filename=DB_FILE_NAME):
        self.db_path = pathlib.Path(".").parent
        self.db_filename = db_filename
        self.monitor_idle_time = MONITOR_IDLE_TIME
        self.worker_idle_time = WORKER_IDLE_TIME
        self.worker_allowed = True  # can be set to False for testing
        self.is_active = False
        if DJANGO_IS_INSTALLED:
            self.is_active = not settings.DEBUG

    @property
    def db_file(self):
        return self.db_path / self.db_filename

    @property
    def semaphore_file(self):
        return self.db_path / SEMAPHORE_FILE_NAME

    @property
    def cwd(self):
        if DJANGO_IS_INSTALLED:
            return settings.BASE_DIR
        return pathlib.Path.cwd()


configuration = Configuration()
