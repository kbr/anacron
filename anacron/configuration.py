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


class Configuration:
    """
    Configuration as instance attributes
    for better testing.
    """
    def __init__(self, db_filename=DB_FILE_NAME):
        self.db_path = pathlib.Path(".").parent
        self.db_filename = db_filename
        self.is_active = False
        if DJANGO_IS_INSTALLED:
            self.is_active = not settings.DEBUG

    @property
    def db_file(self):
        return self.db_path / self.db_filename

    @property
    def semaphore(self):
        return self.db_path / SEMAPHORE_FILE_NAME


configuration = Configuration()
