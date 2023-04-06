"""
configuration.py

Module to get the configuration from the standard (default) settings or
adapt them from web-frameworks (currently django).
"""

import datetime
import pathlib

try:
    from django.conf import settings
except ImportError:
    DJANGO_IS_INSTALLED = False
else:
    DJANGO_IS_INSTALLED = True

DB_FILE_NAME = "anacron.db"
SEMAPHORE_FILE_NAME = "anacron.flag"
MONITOR_IDLE_TIME = 1.0  # seconds
WORKER_IDLE_TIME = 1.0  # seconds
RESULT_TTL = 30  # storage time to live for results in minutes


class Configuration:
    """
    Configuration as instance attributes
    for better testing.
    """
    def __init__(self, db_filename=DB_FILE_NAME):
        self.db_path = self._get_db_path()
        self.db_filename = db_filename
        self.monitor_idle_time = MONITOR_IDLE_TIME
        self.worker_idle_time = WORKER_IDLE_TIME
        self.result_ttl = datetime.timedelta(minutes=RESULT_TTL)
        self.is_active = False
        if DJANGO_IS_INSTALLED:
            self.is_active = not settings.DEBUG

    def _get_db_path(self):
        """
        Return the directory as a Path object, where the anacron
        database (and also the test-database) get stored.
        """
        try:
            home_dir = pathlib.Path().home()
        except RuntimeError:
            # can't resolve homedir, take the present working
            # directory. Depending on the application .gitignore
            # should get extended with a ".anacron/*" entry.
            home_dir = self.cwd
        home_dir = home_dir / ".anacron"
        home_dir.mkdir(exist_ok=True)
        return home_dir

    @staticmethod
    def _get_filename_prefix():
        """
        Return a prefix for files in the .anacron directory to support
        multiple running applications.
        """
        cwd = pathlib.Path().cwd().as_posix()
        return cwd.replace("/", "_")

    @property
    def db_file(self):
        """
        Provides the path to the semaphore-file.
        """
        fname = f"{self._get_filename_prefix()}_{self.db_filename}"
        return self.db_path / fname

    @property
    def semaphore_file(self):
        """
        Provides the path to the semaphore-file.
        """
        fname = f"{self._get_filename_prefix()}_{SEMAPHORE_FILE_NAME}"
        return self.db_path / fname

    @property
    def cwd(self):
        """
        Provides the current working directory.
        In case of django this is the BASE_DIR of the project.
        """
        if DJANGO_IS_INSTALLED:
            return settings.BASE_DIR
        return pathlib.Path.cwd()


configuration = Configuration()
