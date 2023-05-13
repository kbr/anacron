"""
configuration.py

Module to get the configuration from the standard (default) settings or
adapt them from web-frameworks (currently django).
"""

import configparser
import datetime
import pathlib
import time

try:
    from django.conf import settings
except ImportError:
    DJANGO_IS_INSTALLED = False
else:
    DJANGO_IS_INSTALLED = True

DB_FILE_NAME = "anacron.db"
SEMAPHORE_FILE_NAME = "anacron.semaphore"
CONFIGURATION_FILE_NAME = "anacron.conf"
CONFIGURATION_SECTION = "anacron"
MONITOR_IDLE_TIME = 2.0  # seconds
WORKER_IDLE_TIME = 1.0  # seconds
RESULT_TTL = 1800  # storage time (time to live) for results in seconds
AUTOCONFIGURATION_TIMEOUT = 2  # seconds
AUTOCONFIGURATION_IDLE_TIME = 0.1  # seconds
CONFIGURABLE_SETTING_NAMES = (
    "monitor_idle_time",
    "worker_idle_time",
    "result_ttl",
    "autoconfiguration_timeout",
    "autoconfiguration_idle_time",
)


class Configuration:
    """
    Class providing the configuration settings.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, db_filename=DB_FILE_NAME):
        self.anacron_path = self._get_anacron_directory()
        self.db_filename = db_filename
        self.monitor_idle_time = MONITOR_IDLE_TIME
        self.worker_idle_time = WORKER_IDLE_TIME
        self.result_ttl = datetime.timedelta(minutes=RESULT_TTL)
        self.autoconfiguration_timeout = AUTOCONFIGURATION_TIMEOUT
        self.autoconfiguration_idle_time = AUTOCONFIGURATION_IDLE_TIME
        self.is_active = None
        self._read_configuration()

    def _get_anacron_directory(self):
        """
        Return the directory as a Path object, where the anacron files
        are stored. These files are the database, the semaphore file and
        an optional configuration files. This directory is typically

            "~.anacron/cwd_prefix/"

        """
        try:
            home_dir = pathlib.Path().home()
        except RuntimeError:
            # can't resolve homedir, take the present working
            # directory. Depending on the application .gitignore
            # should get extended with a ".anacron/*" entry.
            home_dir = self.cwd
            prefix = None
        else:
            prefix = self.cwd.as_posix().replace("/", "_")
        anacron_dir = home_dir / ".anacron"
        if prefix:
            anacron_dir = anacron_dir / prefix
        anacron_dir.mkdir(exist_ok=True)
        return anacron_dir

    def _read_configuration(self):
        """
        Read configuration data from an optional configuration file.
        The file must be in the anacron-directory named "anacron.conf".
        """
        parser = configparser.ConfigParser()
        if parser.read(self.configuration_file):
            # success
            try:
                values = parser[CONFIGURATION_SECTION]
            except KeyError:
                # ignore misconfigured file
                pass
            else:
                for name in CONFIGURABLE_SETTING_NAMES:
                    value = values.getfloat(name)
                    if value is not None:
                        self.__dict__[name] = value
                try:
                    setattr(self, "is_active", values.getboolean("is_active"))
                except ValueError:
                    pass

    def wait_for_autoconfiguration(self):
        """
        This method is designed to get called from a separate thread,
        so, in case of django, it can wait until the the django-settings
        are loaded without blocking the application. The method will
        return, when the application is ready to support the
        project-specific settings.
        """
        if DJANGO_IS_INSTALLED and self.is_active is None:
            start = time.monotonic()
            while True:
                if settings.configured:
                    try:
                        self.is_active = not settings.DEBUG
                    except AttributeError:
                        # this is a django configuration error
                        pass
                    break
                if time.monotonic() - start > self.autoconfiguration_timeout:
                    # on timeout anacron will not start
                    break
                time.sleep(self.autoconfiguration_idle_time)
        if self.is_active is None:
            # default is False
            self.is_active = False

    @property
    def configuration_file(self):
        """
        Provides the path to the configuration-file.
        """
        return self.anacron_path / CONFIGURATION_FILE_NAME

    @property
    def db_file(self):
        """
        Provides the path to the database-file.
        """
        return self.anacron_path / self.db_filename

    @property
    def semaphore_file(self):
        """
        Provides the path to the semaphore-file.
        """
        return self.anacron_path / SEMAPHORE_FILE_NAME

    @property
    def cwd(self):
        """
        Provides the current working directory.
        This is the working directory of the anacron importing application.
        """
        return pathlib.Path.cwd()


configuration = Configuration()
