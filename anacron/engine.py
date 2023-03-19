"""
engine.py

decorators and taskhandlers
"""

from .configuration import configuration
from .sql_interface import interface


# run every minute:
DEFAULT_CRONTAB = "* * * * *"


def cron(crontab=DEFAULT_CRONTAB):
    """
    Decorator function for a cronjob.
    Functions running cronjobs should not get called from the main
    program and therefore don't get attributes. Usage for a cronjob to
    run every hour:

        @cron("* 1 * * *")
        def some_callable():
            # do periodic stuff here ...
    """
    def inner(func):
        if configuration.is_active:
#             interface.register_callable(func, )
            pass

