"""
engine.py

decorators and taskhandlers
"""

from .configuration import configuration
from .schedule import CronScheduler
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
            cs = CronScheduler(crontab=crontab)
            schedule = cs.get_next_schedule()
            interface.register_callable(func, schedule=schedule, crontab=crontab)
        return func

