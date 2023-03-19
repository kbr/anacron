"""
decorators.py
"""
import functools

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
    def wrapper(func):
        if configuration.is_active:
            cs = CronScheduler(crontab=crontab)
            schedule = cs.get_next_schedule()
            for entry in interface.find_callables(func):
                # there should be just a single entry.
                # however iterate over all entries and
                # test for a non-empty crontab-string.
                if entry["crontab"]:
                    # delete existing cronjob(s) of the same callable
                    interface.delete_callable(entry)
            interface.register_callable(
                func, schedule=schedule, crontab=crontab
            )
        return func
    return wrapper


def delegate(func):
    """
    Decorator function for a delayed task.

    The decorated function will return immediately without running
    the function. The function will get executed later.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        interface.register_callable(func, args=args, kwargs=kwargs)
        return None  # or uuid later?

    if not configuration.is_active:
        return func
    return wrapper
