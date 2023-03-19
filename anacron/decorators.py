"""
decorators.py
"""

from .configuration import configuration
from .schedule import CronScheduler
from .sql_interface import interface


# run every minute:
DEFAULT_CRONTAB = "* * * * *"


def cron(crontab=DEFAULT_CRONTAB, interface=interface):
    """
    Decorator function for a cronjob.
    Functions running cronjobs should not get called from the main
    program and therefore don't get attributes. Usage for a cronjob to
    run every hour:

        @cron("* 1 * * *")
        def some_callable():
            # do periodic stuff here ...

    The `interface` argument is for testing and should not get
    used in real applications.
    """
    def wrapper(func):
        if configuration.is_active:
            cs = CronScheduler(crontab=crontab)
            schedule = cs.get_next_schedule()
            # convert to a list because parts of the selection provided
            # by the generator may get deleted during iteration.
            # (at time of writing it is unknown whether there may bei side-effects)
            for entry in list(interface.find_callables(func)):
                # there should be just a single entry.
                # however iterate over all entries and
                # test for a non-empty crontab-string.
                if entry["crontab"]:
                    # delete existing cronjob and store a new one.
                    interface.delete_callable(entry)
            interface.register_callable(func, schedule=schedule, crontab=crontab)
        return func
    return wrapper

