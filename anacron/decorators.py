"""
decorators.py
"""

import uuid

from .configuration import configuration
from .schedule import CronScheduler
from .sql_interface import interface


# run every minute:
DEFAULT_CRONTAB = "* * * * *"


# pylint: disable=too-many-arguments
def cron(minutes=None, hours=None, dow=None, months=None, dom=None,
         crontab=DEFAULT_CRONTAB):
    """
    Decorator function for a cronjob.

    Functions running cronjobs should not get called from the main
    program and therefore don't get attributes. Usage for a cronjob to
    run every hour:

        @cron("* 1 * * *")
        def some_callable():
            # do periodic stuff here ...

    """
    # set crontab to default if no other arguments are given:
    if not any(locals().values()):
        crontab = DEFAULT_CRONTAB

    def wrapper(func):
        scheduler = CronScheduler(
            minutes=minutes,
            hours=hours,
            dow=dow,
            months=months,
            dom=dom,
            crontab=crontab
        )
        schedule = scheduler.get_next_schedule()
        for entry in interface.get_tasks_by_signature(func):
            # there should be just a single entry.
            # however iterate over all entries and
            # test for a non-empty crontab-string.
            if entry["crontab"]:
                # delete existing cronjob(s) of the same callable
                interface.delete_callable(entry)
        interface.register_callable(func, schedule=schedule, crontab=crontab)
        return func

    return wrapper


class delegate:  # pylint: disable=invalid-name
    """
    class based decorator for a delayed task.
    Can get called with an optional argument:

        @delegate
        def do_work(arguments):
            ...

    A call to `do_work()` will return None and the function itself will
    get handled with some delay in another process.

    The decorator get called with an optional argument (in the example
    given as keyword-argument, but will also work with just `True`
    provided as first argument):

        @delegate(provide_result=True)
        def do_work(arguments):
            ...

    In this case a call to `do_work()` will return an id (a `uuid`) that
    can be used later to fetch the result.

    """
    def __init__(self, provide_result=False):
        if callable(provide_result):
            self.func = provide_result
            self.provide_result = None
        else:
            self.func = None
            self.provide_result = provide_result

    def __call__(self, *args, **kwargs):
        if self.func:
            return self.wrapper(*args, **kwargs)
        self.func = args[0]
        return self.wrapper

    def wrapper(self, *args, **kwargs):
        """Wraps the decorated function
        """
        if not configuration.is_active:
            return self.func(*args, **kwargs)
        data = {"args": args, "kwargs": kwargs}
        if self.provide_result:
            uid = uuid.uuid4().hex
        else:
            uid = None
        data["uuid"] = uid
        interface.register_callable(self.func, **data)
        if uid:
            # result requested:
            interface.register_result(self.func, **data)
        return uid
