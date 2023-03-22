"""
worker.py

worker class for handling cron and delegated tasks.
"""

import importlib
import signal
import time

from .configuration import configuration
from .sql_interface import interface


class Worker:
    """
    Runs in a separate process for task-handling.
    Gets supervised and monitored from the engine.
    """
    def __init__(self):
        self.active = True
        self.result = None
        self.error_message = None
        # set the termination handler
        for _signal in (
            signal.SIGHUP,
            signal.SIGINT,
            signal.SIGQUIT,
            signal.SIGTERM,
            signal.SIGXCPU
        ):
            signal.signal(_signal, self.terminate)

    def terminate(self, *args):
        self.active = False

    def run(self):
        """
        Main event loop for the worker. Takes callables and processes
        them as long as callables are available. Otherwise keep idle.
        """
        while self.active:
            if not self.handle_tasks():
                time.sleep(configuration.worker_idle_time)

    def handle_tasks(self):
        """
        Checks for tasks and process them. If there are no tasks to
        handle the method returns False indication that the main loop
        can switch to idle state. If  tasks have been handled, the
        method returns True to indicate that meanwhile more tasks may be
        waiting.
        """
        tasks = interface.get_callables()
        if tasks:
            for task in tasks:
                self.process_task(task)
                self.postprocess_task(task)
            return True
        return False

    def process_task(self, task):
        """
        Handle the given task. The task is a dictionary as returned from
        SQLInterface._fetch_all_callable_entries(cursor):

            {
                "rowid": integer,
                "schedule": datetime,
                "crontab": string,
                "function_module": string,
                "function_name": string,
                "args": tuple(of original datatypes),
                "kwargs": dict(of original datatypes),
            }

        """
        module = importlib.import_module(task["function_module"])
        function = getattr(module, task["function_name"])
        try:
            self.result = function(*args, **kwargs)
        except Exception as err:
            self.error_message = err.__repr__()

    def postprocess_task(self, task):
        """
        Delete or update the task and do something with the result or
        error-message.
        """
        pass
        # clean up after processing
        self.error_message = None
        self.result = None


def start_worker():
    worker = Worker()
    worker.run()


if __name__ == "__main__":
    start_worker()
