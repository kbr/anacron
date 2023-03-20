"""
worker.py

worker class for handling cron and delegated tasks.
"""

import signal
import time

from .configuration import configuration
from .sql_interface import interface


class Worker:

    def __init__(self):
        self.active = True
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
        while self.active:
            tasks = list(interface.get_callables())
            if tasks:
                for task in tasks:
                    self.handle_task(task)
            else:
                time.sleep(configuration.worker_idle_time)


def start_worker():
    worker = Worker()
    worker.run()


if __name__ == "__main__":
    start_worker()
