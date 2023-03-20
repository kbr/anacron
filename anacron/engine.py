"""
engine.py

Implementation of the anacron starter, worker and monitor.
"""
import functools
import signal
import subprocess
import sys
import threading

from anacron import worker
from .configuration import configuration


class Monitor:
    """
    Manages the worker: start, restart and stop.
    The Monitor runs in a separate thread.
    """
    def __init__(self):
        self.process = None

    def run(self, exit_event):
        """
        Starts the monitor on calling the instance.
        The monitor runs in a separate thread.
        """
        while True:
            if self.process is None or self.process.poll() is None:
                self.start_process()
            if exit_event.wait(timeout=configuration.monitor_idle_time):
                break
        if self.process and not self.process.poll():
            self.process.terminate()

    def start_process(self):
        """
        Starts the worker process in a detached subprocess.
        """
        if configuration.worker_allowed:
            cmd = [sys.executable, worker.__file__]
            cwd = configuration.cwd
            self.process = subprocess.Popen(cmd, cwd=cwd)


class Engine:

    def __init__(self):
        self.exit_event = threading.Event()
        self.monitor_thread = None
        self.monitor = None
        for _signal in (
            signal.SIGHUP,
            signal.SIGINT,
            signal.SIGQUIT,
            signal.SIGTERM,
            signal.SIGXCPU
        ):
            signal.signal(_signal, self.stop)

    def start(self):
        """
        Starts the monitor in case anacron is active and no other
        monitor is already running. Return True if a monitor thread has
        been started, otherwise False. These return values are for
        testing.
        """
        if configuration.is_active:
            try:
                configuration.semaphore_file.touch(exist_ok=False)
            except FileExistsError:
                # don't start the monitor if semaphore set
                pass
            else:
                # start monitor thread
                self.monitor = Monitor()
                self.monitor_thread = threading.Thread(
                    target=self.monitor.run, args=(self.exit_event,)
                )
                self.monitor_thread.start()
                return True  # monitor started
        return False  # monitor not started

    def stop(self, *args):
        """
        Shut down monitor thread and release semaphore file.
        """
        if self.monitor_thread.is_alive():
            self.exit_event.set()
        # keep compatibility with Python 3.7:
        if configuration.semaphore_file.exists():
            configuration.semaphore_file.unlink()
