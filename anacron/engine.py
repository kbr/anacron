"""
engine.py

Implementation of the anacron engine and the worker monitor.
"""

import pathlib
import signal
import subprocess
import sys
import threading

from .configuration import configuration


WORKER_MODULE_NAME = "worker.py"


def start_subprocess(database_file=None):
    """
    Starts the worker process in a detached subprocess.
    An optional `database_file` will get forwarded to the worker to use
    this instead of the configured one. This argument is for testing.
    """
    worker_file = pathlib.Path(__file__).parent / WORKER_MODULE_NAME
    cmd = [sys.executable, worker_file]
    if database_file:
        cmd.append(database_file)
    cwd = configuration.cwd
    return subprocess.Popen(cmd, cwd=cwd)


def start_worker_monitor(exit_event, database_file=None):
    """
    Monitors the subprocess and start/restart if the process is not up.
    """
    process = None
    while True:
        if process is None or process.poll() is not None:
            process = start_subprocess(database_file)
        if exit_event.wait(timeout=configuration.monitor_idle_time):
            break
    # got exit event: terminate worker and clear semaphore
    process.terminate()
    remove_semaphore_file()


def remove_semaphore_file():
    """
    Delete the semaphore file. It is not an error if the file does not
    exist.
    """
    # keep compatibility with Python 3.7 for file deletion.
    # From Python 3.8 on .unlink(missing_ok=True) will do the job.
    try:
        configuration.semaphore_file.unlink()
    except FileNotFoundError:
        pass


class Engine:
    """
    The Engine is the entry-point for anacron. On import an Entry
    instance gets created and the method start is called. Depending on
    the configuration will start the worker-monitor and the background
    process. If the (auto-)configuration is not active, the method start
    will just return doing nothing.
    """
    def __init__(self):
        self.exit_event = threading.Event()
        self.monitor_thread = None
        self.monitor = None
        self._start_allowed = None
        self.original_handlers = {
            signalnum: signal.signal(signalnum, self._terminate)
            for signalnum in (signal.SIGINT, signal.SIGTERM)
        }

    @property
    def start_allowed(self):
        """
        boolean: indicates whether the engine is allowed to start a
        monitor and a worker.
        """
        if self._start_allowed is None:
            result = False
            if configuration.is_active:
                try:
                    configuration.semaphore_file.touch(exist_ok=False)
                except FileExistsError:
                    pass
                else:
                    result = True
            self._start_allowed = result
        return self._start_allowed

    def django_autostart(self, database_file=None):
        """
        Special start-method for django.
        Will start anacron if django.settings.DEBUG is False
        """
        # this will block if django is not ready.
        # therefore django_autostart must get called from the
        # AppConfig.ready() method.
        debug_mode = configuration.get_django_debug_setting()
        if not debug_mode:
            self.start(database_file=database_file)

    def start(self, database_file=None):
        """
        Starts the monitor in case no other monitor is already running
        and the configuration indicates that anacron is active.
        """
        if self.start_allowed:
            # start monitor thread
            self.monitor_thread = threading.Thread(
                target=start_worker_monitor,
                args=(self.exit_event, database_file)
            )
            self.monitor_thread.start()

    def stop(self):
        """
        Shut down monitor thread and release semaphore file. `args`
        collect arguments provided because the method is a
        signal-handler. The arguments are the signal number and the
        current stack frame, that could be None or a frame object. To
        shut down, both arguments are ignored.
        """
        if self.monitor_thread:  # and self.monitor_thread.is_alive():
            self.exit_event.set()
            self.monitor_thread = None
            remove_semaphore_file()

    def _terminate(self, signalnum, stackframe=None):
        """
        Terminate anacron by calling the engine.stop method. Afterward
        reraise the signal again for the original signal-handler.
        """
        self.stop()
        signal.signal(signalnum, self.original_handlers[signalnum])
        signal.raise_signal(signalnum)  # requires Python >= 3.8


engine = Engine()
