"""
engine.py

Implementation of the anacron engine and the worker monitor.
"""
import atexit
import pathlib
import subprocess
import sys
import threading

from .configuration import configuration
from .sql_interface import interface


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


def worker_monitor(exit_event, database_file=None):
    """
    Monitors the subprocess and start/restart if the process is not up.
    """
    # wait for auto-configuration to decide whether the thread should
    # start the worker process, or terminate.
    configuration.wait_for_autoconfiguration()
    if configuration.is_active:
        process = None
        while True:
            if process is None or process.poll() is not None:
                process = start_subprocess(database_file)
            if exit_event.wait(timeout=configuration.monitor_idle_time):
                break
        process.terminate()
    else:
        remove_semaphore_file()

def remove_semaphore_file():
    """
    Delete the semaphore file. It is not an error if the file does not
    exist.
    """
    # keep compatibility with Python 3.7 for file deletion.
    # From Python 3.8 on .unlink(missing_ok=True) will do the job.
    if configuration.semaphore_file.exists():
        configuration.semaphore_file.unlink()


def clean_up():
    """
    Clean up on shutting down the application: delete an existing
    semaphore file and remove all cronjob entries from the database.
    (cronjobs will populate the database at start-up and can change.)
    This method may get called multiple times, but that doesn't
    hurt.
    """
    remove_semaphore_file()
    # delete cronjobs on shutdown because on next startup
    # the callables and according crontabs may have changed
    interface.delete_cronjobs()


def start_allowed():
    """
    Returns a boolean whether the Engine is allowed to start.
    The Engine is not allowed to start if the configuration forbids this
    or if another worker is already running and the semaphore file is
    set.
    """
    result = False
    if configuration.is_active:
        try:
            configuration.semaphore_file.touch(exist_ok=False)
        except FileExistsError:
            pass
        else:
            result = True
    return result


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

    def start(self, database_file=None):
        """
        Starts the monitor in case no other monitor is already running
        and the configuration indicates that anacron is active.
        """
        if start_allowed():
            # start monitor thread
            self.monitor_thread = threading.Thread(
                target=worker_monitor,
                args=(self.exit_event, database_file)
            )
            self.monitor_thread.start()
            # register stop() to terminate monitor-thread
            # and subprocess on shutdown
            atexit.register(self.stop)


    def stop(self, *args, **kwargs):  # pylint: disable=unused-argument
        """
        Shut down monitor thread and release semaphore file. `args`
        collect arguments provided because the method is a
        signal-handler. The arguments are the signal number and the
        current stack frame, that could be None or a frame object. To
        shut down, both arguments are ignored.
        """
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.exit_event.set()
        clean_up()


engine = Engine()
