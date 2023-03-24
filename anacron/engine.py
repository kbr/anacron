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


def worker_monitor(exit_event, database_file=None):
    """
    Monitors the subprocess and start/restart if the process is not up.
    """
    process = None
    while True:
        if process is None or process.poll() is not None:
            process = start_subprocess(database_file)
        if exit_event.wait(timeout=configuration.monitor_idle_time):
            break
    process.terminate()


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
        for _signal in (
            signal.SIGHUP,
            signal.SIGINT,
            signal.SIGQUIT,
            signal.SIGTERM,
            signal.SIGXCPU
        ):
            signal.signal(_signal, self.stop)

    def start(self, database_file=None):
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
                self.monitor_thread = threading.Thread(
                    target=worker_monitor,
                    args=(self.exit_event, database_file)
                )
                self.monitor_thread.start()
                return True  # monitor started
        return False  # monitor not started

    def stop(self, *args):  # pylint: disable=unused-argument
        """
        Shut down monitor thread and release semaphore file. `args`
        collect arguments provided because the method is a
        signal-handler. The arguments are the signal number and the
        current stack frame, that could be None or a frame object. To
        shut down, both arguments are ignored.
        """
        if self.monitor_thread.is_alive():
            self.exit_event.set()
        # keep compatibility with Python 3.7:
        if configuration.semaphore_file.exists():
            configuration.semaphore_file.unlink()


engine = Engine()
engine.start()
