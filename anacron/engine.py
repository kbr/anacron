"""
engine.py

Implementation of the anacron starter, worker and monitor.
"""
import atexit
import functools
import threading

from .configuration import configuration


class Monitor:
    """
    Manages the worker: start, restart and stop.
    The Monitor runs in a separate thread.
    """
    def __init__(self, terminate_flag):
        self.terminate_flag = terminate_flag  # threading.Event
        self.process = None

    def __call__(self):
        """
        Starts the monitor on calling the instance.
        The monitor runs in a separate thread.
        """
        pass



def start():
    """
    Starts the anacron monitor in case anacron is active and no other
    worker is already running. Otherwise do nothing. In case of success
    returns the terminator (the stop function with arguments provided by
    partial), otherwise returns None.
    """
    if configuration.is_active:
        try:
            configuration.semaphore_file.touch(exists_ok=False)
        except FileExistsError:
            # don't start the monitor if semaphore set
            pass
        else:
            # start monitor thread
            terminate_flag = threading.Event()
            monitor_thread = threading.Thread(target=Monitor(terminate_flag))
            monitor_thread.start()
            terminator = functools.partial(stop, terminate_flag, monitor_thread)
            atexit.register(terminator)
            return terminator
    return None


def stop(terminate_flag, monitor_thread):
    """
    Stop the monitor thread and in turn stopping the worker process.
    Also delete the semaphore_file.
    """
    if monitor_thread.is_alive():
        terminate_flag.set()
    if configuration.semaphore_file.exists():
        configuration.semaphore_file.unlink()
