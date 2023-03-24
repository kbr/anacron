"""
utils.py

space for reusable functions
"""

import signal


SHUTDOWN_SIGNALS = (
    signal.SIGHUP,
    signal.SIGINT,
    signal.SIGQUIT,
    signal.SIGTERM,
    signal.SIGXCPU,
)


def register_shutdown_handler(handler):
    """
    Register the given `handler` (a callable) to get called on signals
    indicating a shutdown of the running process.
    """
    for signal_number in SHUTDOWN_SIGNALS:
        signal.signal(signal_number, handler)
