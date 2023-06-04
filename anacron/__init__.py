"""
anacron:

simple background task handling with no dependencies.
"""
from . import configuration
from .decorators import (
    cron,
    delay,
    delegate,
)
from .engine import engine as _engine


__all__ = ["start", "cron", "delay", "delegate"]
__version__ = "0.3.dev"


def start():
    """
    Call this from the framework of choice to explicitly
    activate anacron (not necessary for django).
    """
    _engine.start()
