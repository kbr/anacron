"""
anacron:

simple background task handling with no dependencies.
"""

from . import configuration
from .decorators import (
    cron,
    delegate,
)
from .engine import Engine


__all__ = ["activate", "cron", "delegate"]
__version__ = "0.2.dev"


_engine = Engine()
_engine.start()


def activate():
    """
    Call this from the framework of choice to explicitly
    activate anacron (not necessary for django).
    """
    configuration.configuration.is_active = True
    _engine.start()
