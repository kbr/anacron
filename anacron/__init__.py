"""
anacron:

simple background task handling with no dependencies.
"""

from .configuration import activate
from .decorators import (
    cron,
    delegate,
)
from .engine import Engine


__all__ = ["activate", "cron", "delegate"]
__version__ = "0.2.dev"


_engine = Engine()
_engine.start()
