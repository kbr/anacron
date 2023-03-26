"""
anacron:

simple background task handling with no dependencies.
"""

# import shortcuts:
from .decorators import cron, delegate
# start engine depending on configuration
import .engine

__version__ = "0.1.dev"
