"""Polymarket data collector and analyzer."""

__version__ = "1.0.0"
__author__ = "Polymarket Collector Team"

from .collectors import UserDataCollector
from .reports import ReportAnalyzer
from .api import GammaAPIClient, DataAPIClient

__all__ = [
    "UserDataCollector",
    "ReportAnalyzer",
    "GammaAPIClient",
    "DataAPIClient",
]