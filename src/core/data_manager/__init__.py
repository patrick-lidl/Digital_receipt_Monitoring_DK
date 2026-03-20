"""Manage all data loading and saving."""

__all__ = ["DataManager", "TableNotFoundError"]

from .base import TableNotFoundError
from .data_manager import DataManager
