"""
Storage Package

This package contains all the storage handler implementations.
"""

from .base import BaseStorageHandler
from .csv_handler import CSVStorageHandler
from .json_handler import JSONStorageHandler
from .excel_handler import ExcelStorageHandler
from .database_handler import DatabaseStorageHandler
from ..utils.exceptions import StorageError


def get_storage_handler(format_type: str) -> BaseStorageHandler:
    """
    Factory function to get a storage handler instance based on format type.
    
    Args:
        format_type (str): Type of format ('csv', 'json', 'excel', 'db', 'database')
        
    Returns:
        BaseStorageHandler: An instance of the appropriate storage handler
        
    Raises:
        ValueError: If the format type is not supported
    """
    format_type = format_type.lower()
    
    handlers = {
        'csv': CSVStorageHandler,
        'json': JSONStorageHandler,
        'excel': ExcelStorageHandler,
        'xlsx': ExcelStorageHandler,
        'xls': ExcelStorageHandler,
        'db': DatabaseStorageHandler,
        'database': DatabaseStorageHandler,
        'sqlite': DatabaseStorageHandler,
        'postgresql': DatabaseStorageHandler,
        'postgres': DatabaseStorageHandler,
        'mysql': DatabaseStorageHandler,
    }
    
    if format_type not in handlers:
        raise ValueError(f"Unsupported storage format: {format_type}. "
                         f"Supported formats: {list(handlers.keys())}")
    
    return handlers[format_type]()