"""
Base Storage Handler Module

This module defines the abstract base class for all storage handlers.
It provides the contract that all concrete storage handlers must follow.
"""

import abc
import logging
from typing import Any, Dict, List, Optional

from ..utils.exceptions import StorageError


class BaseStorageHandler(abc.ABC):
    """
    Abstract base class for all storage handlers.
    
    This class defines the interface that all storage handlers must implement.
    It provides common functionality for saving and loading data in different formats.
    """
    
    def __init__(self):
        """Initialize the base storage handler."""
        self.logger = logging.getLogger(__name__)
    
    @abc.abstractmethod
    def save(self, data: List[Dict[str, Any]], path: str, **options) -> str:
        """
        Save data to the specified path.
        
        Args:
            data: List of dictionaries containing the data to save
            path: Path where the data should be saved
            options: Additional options for the specific storage handler
            
        Returns:
            Path where the data was saved
            
        Raises:
            StorageError: If the data cannot be saved
        """
        pass
    
    @abc.abstractmethod
    def load(self, path: str, **options) -> List[Dict[str, Any]]:
        """
        Load data from the specified path.
        
        Args:
            path: Path from which to load the data
            options: Additional options for the specific storage handler
            
        Returns:
            List of dictionaries containing the loaded data
            
        Raises:
            StorageError: If the data cannot be loaded
        """
        pass
    
    def validate_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Validate the data before saving.
        
        Args:
            data: Data to validate
            
        Raises:
            ValueError: If the data is invalid
        """
        if not isinstance(data, list):
            raise ValueError("Data must be a list of dictionaries")
        
        if not all(isinstance(item, dict) for item in data):
            raise ValueError("Each item in the data must be a dictionary")
    
    def __repr__(self) -> str:
        """String representation of the storage handler."""
        return f"{self.__class__.__name__}()"