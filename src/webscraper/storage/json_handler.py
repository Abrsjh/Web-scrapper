"""
JSON Storage Handler Module

This module implements a storage handler for JSON files.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from .base import BaseStorageHandler
from ..utils.exceptions import StorageError


class JSONStorageHandler(BaseStorageHandler):
    """
    Storage handler for JSON files.
    
    This handler saves and loads data in JSON format.
    """
    
    def save(self, data: List[Dict[str, Any]], path: str, **options) -> str:
        """
        Save data to a JSON file.
        
        Args:
            data: List of dictionaries containing the data to save
            path: Path where the JSON file should be saved
            options: Additional options for JSON saving
                - encoding: File encoding (default: 'utf-8')
                - indent: Indentation level (default: 2)
                - sort_keys: Whether to sort keys (default: False)
                - ensure_ascii: Whether to escape non-ASCII characters (default: False)
                - default: Function to convert non-serializable objects (default: None)
                - mode: File open mode (default: 'w')
                - root_key: Optional root key to wrap the data (default: None)
            
        Returns:
            Path where the JSON file was saved
            
        Raises:
            StorageError: If the data cannot be saved
        """
        try:
            # Validate data
            self.validate_data(data)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Extract options
            encoding = options.get('encoding', 'utf-8')
            indent = options.get('indent', 2)
            sort_keys = options.get('sort_keys', False)
            ensure_ascii = options.get('ensure_ascii', False)
            default = options.get('default')
            mode = options.get('mode', 'w')
            root_key = options.get('root_key')
            
            # Add root key if specified
            if root_key:
                data_to_save = {root_key: data}
            else:
                data_to_save = data
            
            # Save to JSON
            with open(path, mode, encoding=encoding) as f:
                json.dump(
                    data_to_save,
                    f,
                    indent=indent,
                    sort_keys=sort_keys,
                    ensure_ascii=ensure_ascii,
                    default=default
                )
            
            self.logger.info(f"Saved {len(data)} records to JSON file: {path}")
            return path
            
        except Exception as e:
            error_msg = f"Failed to save data to JSON file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e
    
    def load(self, path: str, **options) -> List[Dict[str, Any]]:
        """
        Load data from a JSON file.
        
        Args:
            path: Path of the JSON file to load
            options: Additional options for JSON loading
                - encoding: File encoding (default: 'utf-8')
                - root_key: Root key containing the data array (default: None)
                - object_hook: Function to transform JSON objects (default: None)
            
        Returns:
            List of dictionaries containing the loaded data
            
        Raises:
            StorageError: If the data cannot be loaded
        """
        try:
            if not os.path.exists(path):
                error_msg = f"JSON file not found: {path}"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            # Extract options
            encoding = options.get('encoding', 'utf-8')
            root_key = options.get('root_key')
            object_hook = options.get('object_hook')
            
            # Load from JSON
            with open(path, 'r', encoding=encoding) as f:
                loaded_data = json.load(f, object_hook=object_hook)
            
            # Extract data from root key if specified
            if root_key and isinstance(loaded_data, dict) and root_key in loaded_data:
                data = loaded_data[root_key]
            else:
                data = loaded_data
            
            # Ensure data is a list of dictionaries
            if not isinstance(data, list):
                error_msg = f"JSON data is not a list: {type(data)}"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            if not all(isinstance(item, dict) for item in data):
                error_msg = "JSON data is not a list of dictionaries"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            self.logger.info(f"Loaded {len(data)} records from JSON file: {path}")
            return data
            
        except Exception as e:
            error_msg = f"Failed to load data from JSON file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e