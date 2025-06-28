"""
CSV Storage Handler Module

This module implements a storage handler for CSV files.
"""

import csv
import logging
import os
from typing import Any, Dict, List, Optional

import pandas as pd

from .base import BaseStorageHandler
from ..utils.exceptions import StorageError


class CSVStorageHandler(BaseStorageHandler):
    """
    Storage handler for CSV files.
    
    This handler saves and loads data in CSV format.
    """
    
    def save(self, data: List[Dict[str, Any]], path: str, **options) -> str:
        """
        Save data to a CSV file.
        
        Args:
            data: List of dictionaries containing the data to save
            path: Path where the CSV file should be saved
            options: Additional options for CSV saving
                - encoding: File encoding (default: 'utf-8')
                - index: Whether to include index column (default: False)
                - mode: File open mode (default: 'w')
                - sep: Field separator (default: ',')
                - quoting: CSV quoting style (default: csv.QUOTE_MINIMAL)
                - date_format: Format for date columns (default: None)
                - float_format: Format for float columns (default: None)
                - na_rep: String representation of NULL (default: '')
                - header: Whether to include header (default: True)
            
        Returns:
            Path where the CSV file was saved
            
        Raises:
            StorageError: If the data cannot be saved
        """
        try:
            # Validate data
            self.validate_data(data)
            
            if not data:
                self.logger.warning("No data to save")
                # Create an empty file
                with open(path, 'w', encoding=options.get('encoding', 'utf-8')):
                    pass
                return path
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Extract options
            csv_options = {
                'encoding': options.get('encoding', 'utf-8'),
                'index': options.get('index', False),
                'sep': options.get('sep', ','),
                'quoting': options.get('quoting', csv.QUOTE_MINIMAL),
                'date_format': options.get('date_format'),
                'float_format': options.get('float_format'),
                'na_rep': options.get('na_rep', ''),
                'header': options.get('header', True),
            }
            
            # Save to CSV
            df.to_csv(path, **csv_options)
            
            self.logger.info(f"Saved {len(data)} records to CSV file: {path}")
            return path
            
        except Exception as e:
            error_msg = f"Failed to save data to CSV file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e
    
    def load(self, path: str, **options) -> List[Dict[str, Any]]:
        """
        Load data from a CSV file.
        
        Args:
            path: Path of the CSV file to load
            options: Additional options for CSV loading
                - encoding: File encoding (default: 'utf-8')
                - sep: Field separator (default: ',')
                - quoting: CSV quoting style (default: csv.QUOTE_MINIMAL)
                - parse_dates: List of column names to parse as dates (default: None)
                - na_values: Additional strings to recognize as NA/NaN (default: None)
                - header: Row number to use as column names (default: 0)
            
        Returns:
            List of dictionaries containing the loaded data
            
        Raises:
            StorageError: If the data cannot be loaded
        """
        try:
            if not os.path.exists(path):
                error_msg = f"CSV file not found: {path}"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            # Extract options
            csv_options = {
                'encoding': options.get('encoding', 'utf-8'),
                'sep': options.get('sep', ','),
                'quoting': options.get('quoting', csv.QUOTE_MINIMAL),
                'parse_dates': options.get('parse_dates'),
                'na_values': options.get('na_values'),
                'header': options.get('header', 0),
            }
            
            # Load from CSV
            df = pd.read_csv(path, **csv_options)
            
            # Convert to list of dictionaries
            data = df.to_dict(orient='records')
            
            self.logger.info(f"Loaded {len(data)} records from CSV file: {path}")
            return data
            
        except Exception as e:
            error_msg = f"Failed to load data from CSV file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e