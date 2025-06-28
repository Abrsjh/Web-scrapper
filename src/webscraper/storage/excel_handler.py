"""
Excel Storage Handler Module

This module implements a storage handler for Excel files.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .base import BaseStorageHandler
from ..utils.exceptions import StorageError


class ExcelStorageHandler(BaseStorageHandler):
    """
    Storage handler for Excel files.
    
    This handler saves and loads data in Excel format.
    """
    
    def save(self, data: List[Dict[str, Any]], path: str, **options) -> str:
        """
        Save data to an Excel file.
        
        Args:
            data: List of dictionaries containing the data to save
            path: Path where the Excel file should be saved
            options: Additional options for Excel saving
                - sheet_name: Name of the sheet (default: 'Sheet1')
                - engine: Excel writer engine (default: 'openpyxl')
                - index: Whether to include index column (default: False)
                - freeze_panes: Tuple of (row, col) to freeze (default: None)
                - autofilter: Whether to add autofilter (default: False)
                - float_format: Format for float columns (default: None)
                - datetime_format: Format for datetime columns (default: None)
                - mode: File open mode (default: 'w')
                - multiple_sheets: Dict of sheet_name -> data for multiple sheets
                  (default: None). If provided, the main data is ignored.
            
        Returns:
            Path where the Excel file was saved
            
        Raises:
            StorageError: If the data cannot be saved
        """
        try:
            # Validate data
            self.validate_data(data)
            
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
            
            # Extract options
            sheet_name = options.get('sheet_name', 'Sheet1')
            engine = options.get('engine', 'openpyxl')
            index = options.get('index', False)
            freeze_panes = options.get('freeze_panes')
            autofilter = options.get('autofilter', False)
            float_format = options.get('float_format')
            datetime_format = options.get('datetime_format')
            mode = options.get('mode', 'w')
            multiple_sheets = options.get('multiple_sheets')
            
            # Check if we're saving multiple sheets
            if multiple_sheets and isinstance(multiple_sheets, dict):
                # Validate each sheet's data
                for sheet_data in multiple_sheets.values():
                    self.validate_data(sheet_data)
                
                # Create Excel writer
                with pd.ExcelWriter(
                    path,
                    engine=engine,
                    mode=mode,
                    datetime_format=datetime_format
                ) as writer:
                    # Save each sheet
                    for sheet, sheet_data in multiple_sheets.items():
                        df = pd.DataFrame(sheet_data)
                        df.to_excel(
                            writer,
                            sheet_name=sheet,
                            index=index,
                            float_format=float_format
                        )
                        
                        # Apply autofilter if requested
                        if autofilter:
                            worksheet = writer.sheets[sheet]
                            worksheet.auto_filter.ref = worksheet.dimensions
                        
                        # Apply freeze panes if requested
                        if freeze_panes:
                            worksheet = writer.sheets[sheet]
                            worksheet.freeze_panes = freeze_panes
                
                self.logger.info(f"Saved {len(multiple_sheets)} sheets to Excel file: {path}")
            
            else:
                # Convert to DataFrame
                df = pd.DataFrame(data)
                
                # Create Excel writer
                with pd.ExcelWriter(
                    path,
                    engine=engine,
                    mode=mode,
                    datetime_format=datetime_format
                ) as writer:
                    # Save to Excel
                    df.to_excel(
                        writer,
                        sheet_name=sheet_name,
                        index=index,
                        float_format=float_format
                    )
                    
                    # Apply autofilter if requested
                    if autofilter:
                        worksheet = writer.sheets[sheet_name]
                        worksheet.auto_filter.ref = worksheet.dimensions
                    
                    # Apply freeze panes if requested
                    if freeze_panes:
                        worksheet = writer.sheets[sheet_name]
                        worksheet.freeze_panes = freeze_panes
                
                self.logger.info(f"Saved {len(data)} records to Excel file: {path}")
            
            return path
            
        except Exception as e:
            error_msg = f"Failed to save data to Excel file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e
    
    def load(self, path: str, **options) -> List[Dict[str, Any]]:
        """
        Load data from an Excel file.
        
        Args:
            path: Path of the Excel file to load
            options: Additional options for Excel loading
                - sheet_name: Name or index of the sheet to load (default: 0)
                  Can also be None to load all sheets, or a list to load multiple sheets.
                - engine: Excel reader engine (default: 'openpyxl')
                - header: Row number to use as column names (default: 0)
                - na_values: Additional strings to recognize as NA/NaN (default: None)
                - parse_dates: List of column names to parse as dates (default: None)
                - dtype: Dict of column name -> dtype for column types (default: None)
                - convert_float: Convert integers to float if needed (default: True)
                
        Returns:
            List of dictionaries containing the loaded data. If sheet_name is None,
            returns a dict of sheet_name -> data.
            
        Raises:
            StorageError: If the data cannot be loaded
        """
        try:
            if not os.path.exists(path):
                error_msg = f"Excel file not found: {path}"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            # Extract options
            sheet_name = options.get('sheet_name', 0)
            engine = options.get('engine', 'openpyxl')
            header = options.get('header', 0)
            na_values = options.get('na_values')
            parse_dates = options.get('parse_dates')
            dtype = options.get('dtype')
            convert_float = options.get('convert_float', True)
            
            # Load from Excel
            excel_options = {
                'engine': engine,
                'header': header,
                'na_values': na_values,
                'parse_dates': parse_dates,
                'dtype': dtype,
                'convert_float': convert_float,
            }
            
            # Check if we're loading all sheets
            if sheet_name is None:
                # Load all sheets
                sheet_dict = pd.read_excel(path, sheet_name=None, **excel_options)
                
                # Convert each sheet to list of dictionaries
                result = {}
                for name, df in sheet_dict.items():
                    result[name] = df.to_dict(orient='records')
                
                total_records = sum(len(records) for records in result.values())
                self.logger.info(f"Loaded {total_records} records from {len(result)} sheets in Excel file: {path}")
                return result
            
            # Check if we're loading multiple sheets
            elif isinstance(sheet_name, list):
                # Load specified sheets
                sheet_dict = pd.read_excel(path, sheet_name=sheet_name, **excel_options)
                
                # Convert each sheet to list of dictionaries
                result = {}
                for name, df in sheet_dict.items():
                    result[name] = df.to_dict(orient='records')
                
                total_records = sum(len(records) for records in result.values())
                self.logger.info(f"Loaded {total_records} records from {len(result)} sheets in Excel file: {path}")
                return result
            
            # Load single sheet
            else:
                df = pd.read_excel(path, sheet_name=sheet_name, **excel_options)
                data = df.to_dict(orient='records')
                
                self.logger.info(f"Loaded {len(data)} records from Excel file: {path}")
                return data
            
        except Exception as e:
            error_msg = f"Failed to load data from Excel file: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e