"""
Database Storage Handler Module

This module implements a storage handler for database storage using SQLAlchemy.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from sqlalchemy import create_engine, inspect, MetaData, Table, Column, String, Float, Integer, Text, DateTime, Boolean
from sqlalchemy.exc import SQLAlchemyError

from .base import BaseStorageHandler
from ..utils.exceptions import StorageError


class DatabaseStorageHandler(BaseStorageHandler):
    """
    Storage handler for database storage.
    
    This handler saves and loads data using SQLAlchemy.
    Default database is SQLite, but other databases like PostgreSQL
    are supported through connection strings.
    """
    
    def __init__(self):
        """Initialize the database storage handler."""
        super().__init__()
        self.engine = None
        self.metadata = MetaData()
    
    def save(self, data: List[Dict[str, Any]], path: str, **options) -> str:
        """
        Save data to a database.
        
        Args:
            data: List of dictionaries containing the data to save
            path: Database connection string or path to SQLite file
            options: Additional options for database saving
                - table_name: Name of the table (default: 'scraped_data')
                - if_exists: What to do if the table exists ('fail', 'replace', 'append')
                  (default: 'replace')
                - schema: Database schema (default: None)
                - dtype: Dict of column name -> SQLAlchemy type (default: None)
                - index: Whether to include index column (default: False)
                - index_label: Column label for index (default: None)
                - chunksize: Rows to write at once (default: None)
                - method: SQL insertion method (default: None)
            
        Returns:
            Path where the data was saved (connection string or SQLite path)
            
        Raises:
            StorageError: If the data cannot be saved
        """
        try:
            # Validate data
            self.validate_data(data)
            
            if not data:
                self.logger.warning("No data to save")
                return path
            
            # Extract options
            table_name = options.get('table_name', 'scraped_data')
            if_exists = options.get('if_exists', 'replace')
            schema = options.get('schema')
            dtype = options.get('dtype')
            index = options.get('index', False)
            index_label = options.get('index_label')
            chunksize = options.get('chunksize')
            method = options.get('method')
            
            # Handle SQLite file paths
            if path.startswith('sqlite:///'):
                # Path is already a connection string
                connection_string = path
            elif path.lower().endswith('.db') or path.lower().endswith('.sqlite'):
                # Convert path to SQLite connection string
                db_path = os.path.abspath(path)
                # Ensure directory exists
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
                connection_string = f"sqlite:///{db_path}"
            else:
                # Assume it's a connection string
                connection_string = path
            
            # Create engine
            self.engine = create_engine(connection_string)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Save to database
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                schema=schema,
                dtype=dtype,
                index=index,
                index_label=index_label,
                chunksize=chunksize,
                method=method
            )
            
            self.logger.info(f"Saved {len(data)} records to database table: {table_name}")
            return path
            
        except Exception as e:
            error_msg = f"Failed to save data to database: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e
    
    def load(self, path: str, **options) -> List[Dict[str, Any]]:
        """
        Load data from a database.
        
        Args:
            path: Database connection string or path to SQLite file
            options: Additional options for database loading
                - table_name: Name of the table to load (default: 'scraped_data')
                - schema: Database schema (default: None)
                - columns: List of columns to select (default: None, all columns)
                - where: SQL WHERE clause (default: None)
                - order_by: SQL ORDER BY clause (default: None)
                - limit: Maximum number of rows to return (default: None)
                - offset: Number of rows to skip (default: None)
                - query: Raw SQL query (default: None, overrides other options)
            
        Returns:
            List of dictionaries containing the loaded data
            
        Raises:
            StorageError: If the data cannot be loaded
        """
        try:
            # Handle SQLite file paths
            if path.startswith('sqlite:///'):
                # Path is already a connection string
                connection_string = path
            elif path.lower().endswith('.db') or path.lower().endswith('.sqlite'):
                # Convert path to SQLite connection string
                db_path = os.path.abspath(path)
                if not os.path.exists(db_path):
                    error_msg = f"SQLite database file not found: {db_path}"
                    self.logger.error(error_msg)
                    raise StorageError(error_msg)
                connection_string = f"sqlite:///{db_path}"
            else:
                # Assume it's a connection string
                connection_string = path
            
            # Create engine
            self.engine = create_engine(connection_string)
            
            # Extract options
            table_name = options.get('table_name', 'scraped_data')
            schema = options.get('schema')
            columns = options.get('columns')
            where = options.get('where')
            order_by = options.get('order_by')
            limit = options.get('limit')
            offset = options.get('offset')
            query = options.get('query')
            
            # Check if table exists
            inspector = inspect(self.engine)
            if not query and table_name not in inspector.get_table_names(schema=schema):
                error_msg = f"Table not found: {table_name}"
                self.logger.error(error_msg)
                raise StorageError(error_msg)
            
            # Build query if not provided
            if not query:
                columns_str = "*" if not columns else ", ".join(columns)
                query = f"SELECT {columns_str} FROM {table_name}"
                
                if schema:
                    query = f"SELECT {columns_str} FROM {schema}.{table_name}"
                
                if where:
                    query += f" WHERE {where}"
                
                if order_by:
                    query += f" ORDER BY {order_by}"
                
                if limit:
                    query += f" LIMIT {limit}"
                
                if offset:
                    query += f" OFFSET {offset}"
            
            # Load from database
            df = pd.read_sql(query, self.engine)
            
            # Convert to list of dictionaries
            data = df.to_dict(orient='records')
            
            self.logger.info(f"Loaded {len(data)} records from database")
            return data
            
        except Exception as e:
            error_msg = f"Failed to load data from database: {str(e)}"
            self.logger.error(error_msg)
            raise StorageError(error_msg) from e
    
    def create_table_from_data(self, data: List[Dict[str, Any]], table_name: str, schema: Optional[str] = None) -> Table:
        """
        Create a SQLAlchemy table definition from data.
        
        Args:
            data: List of dictionaries containing the data
            table_name: Name of the table to create
            schema: Database schema (default: None)
            
        Returns:
            SQLAlchemy Table object
        """
        if not data:
            raise ValueError("Cannot create table from empty data")
        
        # Initialize columns
        columns = []
        
        # Sample record to determine column types
        sample = data[0]
        
        # Map Python types to SQLAlchemy types
        for column_name, value in sample.items():
            if isinstance(value, int):
                column = Column(column_name, Integer)
            elif isinstance(value, float):
                column = Column(column_name, Float)
            elif isinstance(value, bool):
                column = Column(column_name, Boolean)
            elif isinstance(value, (list, dict)):
                column = Column(column_name, Text)  # Store as JSON string
            else:
                column = Column(column_name, String(255))
            
            columns.append(column)
        
        # Create table
        table = Table(table_name, self.metadata, *columns, schema=schema)
        
        return table