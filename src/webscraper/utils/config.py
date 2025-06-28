"""
Configuration Module

This module provides utilities for loading and parsing configuration files.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Union

import yaml

from .exceptions import ConfigError

# Logger
logger = logging.getLogger(__name__)


class ConfigManager:
    """
    Configuration manager for the web scraper.
    
    This class handles loading, parsing, and validating configuration files.
    It supports both YAML and JSON formats.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file (default: None)
        """
        self.config = {}
        self.config_path = None
        
        if config_path:
            self.load_config(config_path)
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load a configuration file.
        
        Args:
            config_path: Path to the configuration file
            
        Returns:
            Loaded configuration as a dictionary
            
        Raises:
            ConfigError: If the configuration file cannot be loaded
        """
        if not os.path.exists(config_path):
            error_msg = f"Configuration file not found: {config_path}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        
        try:
            # Determine file format from extension
            _, ext = os.path.splitext(config_path)
            ext = ext.lower()
            
            if ext in ['.yml', '.yaml']:
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = yaml.safe_load(f)
            elif ext == '.json':
                with open(config_path, 'r', encoding='utf-8') as f:
                    self.config = json.load(f)
            else:
                error_msg = f"Unsupported configuration format: {ext}"
                logger.error(error_msg)
                raise ConfigError(error_msg)
            
            self.config_path = config_path
            
            # Validate the configuration
            self._validate_config()
            
            logger.info(f"Loaded configuration from {config_path}")
            return self.config
            
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            
            error_msg = f"Failed to load configuration from {config_path}: {str(e)}"
            logger.error(error_msg)
            raise ConfigError(error_msg) from e
    
    def _validate_config(self) -> None:
        """
        Validate the configuration.
        
        Raises:
            ConfigError: If the configuration is invalid
        """
        # Check if the configuration is empty
        if not self.config:
            error_msg = "Configuration is empty"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        
        # Check for required sections
        required_sections = ['scraper']
        for section in required_sections:
            if section not in self.config:
                error_msg = f"Missing required configuration section: {section}"
                logger.error(error_msg)
                raise ConfigError(error_msg)
        
        # Validate scraper configuration
        scraper_config = self.config['scraper']
        
        # Check for required scraper fields
        required_fields = ['type', 'urls']
        for field in required_fields:
            if field not in scraper_config:
                error_msg = f"Missing required field in scraper configuration: {field}"
                logger.error(error_msg)
                raise ConfigError(error_msg)
        
        # Check scraper type
        scraper_type = scraper_config['type']
        valid_types = ['ecommerce', 'business', 'content']
        if scraper_type not in valid_types:
            error_msg = f"Invalid scraper type: {scraper_type}. Valid types: {valid_types}"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        
        # Check URLs
        urls = scraper_config['urls']
        if not isinstance(urls, list) or not urls:
            error_msg = "URLs must be a non-empty list"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        
        # Validate output configuration if present
        if 'output' in scraper_config:
            output_config = scraper_config['output']
            
            if 'format' not in output_config:
                error_msg = "Missing required field in output configuration: format"
                logger.error(error_msg)
                raise ConfigError(error_msg)
            
            output_format = output_config['format']
            valid_formats = ['csv', 'json', 'excel', 'db', 'database', 'xlsx', 'sqlite', 'postgresql', 'postgres', 'mysql']
            if output_format not in valid_formats:
                error_msg = f"Invalid output format: {output_format}. Valid formats: {valid_formats}"
                logger.error(error_msg)
                raise ConfigError(error_msg)
            
            if 'path' not in output_config and output_format not in ['database', 'db', 'sqlite', 'postgresql', 'postgres', 'mysql']:
                error_msg = "Missing required field in output configuration: path"
                logger.error(error_msg)
                raise ConfigError(error_msg)
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration.
        
        Returns:
            Current configuration as a dictionary
        """
        return self.config
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value.
        
        Args:
            key: Configuration key (dot notation for nested keys)
            default: Default value if key is not found
            
        Returns:
            Configuration value or default
        """
        if not self.config:
            return default
        
        # Handle nested keys with dot notation
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Set a configuration value.
        
        Args:
            key: Configuration key (dot notation for nested keys)
            value: Value to set
        """
        if not self.config:
            self.config = {}
        
        # Handle nested keys with dot notation
        keys = key.split('.')
        config = self.config
        
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                # Last key, set the value
                config[k] = value
            else:
                # Intermediate key, ensure path exists
                if k not in config or not isinstance(config[k], dict):
                    config[k] = {}
                config = config[k]
    
    def save_config(self, config_path: Optional[str] = None) -> None:
        """
        Save the configuration to a file.
        
        Args:
            config_path: Path to save the configuration (default: current config path)
            
        Raises:
            ConfigError: If the configuration cannot be saved
        """
        if not config_path:
            config_path = self.config_path
        
        if not config_path:
            error_msg = "No configuration path specified"
            logger.error(error_msg)
            raise ConfigError(error_msg)
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(config_path)), exist_ok=True)
            
            # Determine file format from extension
            _, ext = os.path.splitext(config_path)
            ext = ext.lower()
            
            if ext in ['.yml', '.yaml']:
                with open(config_path, 'w', encoding='utf-8') as f:
                    yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            elif ext == '.json':
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, indent=2)
            else:
                error_msg = f"Unsupported configuration format: {ext}"
                logger.error(error_msg)
                raise ConfigError(error_msg)
            
            logger.info(f"Saved configuration to {config_path}")
            
        except Exception as e:
            if isinstance(e, ConfigError):
                raise
            
            error_msg = f"Failed to save configuration to {config_path}: {str(e)}"
            logger.error(error_msg)
            raise ConfigError(error_msg) from e


def load_config_file(config_path: str) -> Dict[str, Any]:
    """
    Load a configuration file.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Loaded configuration as a dictionary
        
    Raises:
        ConfigError: If the configuration file cannot be loaded
    """
    manager = ConfigManager()
    return manager.load_config(config_path)


def create_default_config(config_path: str, scraper_type: str = 'ecommerce') -> Dict[str, Any]:
    """
    Create a default configuration file.
    
    Args:
        config_path: Path to save the configuration
        scraper_type: Type of scraper to configure (default: 'ecommerce')
        
    Returns:
        Created configuration as a dictionary
        
    Raises:
        ConfigError: If the configuration cannot be created
    """
    # Create default configuration based on scraper type
    if scraper_type == 'ecommerce':
        config = {
            'scraper': {
                'type': 'ecommerce',
                'urls': ['https://example.com/products'],
                'selectors': {
                    'product_container': '.product',
                    'name': '.product-title',
                    'price': '.price',
                    'availability': '.availability',
                    'images': '.product-image img',
                    'rating': '.rating',
                    'review_count': '.review-count'
                },
                'output': {
                    'format': 'csv',
                    'path': './data/products.csv'
                },
                'delay': 2,
                'retries': 3,
                'timeout': 30,
                'extract_reviews': True,
                'extract_images': True
            }
        }
    elif scraper_type == 'business':
        config = {
            'scraper': {
                'type': 'business',
                'urls': ['https://example.com/businesses'],
                'selectors': {
                    'business_container': '.business',
                    'name': '.business-name',
                    'address': '.address',
                    'phone': '.phone',
                    'email': '.email',
                    'website': '.website',
                    'categories': '.category'
                },
                'output': {
                    'format': 'json',
                    'path': './data/businesses.json'
                },
                'delay': 2,
                'retries': 3,
                'timeout': 30,
                'extract_social_media': True,
                'validate_emails': True,
                'validate_phones': True
            }
        }
    elif scraper_type == 'content':
        config = {
            'scraper': {
                'type': 'content',
                'urls': ['https://example.com/blog'],
                'selectors': {
                    'article_container': '.article',
                    'title': '.article-title',
                    'date': '.article-date',
                    'author': '.article-author',
                    'content': '.article-content',
                    'excerpt': '.article-excerpt',
                    'image': '.article-image img',
                    'categories': '.article-category'
                },
                'output': {
                    'format': 'excel',
                    'path': './data/articles.xlsx'
                },
                'delay': 2,
                'retries': 3,
                'timeout': 30,
                'extract_images': True,
                'extract_metadata': True,
                'generate_summary': True,
                'follow_next_page': True,
                'max_pages': 5
            }
        }
    else:
        error_msg = f"Invalid scraper type: {scraper_type}"
        logger.error(error_msg)
        raise ConfigError(error_msg)
    
    # Save the configuration
    manager = ConfigManager()
    manager.config = config
    manager.save_config(config_path)
    
    logger.info(f"Created default {scraper_type} configuration at {config_path}")
    return config