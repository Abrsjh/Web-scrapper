"""
Web Scraper Pro - A professional-grade web scraping and automation tool.

This package provides a comprehensive set of tools for web scraping,
data extraction, processing, and storage with features like concurrency,
scheduling, error handling, and multiple output formats.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Import core components for easy access
from .scrapers import BaseScraper, EcommerceScraper, BusinessDirectoryScraper, ContentScraper, get_scraper
from .storage import get_storage_handler
from .utils import (
    configure_logging, get_logger, ConfigManager, load_config_file, create_default_config,
    WebScraperError, ScraperError, StorageError, ValidationError, ConfigError
)
from .schedulers import JobScheduler

# Set up default logging
configure_logging()

# Expose main entry point
from .cli import main

__all__ = [
    # Version info
    '__version__', '__author__', '__email__',
    
    # Scrapers
    'BaseScraper', 'EcommerceScraper', 'BusinessDirectoryScraper', 'ContentScraper', 'get_scraper',
    
    # Storage
    'get_storage_handler',
    
    # Utils
    'configure_logging', 'get_logger', 'ConfigManager', 'load_config_file', 'create_default_config',
    
    # Exceptions
    'WebScraperError', 'ScraperError', 'StorageError', 'ValidationError', 'ConfigError',
    
    # Schedulers
    'JobScheduler',
    
    # CLI
    'main'
]