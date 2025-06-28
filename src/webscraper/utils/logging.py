"""
Logging Module

This module provides logging configuration for the web scraper.
"""

import logging
import logging.config
import logging.handlers
import os
import sys
from datetime import datetime
from typing import Dict, Optional, Union


def configure_logging(
    level: str = "INFO",
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
    log_to_console: bool = True,
    log_to_file: bool = False,
    rotate_logs: bool = False,
    max_log_size: int = 10485760,  # 10MB
    backup_count: int = 5,
    logger_name: Optional[str] = None
) -> logging.Logger:
    """
    Configure logging for the web scraper.
    
    Args:
        level: Logging level (default: "INFO")
        log_file: Path to log file (default: None)
        log_format: Log format string (default: None)
        log_to_console: Whether to log to console (default: True)
        log_to_file: Whether to log to file (default: False)
        rotate_logs: Whether to rotate log files (default: False)
        max_log_size: Maximum log file size in bytes (default: 10MB)
        backup_count: Number of backup log files to keep (default: 5)
        logger_name: Name of the logger to configure (default: None)
        
    Returns:
        Configured logger
    """
    # Determine log file path if not provided
    if log_to_file and not log_file:
        home_dir = os.path.expanduser("~")
        logs_dir = os.path.join(home_dir, ".webscraper", "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"webscraper_{timestamp}.log")
    
    # Set default log format if not provided
    if not log_format:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Create formatter
    formatter = logging.Formatter(log_format)
    
    # Configure root logger by default
    logger = logging.getLogger(logger_name)
    
    # Convert level string to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    logger.setLevel(numeric_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Add console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # Add file handler
    if log_to_file and log_file:
        # Ensure directory exists
        log_dir = os.path.dirname(os.path.abspath(log_file))
        os.makedirs(log_dir, exist_ok=True)
        
        if rotate_logs:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_log_size,
                backupCount=backup_count
            )
        else:
            file_handler = logging.FileHandler(log_file)
        
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation if this is not the root logger
    if logger_name:
        logger.propagate = False
    
    return logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Name of the logger
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def configure_logging_from_dict(config: Dict[str, Union[str, Dict]]) -> None:
    """
    Configure logging using a dictionary configuration.
    
    Args:
        config: Logging configuration dictionary
    """
    logging.config.dictConfig(config)


def get_default_logging_config() -> Dict[str, Union[str, Dict]]:
    """
    Get a default logging configuration dictionary.
    
    Returns:
        Default logging configuration
    """
    home_dir = os.path.expanduser("~")
    logs_dir = os.path.join(home_dir, ".webscraper", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(logs_dir, f"webscraper_{timestamp}.log")
    
    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
            "detailed": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "standard",
                "stream": "ext://sys.stdout"
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "formatter": "detailed",
                "filename": log_file,
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8"
            },
            "error_file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "ERROR",
                "formatter": "detailed",
                "filename": os.path.join(logs_dir, f"webscraper_error_{timestamp}.log"),
                "maxBytes": 10485760,  # 10MB
                "backupCount": 5,
                "encoding": "utf8"
            }
        },
        "loggers": {
            "webscraper": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            },
            "webscraper.scrapers": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            },
            "webscraper.storage": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            },
            "webscraper.cli": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            },
            "webscraper.utils": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            },
            "webscraper.schedulers": {
                "level": "DEBUG",
                "handlers": ["console", "file", "error_file"],
                "propagate": False
            }
        },
        "root": {
            "level": "WARNING",
            "handlers": ["console", "file", "error_file"]
        }
    }
    
    return config