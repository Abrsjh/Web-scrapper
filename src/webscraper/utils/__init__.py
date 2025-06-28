"""
Utils Package

This package contains utility modules for the web scraper.
"""

from .exceptions import (
    WebScraperError, ScraperError, StorageError, ValidationError,
    ConfigError, ConnectionError, RateLimitError, ParsingError,
    CaptchaError, ProxyError, AuthenticationError, SchedulingError,
    DataProcessingError
)

from .user_agents import (
    get_random_user_agent, get_next_user_agent, add_user_agent,
    set_user_agents, reset_user_agents, UserAgentManager
)

from .validators import (
    is_valid_email, is_valid_phone, is_valid_url, is_valid_date,
    is_valid_ip, is_valid_credit_card, is_valid_text, is_valid_json,
    is_valid_numeric
)

from .text import (
    clean_text, clean_html, summarize_text, extract_keywords,
    extract_entities, word_count, estimate_reading_time
)

from .config import (
    ConfigManager, load_config_file, create_default_config
)

# Import logging configuration
from .logging import (
    configure_logging, get_logger, configure_logging_from_dict, 
    get_default_logging_config
)

__all__ = [
    # Exceptions
    'WebScraperError', 'ScraperError', 'StorageError', 'ValidationError',
    'ConfigError', 'ConnectionError', 'RateLimitError', 'ParsingError',
    'CaptchaError', 'ProxyError', 'AuthenticationError', 'SchedulingError',
    'DataProcessingError',
    
    # User Agents
    'get_random_user_agent', 'get_next_user_agent', 'add_user_agent',
    'set_user_agents', 'reset_user_agents', 'UserAgentManager',
    
    # Validators
    'is_valid_email', 'is_valid_phone', 'is_valid_url', 'is_valid_date',
    'is_valid_ip', 'is_valid_credit_card', 'is_valid_text', 'is_valid_json',
    'is_valid_numeric',
    
    # Text Processing
    'clean_text', 'clean_html', 'summarize_text', 'extract_keywords',
    'extract_entities', 'word_count', 'estimate_reading_time',
    
    # Configuration
    'ConfigManager', 'load_config_file', 'create_default_config',
    
    # Logging
    'configure_logging', 'get_logger', 'configure_logging_from_dict',
    'get_default_logging_config'
]