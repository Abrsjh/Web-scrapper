"""
Exceptions Module

This module defines custom exceptions for the web scraper.
"""


class WebScraperError(Exception):
    """Base exception for all web scraper errors."""
    pass


class ScraperError(WebScraperError):
    """Exception raised for errors during scraping operations."""
    pass


class StorageError(WebScraperError):
    """Exception raised for errors during data storage operations."""
    pass


class ValidationError(WebScraperError):
    """Exception raised for errors during data validation."""
    pass


class ConfigError(WebScraperError):
    """Exception raised for errors in configuration."""
    pass


class ConnectionError(WebScraperError):
    """Exception raised for network connection errors."""
    pass


class RateLimitError(WebScraperError):
    """Exception raised when a rate limit is exceeded."""
    pass


class ParsingError(WebScraperError):
    """Exception raised for errors during parsing operations."""
    pass


class CaptchaError(WebScraperError):
    """Exception raised when a captcha is encountered."""
    pass


class ProxyError(WebScraperError):
    """Exception raised for errors with proxy servers."""
    pass


class AuthenticationError(WebScraperError):
    """Exception raised for authentication errors."""
    pass


class SchedulingError(WebScraperError):
    """Exception raised for errors during job scheduling."""
    pass


class DataProcessingError(WebScraperError):
    """Exception raised for errors during data processing."""
    pass