"""
Base Scraper Module

This module defines the abstract base classes for all scraper implementations.
It provides the contract that all concrete scrapers must follow.
"""

import abc
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union

from ..utils.exceptions import ScraperError, ValidationError


class BaseScraper(abc.ABC):
    """
    Abstract base class for all scrapers.
    
    This class defines the interface that all scrapers must implement.
    It provides common functionality for scraping operations, including
    setup, extraction, validation, transformation, and storage.
    """
    
    def __init__(
        self,
        urls: List[str],
        selectors: Dict[str, str],
        output_config: Dict[str, Any],
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
        timeout: int = 30,
        retries: int = 3,
        delay: int = 2,
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        max_concurrent: int = 5,
    ):
        """
        Initialize the base scraper with common configuration.
        
        Args:
            urls: List of URLs to scrape
            selectors: Dictionary mapping data fields to CSS selectors or XPath expressions
            output_config: Configuration for data storage
            user_agent: User agent string to use for requests
            proxy: Proxy URL to use for requests
            timeout: Request timeout in seconds
            retries: Number of retry attempts for failed requests
            delay: Delay between requests in seconds
            headers: Additional HTTP headers to include with requests
            cookies: Cookies to include with requests
            max_concurrent: Maximum number of concurrent requests
        """
        self.urls = urls
        self.selectors = selectors
        self.output_config = output_config
        self.user_agent = user_agent
        self.proxy = proxy
        self.timeout = timeout
        self.retries = retries
        self.delay = delay
        self.headers = headers or {}
        self.cookies = cookies or {}
        self.max_concurrent = max_concurrent
        
        self.results = []
        self.failed_urls = set()
        self.visited_urls = set()
        self.start_time = None
        self.end_time = None
        
        self.logger = logging.getLogger(__name__)
        
        # Validate configuration
        self._validate_config()
    
    def _validate_config(self) -> None:
        """
        Validate the scraper configuration.
        
        Raises:
            ValidationError: If the configuration is invalid
        """
        if not self.urls:
            raise ValidationError("No URLs provided for scraping")
        
        if not self.selectors:
            raise ValidationError("No selectors provided for data extraction")
        
        if not self.output_config.get("format"):
            raise ValidationError("Output format not specified")
    
    @abc.abstractmethod
    def extract_data(self, url: str) -> List[Dict[str, Any]]:
        """
        Extract data from a single URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of extracted data items as dictionaries
            
        Raises:
            ScraperError: If data extraction fails
        """
        pass
    
    @abc.abstractmethod
    def _extract_item(self, element: Any) -> Dict[str, Any]:
        """
        Extract data from a single element.
        
        Args:
            element: Element to extract data from
            
        Returns:
            Extracted data as a dictionary
        """
        pass
    
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the extracted data.
        
        This method can be overridden by subclasses to implement
        custom data transformation logic.
        
        Args:
            data: Raw extracted data
            
        Returns:
            Transformed data
        """
        return data
    
    def validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate the extracted data.
        
        This method can be overridden by subclasses to implement
        custom data validation logic.
        
        Args:
            data: Data to validate
            
        Returns:
            Validated data
            
        Raises:
            ValidationError: If data validation fails
        """
        return data
    
    def save_data(self, data: List[Dict[str, Any]], format: str, path: str) -> None:
        """
        Save the extracted data to the specified format.
        
        Args:
            data: Data to save
            format: Output format (csv, json, excel, db)
            path: Output path
            
        Raises:
            ValueError: If the output format is not supported
        """
        from ..storage import get_storage_handler
        
        handler = get_storage_handler(format)
        handler.save(data, path, **self.output_config.get("options", {}))
    
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Execute the scraping operation.
        
        Returns:
            List of extracted and processed data items
            
        Raises:
            ScraperError: If the scraping operation fails
        """
        self.start_time = datetime.now()
        self.logger.info(f"Starting scraping job with {len(self.urls)} URLs")
        
        all_data = []
        
        for url in self.urls:
            try:
                self.logger.info(f"Scraping URL: {url}")
                data = self.extract_data(url)
                transformed_data = self.transform_data(data)
                validated_data = self.validate_data(transformed_data)
                all_data.extend(validated_data)
                self.visited_urls.add(url)
                self.logger.info(f"Successfully scraped {len(validated_data)} items from {url}")
            except Exception as e:
                self.logger.error(f"Error scraping URL {url}: {str(e)}")
                self.failed_urls.add(url)
        
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        self.logger.info(f"Scraping job completed in {duration:.2f} seconds")
        self.logger.info(f"Visited {len(self.visited_urls)} URLs, failed {len(self.failed_urls)} URLs")
        self.logger.info(f"Extracted {len(all_data)} items")
        
        # Save the data if output is configured
        if all_data and self.output_config:
            format = self.output_config.get("format")
            path = self.output_config.get("path")
            if format and path:
                self.save_data(all_data, format, path)
                self.logger.info(f"Saved data to {path} in {format} format")
        
        self.results = all_data
        return all_data
    
    def get_report(self) -> Dict[str, Any]:
        """
        Generate a report of the scraping operation.
        
        Returns:
            Dictionary containing scraping metrics and stats
        """
        if not self.start_time:
            return {"status": "Not started"}
        
        duration = (self.end_time or datetime.now()) - self.start_time
        
        return {
            "status": "Completed" if self.end_time else "In progress",
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration.total_seconds(),
            "urls_total": len(self.urls),
            "urls_visited": len(self.visited_urls),
            "urls_failed": len(self.failed_urls),
            "items_extracted": len(self.results),
        }
    
    def __repr__(self) -> str:
        """String representation of the scraper."""
        return f"{self.__class__.__name__}(urls={len(self.urls)})"