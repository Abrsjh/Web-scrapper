"""
Scrapers Package

This package contains all the scraper implementations.
"""

from .base import BaseScraper
from .ecommerce import EcommerceScraper
from .business import BusinessDirectoryScraper
from .content import ContentScraper

# Factory function to get the appropriate scraper
def get_scraper(scraper_type, config):
    """
    Factory function to get a scraper instance based on type.
    
    Args:
        scraper_type (str): Type of scraper ('ecommerce', 'business', 'content')
        config (dict): Scraper configuration
        
    Returns:
        BaseScraper: An instance of the appropriate scraper
        
    Raises:
        ValueError: If the scraper type is not supported
    """
    scrapers = {
        'ecommerce': EcommerceScraper,
        'business': BusinessDirectoryScraper,
        'content': ContentScraper,
    }
    
    if scraper_type not in scrapers:
        raise ValueError(f"Unsupported scraper type: {scraper_type}. "
                         f"Supported types: {list(scrapers.keys())}")
    
    return scrapers[scraper_type](
        urls=config.get('urls', []),
        selectors=config.get('selectors', {}),
        output_config=config.get('output', {}),
        user_agent=config.get('user_agent'),
        proxy=config.get('proxy'),
        timeout=config.get('timeout', 30),
        retries=config.get('retries', 3),
        delay=config.get('delay', 2),
        headers=config.get('headers', {}),
        cookies=config.get('cookies', {}),
        max_concurrent=config.get('max_concurrent', 5),
    )