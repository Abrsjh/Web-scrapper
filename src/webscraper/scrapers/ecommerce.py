"""
E-commerce Scraper Module

This module implements a scraper for e-commerce websites.
It can extract product information like names, prices, availability,
images, and reviews.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Union

import bs4
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from .base import BaseScraper
from ..utils.exceptions import ScraperError, ValidationError
from ..utils.user_agents import get_random_user_agent


class EcommerceScraper(BaseScraper):
    """
    Scraper for e-commerce websites.
    
    This scraper can extract product information from e-commerce
    websites, including product names, prices, availability,
    images, and reviews.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Initialize the e-commerce scraper.
        
        Extends the BaseScraper initialization with e-commerce specific options.
        """
        super().__init__(*args, **kwargs)
        
        # E-commerce specific options
        self.currency_symbol = kwargs.get("currency_symbol", "$")
        self.price_regex = kwargs.get("price_regex", r"\d+\.\d{2}")
        self.extract_reviews = kwargs.get("extract_reviews", True)
        self.extract_images = kwargs.get("extract_images", True)
        
        # Initialize session
        self.session = requests.Session()
        
        # Set up user agent if not provided
        if not self.user_agent:
            self.user_agent = get_random_user_agent()
            
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        
        if self.proxy:
            self.session.proxies = {
                "http": self.proxy,
                "https": self.proxy,
            }
            
        if self.headers:
            self.session.headers.update(self.headers)
            
        if self.cookies:
            self.session.cookies.update(self.cookies)
    
    @retry(
        retry=retry_if_exception_type(RequestException),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_page(self, url: str) -> str:
        """
        Fetch a page from a URL with retry logic.
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content of the page
            
        Raises:
            ScraperError: If the page fetch fails after retries
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            self.logger.error(f"Error fetching URL {url}: {str(e)}")
            raise ScraperError(f"Failed to fetch URL {url}: {str(e)}") from e
    
    def extract_data(self, url: str) -> List[Dict[str, Any]]:
        """
        Extract product data from an e-commerce URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of extracted product data
            
        Raises:
            ScraperError: If data extraction fails
        """
        try:
            html_content = self._fetch_page(url)
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Find product listings
            product_elements = self._find_product_elements(soup)
            
            if not product_elements:
                self.logger.warning(f"No product elements found on {url}")
                return []
            
            # Extract data from each product element
            products = []
            for element in product_elements:
                try:
                    product_data = self._extract_item(element)
                    if product_data:
                        products.append(product_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting product data: {str(e)}")
            
            return products
        
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {str(e)}")
            raise ScraperError(f"Failed to extract data from {url}: {str(e)}") from e
    
    def _find_product_elements(self, soup: BeautifulSoup) -> List[bs4.element.Tag]:
        """
        Find product elements in the HTML.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of BeautifulSoup elements representing products
        """
        # Look for common product container selectors
        selectors = [
            self.selectors.get("product_container"),
            ".product",
            ".product-item",
            ".item",
            "[data-product-id]",
            ".product-card",
            ".product-grid-item",
        ]
        
        # Try each selector until we find products
        for selector in selectors:
            if not selector:
                continue
                
            elements = soup.select(selector)
            if elements:
                return elements
        
        # If no products found with CSS selectors, try to find them using other attributes
        # This is a fallback for sites with non-standard markup
        candidates = soup.find_all(["div", "li"], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ["product", "item", "card"]
        ))
        
        if candidates:
            return candidates
            
        # Last resort: check for common product patterns
        possible_products = []
        for element in soup.find_all(["div", "li"]):
            # Check if element contains both price and product name/link
            has_price = element.find(text=re.compile(r'(\$|€|£)\s*\d+\.?\d*'))
            has_product = (
                element.find("h2") or
                element.find("h3") or
                element.find("a", href=True)
            )
            
            if has_price and has_product:
                possible_products.append(element)
        
        return possible_products
    
    def _extract_item(self, element: bs4.element.Tag) -> Dict[str, Any]:
        """
        Extract data from a single product element.
        
        Args:
            element: BeautifulSoup element representing a product
            
        Returns:
            Extracted product data as a dictionary
        """
        product = {}
        
        # Extract product name
        product["name"] = self._extract_product_name(element)
        
        # Skip if no product name found (likely not a product)
        if not product["name"]:
            return {}
        
        # Extract price
        product["price"] = self._extract_price(element)
        
        # Extract currency
        product["currency"] = self._extract_currency(element)
        
        # Extract URL
        product["url"] = self._extract_url(element)
        
        # Extract availability
        product["availability"] = self._extract_availability(element)
        
        # Extract images if configured
        if self.extract_images:
            product["images"] = self._extract_images(element)
        
        # Extract reviews if configured
        if self.extract_reviews:
            product["reviews"] = self._extract_reviews(element)
        
        # Extract additional fields defined in selectors
        for field, selector in self.selectors.items():
            if field not in ["product_container"] and field not in product:
                try:
                    product[field] = self._extract_field(element, selector)
                except Exception as e:
                    self.logger.debug(f"Error extracting field {field}: {str(e)}")
        
        return product
    
    def _extract_product_name(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the product name from an element."""
        # Try using the provided selector
        if "name" in self.selectors:
            name_element = element.select_one(self.selectors["name"])
            if name_element:
                return name_element.get_text(strip=True)
        
        # Try common product name patterns
        for selector in [
            "h1", "h2", "h3", 
            ".product-name", ".product-title", 
            "[itemprop='name']",
            ".title", ".name"
        ]:
            name_element = element.select_one(selector)
            if name_element:
                return name_element.get_text(strip=True)
        
        # Try to get text from a product link
        link = element.find("a")
        if link and link.get_text(strip=True):
            return link.get_text(strip=True)
        
        return None
    
    def _extract_price(self, element: bs4.element.Tag) -> Optional[float]:
        """Extract the product price from an element."""
        # Try using the provided selector
        if "price" in self.selectors:
            price_element = element.select_one(self.selectors["price"])
            if price_element:
                price_text = price_element.get_text(strip=True)
                return self._parse_price(price_text)
        
        # Try common price patterns
        for selector in [
            ".price", ".product-price", 
            "[itemprop='price']",
            ".price-current", ".price-new", 
            ".current-price"
        ]:
            price_element = element.select_one(selector)
            if price_element:
                price_text = price_element.get_text(strip=True)
                return self._parse_price(price_text)
        
        # Try to find price in any text
        price_regex = re.compile(
            r'(\$|€|£)?\s*(\d+[.,]\d{2}|\d+)\s*(\$|€|£)?',
            re.IGNORECASE
        )
        price_match = element.find(text=price_regex)
        
        if price_match:
            return self._parse_price(price_match)
        
        return None
    
    def _parse_price(self, price_text: str) -> Optional[float]:
        """Parse a price string into a float."""
        if not price_text:
            return None
            
        # Extract digits and decimal point
        price_match = re.search(
            r'(\d+[.,]\d{2}|\d+)',
            price_text
        )
        
        if price_match:
            price_str = price_match.group(0)
            # Replace comma with dot for proper float conversion
            price_str = price_str.replace(",", ".")
            try:
                return float(price_str)
            except ValueError:
                return None
                
        return None
    
    def _extract_currency(self, element: bs4.element.Tag) -> str:
        """Extract the currency symbol from an element."""
        # Try using the provided selector
        if "currency" in self.selectors:
            currency_element = element.select_one(self.selectors["currency"])
            if currency_element:
                currency_text = currency_element.get_text(strip=True)
                currency_match = re.search(r'(\$|€|£|USD|EUR|GBP)', currency_text)
                if currency_match:
                    return currency_match.group(0)
        
        # Look for currency in price text
        price_element = None
        if "price" in self.selectors:
            price_element = element.select_one(self.selectors["price"])
        
        if not price_element:
            price_element = element.select_one(".price, .product-price, [itemprop='price']")
            
        if price_element:
            price_text = price_element.get_text(strip=True)
            currency_match = re.search(r'(\$|€|£|USD|EUR|GBP)', price_text)
            if currency_match:
                return currency_match.group(0)
        
        # Default to the configured currency symbol
        return self.currency_symbol
    
    def _extract_url(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the product URL from an element."""
        # Try using the provided selector
        if "url" in self.selectors:
            url_element = element.select_one(self.selectors["url"])
            if url_element and url_element.has_attr("href"):
                return url_element["href"]
        
        # Try to find any link
        link = element.find("a", href=True)
        if link:
            return link["href"]
            
        return None
    
    def _extract_availability(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the product availability from an element."""
        # Try using the provided selector
        if "availability" in self.selectors:
            avail_element = element.select_one(self.selectors["availability"])
            if avail_element:
                return avail_element.get_text(strip=True)
        
        # Try common availability patterns
        for selector in [
            "[itemprop='availability']",
            ".availability",
            ".stock-status",
            ".in-stock",
            ".out-of-stock"
        ]:
            avail_element = element.select_one(selector)
            if avail_element:
                return avail_element.get_text(strip=True)
        
        # Look for common availability text patterns
        avail_patterns = [
            (r"in\s*stock", "In Stock"),
            (r"out\s*of\s*stock", "Out of Stock"),
            (r"available", "Available"),
            (r"unavailable", "Unavailable"),
        ]
        
        for pattern, status in avail_patterns:
            avail_match = element.find(text=re.compile(pattern, re.IGNORECASE))
            if avail_match:
                return status
                
        return "Unknown"
    
    def _extract_images(self, element: bs4.element.Tag) -> List[str]:
        """Extract product images from an element."""
        images = []
        
        # Try using the provided selector
        if "images" in self.selectors:
            img_elements = element.select(self.selectors["images"])
            if img_elements:
                for img in img_elements:
                    if img.has_attr("src"):
                        images.append(img["src"])
                    elif img.has_attr("data-src"):
                        images.append(img["data-src"])
        
        # If no images found, try common image patterns
        if not images:
            img_elements = element.select("img")
            for img in img_elements:
                src = None
                # Check various common image attributes
                for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                    if img.has_attr(attr):
                        src = img[attr]
                        break
                
                if src and not src.startswith("data:"):
                    images.append(src)
        
        return images
    
    def _extract_reviews(self, element: bs4.element.Tag) -> Optional[Dict[str, Any]]:
        """Extract product reviews from an element."""
        reviews = {}
        
        # Try to extract rating
        if "rating" in self.selectors:
            rating_element = element.select_one(self.selectors["rating"])
            if rating_element:
                reviews["rating"] = self._parse_rating(rating_element.get_text(strip=True))
        
        # Try common rating patterns if not found
        if "rating" not in reviews:
            for selector in [
                "[itemprop='ratingValue']",
                ".rating",
                ".stars",
                ".star-rating"
            ]:
                rating_element = element.select_one(selector)
                if rating_element:
                    if rating_element.has_attr("style"):
                        # Parse ratings from style attributes (e.g., width: 80%)
                        style = rating_element["style"]
                        pct_match = re.search(r"(\d+)%", style)
                        if pct_match:
                            pct = int(pct_match.group(1))
                            reviews["rating"] = round((pct / 100) * 5, 1)
                    else:
                        reviews["rating"] = self._parse_rating(rating_element.get_text(strip=True))
        
        # Try to extract review count
        if "review_count" in self.selectors:
            count_element = element.select_one(self.selectors["review_count"])
            if count_element:
                count_text = count_element.get_text(strip=True)
                count_match = re.search(r'(\d+)', count_text)
                if count_match:
                    reviews["count"] = int(count_match.group(1))
        
        # Try common review count patterns if not found
        if "count" not in reviews:
            for selector in [
                "[itemprop='reviewCount']",
                ".review-count",
                ".ratings-count"
            ]:
                count_element = element.select_one(selector)
                if count_element:
                    count_text = count_element.get_text(strip=True)
                    count_match = re.search(r'(\d+)', count_text)
                    if count_match:
                        reviews["count"] = int(count_match.group(1))
        
        return reviews if reviews else None
    
    def _parse_rating(self, rating_text: str) -> Optional[float]:
        """Parse a rating string into a float."""
        if not rating_text:
            return None
        
        # Try to extract a float from the text
        rating_match = re.search(r'(\d+\.\d+|\d+)', rating_text)
        if rating_match:
            try:
                rating = float(rating_match.group(1))
                
                # Check if rating is out of 10 and convert to out of 5
                if rating > 5 and rating <= 10:
                    rating /= 2
                
                return round(rating, 1)
            except ValueError:
                pass
        
        # Look for X/Y pattern (e.g., 4/5)
        fraction_match = re.search(r'(\d+)\s*/\s*(\d+)', rating_text)
        if fraction_match:
            try:
                numerator = float(fraction_match.group(1))
                denominator = float(fraction_match.group(2))
                if denominator > 0:
                    rating = numerator / denominator * 5
                    return round(rating, 1)
            except ValueError:
                pass
        
        # Count stars (e.g., ★★★☆☆)
        star_count = rating_text.count("★")
        if star_count > 0:
            return star_count
            
        return None
    
    def _extract_field(self, element: bs4.element.Tag, selector: str) -> Optional[str]:
        """Extract a field using a CSS selector or XPath expression."""
        if not selector:
            return None
            
        # Check if selector is XPath (starts with /)
        if selector.startswith("/"):
            # BeautifulSoup doesn't support XPath directly, would need lxml for this
            # For simplicity, we'll just use CSS selectors in this implementation
            self.logger.warning("XPath selectors not supported in this implementation")
            return None
            
        # Use CSS selector
        field_element = element.select_one(selector)
        if field_element:
            return field_element.get_text(strip=True)
            
        return None
    
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the extracted product data.
        
        This method can perform additional transformations specific to
        e-commerce product data.
        
        Args:
            data: Raw extracted product data
            
        Returns:
            Transformed product data
        """
        transformed_data = []
        
        for product in data:
            # Skip empty products
            if not product or not product.get("name"):
                continue
                
            # Convert price strings to float if needed
            if isinstance(product.get("price"), str):
                product["price"] = self._parse_price(product["price"])
            
            # Clean up product name
            if product.get("name"):
                product["name"] = product["name"].strip()
                
            # Make sure URLs are absolute
            if product.get("url") and not product["url"].startswith(("http://", "https://")):
                # Check if it's a root-relative URL
                if product["url"].startswith("/"):
                    # Try to get the base URL from the source URL
                    if self.urls and self.urls[0]:
                        from urllib.parse import urlparse
                        parsed_url = urlparse(self.urls[0])
                        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
                        product["url"] = f"{base_url}{product['url']}"
                        
            # Add timestamp
            from datetime import datetime
            product["scraped_at"] = datetime.now().isoformat()
            
            transformed_data.append(product)
            
        return transformed_data
    
    def validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate the product data.
        
        Args:
            data: Data to validate
            
        Returns:
            Validated data
        """
        validated_data = []
        
        for product in data:
            # Validate required fields
            if not product.get("name"):
                self.logger.warning("Skipping product without a name")
                continue
                
            # Add default values for missing fields
            if "price" not in product:
                product["price"] = None
                
            if "currency" not in product:
                product["currency"] = self.currency_symbol
                
            if "availability" not in product:
                product["availability"] = "Unknown"
                
            if "images" not in product:
                product["images"] = []
                
            validated_data.append(product)
            
        return validated_data