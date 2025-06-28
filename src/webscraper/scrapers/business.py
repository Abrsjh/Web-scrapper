"""
Business Directory Scraper Module

This module implements a scraper for business directories.
It can extract business information such as names, addresses,
phone numbers, emails, and websites.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urljoin, urlparse

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
from ..utils.validators import is_valid_email, is_valid_phone, is_valid_url


class BusinessDirectoryScraper(BaseScraper):
    """
    Scraper for business directories.
    
    This scraper can extract business contact information from directory
    websites, including business names, phone numbers, email addresses,
    physical addresses, and websites.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Initialize the business directory scraper.
        
        Extends the BaseScraper initialization with business directory specific options.
        """
        super().__init__(*args, **kwargs)
        
        # Business directory specific options
        self.extract_social_media = kwargs.get("extract_social_media", True)
        self.validate_emails = kwargs.get("validate_emails", True)
        self.validate_phones = kwargs.get("validate_phones", True)
        self.validate_urls = kwargs.get("validate_urls", True)
        self.country_code = kwargs.get("country_code", "US")
        
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
        Extract business data from a directory URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of extracted business data
            
        Raises:
            ScraperError: If data extraction fails
        """
        try:
            html_content = self._fetch_page(url)
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Find business listings
            business_elements = self._find_business_elements(soup)
            
            if not business_elements:
                self.logger.warning(f"No business listings found on {url}")
                return []
            
            # Extract data from each business element
            businesses = []
            for element in business_elements:
                try:
                    business_data = self._extract_item(element)
                    if business_data:
                        businesses.append(business_data)
                except Exception as e:
                    self.logger.warning(f"Error extracting business data: {str(e)}")
            
            return businesses
        
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {str(e)}")
            raise ScraperError(f"Failed to extract data from {url}: {str(e)}") from e
    
    def _find_business_elements(self, soup: BeautifulSoup) -> List[bs4.element.Tag]:
        """
        Find business listing elements in the HTML.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of BeautifulSoup elements representing business listings
        """
        # Look for common business listing container selectors
        selectors = [
            self.selectors.get("business_container"),
            ".business",
            ".business-listing",
            ".listing",
            ".vcard",
            ".result",
            "[itemtype*='LocalBusiness']",
            ".business-card",
            ".directory-listing",
        ]
        
        # Try each selector until we find businesses
        for selector in selectors:
            if not selector:
                continue
                
            elements = soup.select(selector)
            if elements:
                return elements
        
        # If no businesses found with CSS selectors, try to find them using other attributes
        candidates = soup.find_all(["div", "li"], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ["business", "listing", "result", "vcard", "card"]
        ))
        
        if candidates:
            return candidates
            
        # Last resort: check for common business listing patterns
        possible_businesses = []
        for element in soup.find_all(["div", "li", "article"]):
            # Check if element contains business name and contact info
            has_name = element.find(["h1", "h2", "h3", "h4", "strong", "b"])
            has_contact = (
                element.find(text=re.compile(r'\(\d{3}\)\s*\d{3}-\d{4}')) or  # Phone
                element.find(text=re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')) or  # Email
                element.find("address") or
                element.find(text=re.compile(r'\d+\s+[A-Za-z\s]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}'))  # Address
            )
            
            if has_name and has_contact:
                possible_businesses.append(element)
        
        return possible_businesses
    
    def _extract_item(self, element: bs4.element.Tag) -> Dict[str, Any]:
        """
        Extract data from a single business listing element.
        
        Args:
            element: BeautifulSoup element representing a business listing
            
        Returns:
            Extracted business data as a dictionary
        """
        business = {}
        
        # Extract business name
        business["name"] = self._extract_business_name(element)
        
        # Skip if no business name found (likely not a business)
        if not business["name"]:
            return {}
        
        # Extract address
        business["address"] = self._extract_address(element)
        
        # Extract phone
        business["phone"] = self._extract_phone(element)
        
        # Extract email
        business["email"] = self._extract_email(element)
        
        # Extract website
        business["website"] = self._extract_website(element)
        
        # Extract social media if configured
        if self.extract_social_media:
            business["social_media"] = self._extract_social_media(element)
        
        # Extract categories/business type
        business["categories"] = self._extract_categories(element)
        
        # Extract additional fields defined in selectors
        for field, selector in self.selectors.items():
            if field not in ["business_container"] and field not in business:
                try:
                    business[field] = self._extract_field(element, selector)
                except Exception as e:
                    self.logger.debug(f"Error extracting field {field}: {str(e)}")
        
        return business
    
    def _extract_business_name(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the business name from an element."""
        # Try using the provided selector
        if "name" in self.selectors:
            name_element = element.select_one(self.selectors["name"])
            if name_element:
                return name_element.get_text(strip=True)
        
        # Try common business name patterns
        for selector in [
            "h1", "h2", "h3", 
            ".business-name", ".listing-name", 
            "[itemprop='name']",
            ".name", ".title"
        ]:
            name_element = element.select_one(selector)
            if name_element:
                return name_element.get_text(strip=True)
        
        # Try to get text from a business link
        link = element.find("a", class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ["name", "title", "business"]
        ))
        if link and link.get_text(strip=True):
            return link.get_text(strip=True)
        
        # Try first heading or strong text
        heading = element.find(["h1", "h2", "h3", "h4", "strong", "b"])
        if heading:
            return heading.get_text(strip=True)
        
        return None
    
    def _extract_address(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the business address from an element."""
        # Try using the provided selector
        if "address" in self.selectors:
            addr_element = element.select_one(self.selectors["address"])
            if addr_element:
                return addr_element.get_text(strip=True)
        
        # Try common address patterns
        for selector in [
            "address", 
            "[itemprop='address']",
            ".address", 
            ".business-address",
            ".street-address"
        ]:
            addr_element = element.select_one(selector)
            if addr_element:
                return addr_element.get_text(strip=True)
        
        # Try to find address pattern in text
        addr_regex = re.compile(
            r'\d+\s+[A-Za-z0-9\s\.,]+,\s+[A-Za-z\s]+,\s+[A-Z]{2}(\s+\d{5})?',
            re.IGNORECASE
        )
        addr_match = element.find(text=addr_regex)
        
        if addr_match:
            return addr_match.strip()
        
        return None
    
    def _extract_phone(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the business phone number from an element."""
        # Try using the provided selector
        if "phone" in self.selectors:
            phone_element = element.select_one(self.selectors["phone"])
            if phone_element:
                phone_text = phone_element.get_text(strip=True)
                return self._parse_phone(phone_text)
        
        # Try common phone patterns
        for selector in [
            "[itemprop='telephone']",
            ".phone", 
            ".tel",
            ".business-phone",
            ".phone-number"
        ]:
            phone_element = element.select_one(selector)
            if phone_element:
                phone_text = phone_element.get_text(strip=True)
                return self._parse_phone(phone_text)
        
        # Try to find phone pattern in text
        phone_patterns = [
            r'\(\d{3}\)\s*\d{3}[-.]?\d{4}',  # (123) 456-7890
            r'\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # 123-456-7890 or 123.456.7890
            r'\+\d{1,3}[-.\s]?\d{3}[-.\s]?\d{3}[-.\s]?\d{4}',  # +1-123-456-7890
        ]
        
        for pattern in phone_patterns:
            phone_regex = re.compile(pattern)
            phone_match = element.find(text=phone_regex)
            
            if phone_match:
                match = re.search(phone_regex, phone_match)
                if match:
                    return match.group(0)
        
        return None
    
    def _parse_phone(self, phone_text: str) -> Optional[str]:
        """Parse and format a phone number string."""
        if not phone_text:
            return None
            
        # Extract digits
        digits = re.sub(r'[^\d+]', '', phone_text)
        
        if not digits:
            return None
            
        # If validating phones is enabled, check if it's valid
        if self.validate_phones and not is_valid_phone(digits, self.country_code):
            return None
            
        return digits
    
    def _extract_email(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the business email from an element."""
        # Try using the provided selector
        if "email" in self.selectors:
            email_element = element.select_one(self.selectors["email"])
            if email_element:
                if email_element.name == "a" and email_element.has_attr("href"):
                    href = email_element["href"]
                    if href.startswith("mailto:"):
                        email = href[7:]  # Remove 'mailto:' prefix
                        return email if not self.validate_emails or is_valid_email(email) else None
                return self._parse_email(email_element.get_text(strip=True))
        
        # Try common email patterns
        for selector in [
            "[itemprop='email']",
            ".email", 
            ".business-email",
            "a[href^='mailto:']"
        ]:
            email_element = element.select_one(selector)
            if email_element:
                if email_element.name == "a" and email_element.has_attr("href"):
                    href = email_element["href"]
                    if href.startswith("mailto:"):
                        email = href[7:]  # Remove 'mailto:' prefix
                        return email if not self.validate_emails or is_valid_email(email) else None
                return self._parse_email(email_element.get_text(strip=True))
        
        # Try to find email pattern in text
        email_regex = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        email_match = element.find(text=email_regex)
        
        if email_match:
            match = re.search(email_regex, email_match)
            if match:
                email = match.group(0)
                return email if not self.validate_emails or is_valid_email(email) else None
        
        return None
    
    def _parse_email(self, email_text: str) -> Optional[str]:
        """Parse and validate an email string."""
        if not email_text:
            return None
            
        # Extract email pattern
        email_regex = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
        match = re.search(email_regex, email_text)
        
        if match:
            email = match.group(0)
            # If validating emails is enabled, check if it's valid
            if self.validate_emails and not is_valid_email(email):
                return None
            return email
            
        return None
    
    def _extract_website(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the business website from an element."""
        # Try using the provided selector
        if "website" in self.selectors:
            website_element = element.select_one(self.selectors["website"])
            if website_element and website_element.name == "a" and website_element.has_attr("href"):
                href = website_element["href"]
                # Skip mailto and tel links
                if href.startswith(("mailto:", "tel:")):
                    return None
                # Clean and validate URL
                return self._clean_url(href)
        
        # Try common website patterns
        for selector in [
            "[itemprop='url']",
            ".website", 
            ".url",
            ".business-website",
            ".web"
        ]:
            website_element = element.select_one(selector)
            if website_element and website_element.name == "a" and website_element.has_attr("href"):
                href = website_element["href"]
                # Skip mailto and tel links
                if href.startswith(("mailto:", "tel:")):
                    continue
                # Clean and validate URL
                return self._clean_url(href)
        
        # Try to find any external links
        for link in element.find_all("a", href=True):
            href = link["href"]
            # Skip internal, mailto, and tel links
            if href.startswith(("mailto:", "tel:", "#", "/")):
                continue
            # Clean and validate URL
            cleaned_url = self._clean_url(href)
            if cleaned_url:
                return cleaned_url
        
        return None
    
    def _clean_url(self, url: str) -> Optional[str]:
        """Clean and validate a URL."""
        if not url:
            return None
            
        # Remove tracking parameters and fragments
        url = re.sub(r'[?#].*$', '', url)
        
        # Ensure URL starts with http:// or https://
        if not url.startswith(("http://", "https://")):
            url = "http://" + url
        
        # If validating URLs is enabled, check if it's valid
        if self.validate_urls and not is_valid_url(url):
            return None
            
        return url
    
    def _extract_social_media(self, element: bs4.element.Tag) -> Dict[str, str]:
        """Extract social media links from an element."""
        social_media = {}
        
        # Define social media patterns to look for
        social_patterns = {
            "facebook": [r'facebook\.com', r'fb\.com'],
            "twitter": [r'twitter\.com', r'x\.com'],
            "linkedin": [r'linkedin\.com'],
            "instagram": [r'instagram\.com'],
            "youtube": [r'youtube\.com', r'youtu\.be'],
            "pinterest": [r'pinterest\.com'],
            "yelp": [r'yelp\.com'],
        }
        
        # Look for social media links
        for link in element.find_all("a", href=True):
            href = link["href"]
            
            for platform, patterns in social_patterns.items():
                if any(re.search(pattern, href, re.IGNORECASE) for pattern in patterns):
                    social_media[platform] = href
                    break
        
        return social_media
    
    def _extract_categories(self, element: bs4.element.Tag) -> List[str]:
        """Extract business categories/types from an element."""
        categories = []
        
        # Try using the provided selector
        if "categories" in self.selectors:
            cat_elements = element.select(self.selectors["categories"])
            if cat_elements:
                for cat in cat_elements:
                    category = cat.get_text(strip=True)
                    if category:
                        categories.append(category)
        
        # Try common category patterns
        for selector in [
            "[itemprop='category']",
            ".category", 
            ".categories",
            ".business-category",
            ".tags"
        ]:
            cat_elements = element.select(selector)
            if cat_elements:
                for cat in cat_elements:
                    category = cat.get_text(strip=True)
                    if category:
                        categories.append(category)
        
        return categories
    
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
        Transform the extracted business data.
        
        This method can perform additional transformations specific to
        business contact data.
        
        Args:
            data: Raw extracted business data
            
        Returns:
            Transformed business data
        """
        transformed_data = []
        
        for business in data:
            # Skip empty businesses
            if not business or not business.get("name"):
                continue
                
            # Clean up business name
            if business.get("name"):
                business["name"] = business["name"].strip()
                
            # Format phone numbers consistently
            if business.get("phone"):
                # Keep only digits and + sign
                phone = re.sub(r'[^\d+]', '', business["phone"])
                # Format as XXX-XXX-XXXX or international format
                if len(phone) == 10:
                    business["phone"] = f"{phone[:3]}-{phone[3:6]}-{phone[6:]}"
                elif len(phone) > 10 and phone.startswith("+"):
                    # Keep international format
                    business["phone"] = phone
                elif len(phone) == 11 and phone.startswith("1"):
                    # Format US number with country code
                    business["phone"] = f"+{phone[0]}-{phone[1:4]}-{phone[4:7]}-{phone[7:]}"
                    
            # Make sure URLs are absolute
            if business.get("website") and not business["website"].startswith(("http://", "https://")):
                business["website"] = "http://" + business["website"]
            
            # Clean up email addresses
            if business.get("email"):
                business["email"] = business["email"].lower().strip()
                
            # Add timestamp
            from datetime import datetime
            business["scraped_at"] = datetime.now().isoformat()
            
            transformed_data.append(business)
            
        return transformed_data
    
    def validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate the business data.
        
        Args:
            data: Data to validate
            
        Returns:
            Validated data
        """
        validated_data = []
        
        for business in data:
            # Validate required fields
            if not business.get("name"):
                self.logger.warning("Skipping business without a name")
                continue
                
            # Validate email if present
            if business.get("email") and self.validate_emails:
                if not is_valid_email(business["email"]):
                    self.logger.warning(f"Invalid email format: {business['email']}")
                    business["email"] = None
                    
            # Validate phone if present
            if business.get("phone") and self.validate_phones:
                if not is_valid_phone(business["phone"], self.country_code):
                    self.logger.warning(f"Invalid phone format: {business['phone']}")
                    business["phone"] = None
                    
            # Validate website if present
            if business.get("website") and self.validate_urls:
                if not is_valid_url(business["website"]):
                    self.logger.warning(f"Invalid website URL: {business['website']}")
                    business["website"] = None
                
            # Add default values for missing fields
            if "address" not in business:
                business["address"] = None
                
            if "phone" not in business:
                business["phone"] = None
                
            if "email" not in business:
                business["email"] = None
                
            if "website" not in business:
                business["website"] = None
                
            if "categories" not in business:
                business["categories"] = []
                
            validated_data.append(business)
            
        return validated_data