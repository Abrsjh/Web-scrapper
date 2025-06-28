"""
Content Scraper Module

This module implements a scraper for articles, blog posts, and similar content.
It can extract article titles, authors, publication dates, content text,
images, and tags/categories.
"""

import logging
import re
from datetime import datetime
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
from ..utils.text import clean_text, summarize_text, extract_keywords


class ContentScraper(BaseScraper):
    """
    Scraper for articles, blog posts, and similar content.
    
    This scraper can extract articles and blog posts from content websites,
    including titles, authors, publication dates, content text, images,
    and tags/categories.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Initialize the content scraper.
        
        Extends the BaseScraper initialization with content scraping specific options.
        """
        super().__init__(*args, **kwargs)
        
        # Content scraping specific options
        self.extract_images = kwargs.get("extract_images", True)
        self.extract_metadata = kwargs.get("extract_metadata", True)
        self.generate_summary = kwargs.get("generate_summary", True)
        self.summary_length = kwargs.get("summary_length", 150)
        self.extract_keywords = kwargs.get("extract_keywords", True)
        self.max_keywords = kwargs.get("max_keywords", 5)
        self.follow_next_page = kwargs.get("follow_next_page", False)
        self.max_pages = kwargs.get("max_pages", 1)
        
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
        Extract content data from a URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of extracted content items
            
        Raises:
            ScraperError: If data extraction fails
        """
        try:
            html_content = self._fetch_page(url)
            soup = BeautifulSoup(html_content, "html.parser")
            base_url = url
            
            # Check if this is a single article page or a list of articles
            is_article_page = self._is_article_page(soup)
            
            if is_article_page:
                # Extract a single article
                article = self._extract_single_article(soup, base_url)
                return [article] if article else []
            else:
                # Find article listings
                article_elements = self._find_article_elements(soup)
                
                if not article_elements:
                    self.logger.warning(f"No article listings found on {url}")
                    return []
                
                # Extract data from each article element
                articles = []
                for element in article_elements:
                    try:
                        # Extract basic info and link from listing
                        article_data = self._extract_article_listing(element, base_url)
                        
                        if article_data and article_data.get("url"):
                            # If we have a URL, fetch the full article
                            if article_data.get("fetch_full"):
                                try:
                                    article_url = article_data["url"]
                                    article_html = self._fetch_page(article_url)
                                    article_soup = BeautifulSoup(article_html, "html.parser")
                                    full_article = self._extract_single_article(article_soup, article_url)
                                    
                                    # Merge the listing data with the full article data
                                    if full_article:
                                        article_data.update({
                                            k: v for k, v in full_article.items() 
                                            if k not in article_data or not article_data[k]
                                        })
                                except Exception as e:
                                    self.logger.warning(f"Error fetching full article from {article_data['url']}: {str(e)}")
                            
                            # Remove the fetch_full flag
                            article_data.pop("fetch_full", None)
                            
                            articles.append(article_data)
                    except Exception as e:
                        self.logger.warning(f"Error extracting article data: {str(e)}")
                
                # Follow pagination if configured
                if self.follow_next_page and len(articles) > 0 and self.max_pages > 1:
                    current_page = 1
                    next_url = self._find_next_page(soup, base_url)
                    
                    while next_url and current_page < self.max_pages:
                        try:
                            current_page += 1
                            self.logger.info(f"Following next page: {next_url}")
                            
                            next_html = self._fetch_page(next_url)
                            next_soup = BeautifulSoup(next_html, "html.parser")
                            next_elements = self._find_article_elements(next_soup)
                            
                            if not next_elements:
                                break
                                
                            for element in next_elements:
                                try:
                                    article_data = self._extract_article_listing(element, next_url)
                                    
                                    if article_data and article_data.get("url"):
                                        # If we have a URL, fetch the full article
                                        if article_data.get("fetch_full"):
                                            try:
                                                article_url = article_data["url"]
                                                article_html = self._fetch_page(article_url)
                                                article_soup = BeautifulSoup(article_html, "html.parser")
                                                full_article = self._extract_single_article(article_soup, article_url)
                                                
                                                # Merge the listing data with the full article data
                                                if full_article:
                                                    article_data.update({
                                                        k: v for k, v in full_article.items() 
                                                        if k not in article_data or not article_data[k]
                                                    })
                                            except Exception as e:
                                                self.logger.warning(f"Error fetching full article from {article_data['url']}: {str(e)}")
                                        
                                        # Remove the fetch_full flag
                                        article_data.pop("fetch_full", None)
                                        
                                        articles.append(article_data)
                                except Exception as e:
                                    self.logger.warning(f"Error extracting article data from next page: {str(e)}")
                            
                            # Find the next page link
                            next_url = self._find_next_page(next_soup, next_url)
                            
                        except Exception as e:
                            self.logger.error(f"Error following pagination: {str(e)}")
                            break
                
                return articles
        
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {str(e)}")
            raise ScraperError(f"Failed to extract data from {url}: {str(e)}") from e
    
    def _is_article_page(self, soup: BeautifulSoup) -> bool:
        """
        Determine if the page is a single article or a list of articles.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            True if the page is a single article, False if it's a list
        """
        # Check for typical article page characteristics
        article_indicators = [
            soup.find("article"),
            soup.find(class_=lambda x: x and "article" in x),
            soup.find(class_=lambda x: x and "post" in x and "post-list" not in x),
            soup.find(["h1", "h2"], class_=lambda x: x and any(
                term in str(x).lower() for term in ["title", "headline", "heading"]
            )),
            soup.find(attrs={"itemprop": "headline"}),
            soup.find(attrs={"property": "og:type", "content": "article"}),
        ]
        
        # Check for typical listing page characteristics
        listing_indicators = [
            len(soup.find_all("article")) > 1,
            len(soup.find_all(class_=lambda x: x and "post-" in x)) > 3,
            soup.find(class_=lambda x: x and any(
                term in str(x).lower() for term in ["archive", "listing", "index", "blog-list", "post-list"]
            )),
            soup.find(["ul", "div"], class_=lambda x: x and any(
                term in str(x).lower() for term in ["posts", "articles", "entries"]
            )),
        ]
        
        # Count indicators
        article_score = sum(1 for indicator in article_indicators if indicator)
        listing_score = sum(1 for indicator in listing_indicators if indicator)
        
        # If it's clearly a list, return False
        if listing_score > article_score:
            return False
            
        # Check for article content
        content_indicators = [
            len(soup.get_text()) > 2000,  # Long text content
            soup.find(["p", "div"], class_=lambda x: x and any(
                term in str(x).lower() for term in ["content", "body", "entry", "article-text"]
            )),
            soup.find(attrs={"itemprop": "articleBody"}),
            len(soup.find_all("p")) > 5,  # Multiple paragraphs
        ]
        
        content_score = sum(1 for indicator in content_indicators if indicator)
        
        # If it has significant content, it's likely an article
        return content_score >= 2 or article_score > listing_score
    
    def _find_article_elements(self, soup: BeautifulSoup) -> List[bs4.element.Tag]:
        """
        Find article listing elements in the HTML.
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            List of BeautifulSoup elements representing article listings
        """
        # Look for common article listing container selectors
        selectors = [
            self.selectors.get("article_container"),
            "article",
            ".post",
            ".entry",
            ".article",
            ".blog-post",
            ".blog-entry",
            "[itemtype*='BlogPosting']",
            "[itemtype*='Article']",
        ]
        
        # Try each selector until we find articles
        for selector in selectors:
            if not selector:
                continue
                
            elements = soup.select(selector)
            if elements:
                return elements
        
        # If no articles found with CSS selectors, try to find them using other attributes
        candidates = soup.find_all(["div", "li", "article"], class_=lambda x: x and any(
            keyword in str(x).lower() for keyword in ["post", "article", "entry", "item", "content"]
        ))
        
        if candidates:
            return candidates
            
        # Last resort: check for common article listing patterns
        possible_articles = []
        for element in soup.find_all(["div", "li", "article"]):
            # Check if element contains title and date or excerpt
            has_title = element.find(["h1", "h2", "h3", "h4"]) or element.find(class_=lambda x: x and "title" in str(x).lower())
            has_meta = (
                element.find(text=re.compile(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}')) or  # Date
                element.find(class_=lambda x: x and any(term in str(x).lower() for term in ["date", "time", "author", "meta"])) or
                element.find("time")
            )
            has_excerpt = element.find(["p", "div"], class_=lambda x: x and any(
                term in str(x).lower() for term in ["excerpt", "summary", "description", "intro"]
            ))
            
            if has_title and (has_meta or has_excerpt):
                possible_articles.append(element)
        
        return possible_articles
    
    def _find_next_page(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """
        Find the next page URL in pagination.
        
        Args:
            soup: BeautifulSoup object of the page
            base_url: Base URL for resolving relative links
            
        Returns:
            URL of the next page, or None if not found
        """
        # Common next page selectors
        selectors = [
            ".next",
            ".next-page",
            ".pagination .next",
            "a[rel='next']",
            "a:contains('Next')",
            "a:contains('»')",
            "a.next",
            ".pagination a:contains('›')",
        ]
        
        # Try each selector
        for selector in selectors:
            try:
                next_link = soup.select_one(selector)
                if next_link and next_link.has_attr("href"):
                    next_url = next_link["href"]
                    # Make sure it's an absolute URL
                    if not next_url.startswith(("http://", "https://")):
                        next_url = urljoin(base_url, next_url)
                    return next_url
            except Exception:
                continue
        
        # Try to find links with page numbers
        current_page_indicator = None
        
        # Look for current page indicator
        for selector in [".current", ".active", ".selected", "[aria-current='page']"]:
            indicator = soup.select_one(selector)
            if indicator:
                current_page_indicator = indicator
                break
        
        if current_page_indicator:
            # Try to find the next page number
            if current_page_indicator.name == "a" and current_page_indicator.has_attr("href"):
                # The current page is a link, look for links with higher page numbers
                try:
                    current_num = int(re.search(r'\d+', current_page_indicator.get_text()).group())
                    next_page_links = soup.find_all("a", href=True, text=re.compile(r'\d+'))
                    
                    for link in next_page_links:
                        try:
                            page_num = int(re.search(r'\d+', link.get_text()).group())
                            if page_num == current_num + 1:
                                next_url = link["href"]
                                if not next_url.startswith(("http://", "https://")):
                                    next_url = urljoin(base_url, next_url)
                                return next_url
                        except Exception:
                            continue
                except Exception:
                    pass
            else:
                # The current page is not a link, look for its parent and siblings
                parent = current_page_indicator.parent
                if parent:
                    next_element = current_page_indicator.find_next_sibling()
                    if next_element and next_element.name == "a" and next_element.has_attr("href"):
                        next_url = next_element["href"]
                        if not next_url.startswith(("http://", "https://")):
                            next_url = urljoin(base_url, next_url)
                        return next_url
        
        # Look for pagination patterns in URL
        parsed_url = urlparse(base_url)
        path = parsed_url.path
        query = parsed_url.query
        
        # Check for page parameter in query string
        if "page=" in query:
            page_match = re.search(r'page=(\d+)', query)
            if page_match:
                current_page = int(page_match.group(1))
                next_page = current_page + 1
                next_query = re.sub(r'page=\d+', f'page={next_page}', query)
                next_url = f"{parsed_url.scheme}://{parsed_url.netloc}{path}?{next_query}"
                return next_url
        
        # Check for pagination in path (e.g., /blog/page/2/)
        page_in_path = re.search(r'/page/(\d+)/?$', path)
        if page_in_path:
            current_page = int(page_in_path.group(1))
            next_page = current_page + 1
            next_path = re.sub(r'/page/\d+/?$', f'/page/{next_page}/', path)
            next_url = f"{parsed_url.scheme}://{parsed_url.netloc}{next_path}"
            return next_url
        elif "/page/" not in path and soup.find_all("a", href=re.compile(r'/page/\d+/?$')):
            # First page without /page/ in URL but other pages have it
            next_url = f"{parsed_url.scheme}://{parsed_url.netloc}{path.rstrip('/')}/page/2/"
            return next_url
        
        return None
    
    def _extract_article_listing(
        self, element: bs4.element.Tag, base_url: str
    ) -> Dict[str, Any]:
        """
        Extract basic information from an article listing.
        
        Args:
            element: BeautifulSoup element representing an article listing
            base_url: Base URL for resolving relative links
            
        Returns:
            Basic article data extracted from the listing
        """
        article = {}
        
        # Extract title
        article["title"] = self._extract_title(element)
        
        # Skip if no title found (likely not an article)
        if not article["title"]:
            return {}
        
        # Extract URL
        article["url"] = self._extract_url(element, base_url)
        
        # Extract date
        article["date"] = self._extract_date(element)
        
        # Extract author
        article["author"] = self._extract_author(element)
        
        # Extract excerpt/summary
        article["excerpt"] = self._extract_excerpt(element)
        
        # Extract featured image
        if self.extract_images:
            article["image"] = self._extract_featured_image(element, base_url)
        
        # Extract categories/tags
        article["categories"] = self._extract_categories(element)
        
        # Flag to indicate whether to fetch the full article
        article["fetch_full"] = bool(article.get("url") and not article.get("content"))
        
        return article
    
    def _extract_single_article(
        self, soup: BeautifulSoup, url: str
    ) -> Dict[str, Any]:
        """
        Extract data from a single article page.
        
        Args:
            soup: BeautifulSoup object of the article page
            url: URL of the article
            
        Returns:
            Extracted article data
        """
        article = {}
        
        # Extract title
        article["title"] = self._extract_title(soup)
        
        # Skip if no title found (likely not an article)
        if not article["title"]:
            return {}
        
        # Set URL
        article["url"] = url
        
        # Extract date
        article["date"] = self._extract_date(soup)
        
        # Extract author
        article["author"] = self._extract_author(soup)
        
        # Extract content
        article["content"] = self._extract_content(soup)
        
        # Extract excerpt/summary
        if self.generate_summary and article["content"]:
            article["excerpt"] = summarize_text(article["content"], self.summary_length)
        else:
            article["excerpt"] = self._extract_excerpt(soup)
        
        # Extract featured image
        if self.extract_images:
            article["image"] = self._extract_featured_image(soup, url)
            article["images"] = self._extract_content_images(soup, url)
        
        # Extract categories/tags
        article["categories"] = self._extract_categories(soup)
        
        # Extract metadata
        if self.extract_metadata:
            article["metadata"] = self._extract_metadata(soup)
        
        # Extract keywords
        if self.extract_keywords and article["content"]:
            article["keywords"] = extract_keywords(article["content"], self.max_keywords)
        
        return article
    
    def _extract_title(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the article title from an element."""
        # Try using the provided selector
        if "title" in self.selectors:
            title_element = element.select_one(self.selectors["title"])
            if title_element:
                return title_element.get_text(strip=True)
        
        # Try common title patterns
        for selector in [
            "h1",
            "h1.entry-title", 
            "h1.post-title", 
            "h1.article-title",
            ".entry-title", 
            ".post-title", 
            ".article-title",
            "[itemprop='headline']",
            "header h1",
            "header h2",
            "h2.entry-title",
            ".title"
        ]:
            title_element = element.select_one(selector)
            if title_element:
                return title_element.get_text(strip=True)
        
        # Try to get text from a title in meta tags
        meta_title = element.find("meta", property="og:title")
        if meta_title and meta_title.has_attr("content"):
            return meta_title["content"]
        
        # Try first heading
        heading = element.find(["h1", "h2", "h3"])
        if heading:
            return heading.get_text(strip=True)
        
        return None
    
    def _extract_url(self, element: bs4.element.Tag, base_url: str) -> Optional[str]:
        """Extract the article URL from an element."""
        # Try using the provided selector
        if "url" in self.selectors:
            url_element = element.select_one(self.selectors["url"])
            if url_element and url_element.has_attr("href"):
                url = url_element["href"]
                # Make sure it's an absolute URL
                if not url.startswith(("http://", "https://")):
                    url = urljoin(base_url, url)
                return url
        
        # Try to find a link in the title
        title_element = element.find(["h1", "h2", "h3", "h4"])
        if title_element:
            link = title_element.find("a", href=True)
            if link:
                url = link["href"]
                # Make sure it's an absolute URL
                if not url.startswith(("http://", "https://")):
                    url = urljoin(base_url, url)
                return url
        
        # Try to find any link
        link = element.find("a", href=True)
        if link:
            url = link["href"]
            # Make sure it's an absolute URL
            if not url.startswith(("http://", "https://")):
                url = urljoin(base_url, url)
            return url
            
        return None
    
    def _extract_date(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the article publication date from an element."""
        # Try using the provided selector
        if "date" in self.selectors:
            date_element = element.select_one(self.selectors["date"])
            if date_element:
                return self._parse_date(date_element)
        
        # Try common date patterns
        for selector in [
            "time",
            "[itemprop='datePublished']",
            "[property='article:published_time']",
            ".date",
            ".published",
            ".post-date",
            ".entry-date",
            ".article-date",
            ".meta-date",
            "meta[property='article:published_time']",
        ]:
            date_element = element.select_one(selector)
            if date_element:
                return self._parse_date(date_element)
        
        # Try to find a date pattern in text
        date_patterns = [
            # Various date formats
            r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}',  # 25 December 2022
            r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}',  # December 25, 2022
            r'\d{4}-\d{2}-\d{2}',  # 2022-12-25
            r'\d{2}/\d{2}/\d{4}',  # 12/25/2022
            r'\d{1,2}/\d{1,2}/\d{2,4}',  # 12/25/22
        ]
        
        for pattern in date_patterns:
            date_regex = re.compile(pattern)
            date_match = element.find(text=date_regex)
            
            if date_match:
                match = re.search(date_regex, date_match)
                if match:
                    date_str = match.group(0)
                    try:
                        # Try multiple date formats
                        for fmt in [
                            '%d %B %Y', '%B %d, %Y', '%B %d %Y',
                            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y'
                        ]:
                            try:
                                date_obj = datetime.strptime(date_str, fmt)
                                return date_obj.isoformat()[:10]
                            except ValueError:
                                continue
                    except Exception:
                        return date_str
        
        return None
    
    def _parse_date(self, element: bs4.element.Tag) -> Optional[str]:
        """Parse a date from an element."""
        # Check for datetime attribute
        if element.has_attr("datetime"):
            date_str = element["datetime"]
            try:
                # ISO format
                if 'T' in date_str:
                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    return date_obj.date().isoformat()
                # Date only
                else:
                    return date_str.split('T')[0]
            except Exception:
                pass
        
        # Check for content attribute (meta tags)
        if element.has_attr("content"):
            date_str = element["content"]
            try:
                # ISO format
                if 'T' in date_str:
                    date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    return date_obj.date().isoformat()
                # Date only
                else:
                    return date_str.split('T')[0]
            except Exception:
                pass
        
        # Try text content
        date_text = element.get_text(strip=True)
        date_patterns = [
            # Various date formats
            (r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}', '%d %B %Y'),
            (r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}', '%B %d, %Y'),
            (r'\d{4}-\d{2}-\d{2}', '%Y-%m-%d'),
            (r'\d{2}/\d{2}/\d{4}', '%m/%d/%Y'),
        ]
        
        for pattern, fmt in date_patterns:
            date_match = re.search(pattern, date_text)
            if date_match:
                date_str = date_match.group(0)
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.date().isoformat()
                except ValueError:
                    # Try alternate format
                    try:
                        if fmt == '%m/%d/%Y':
                            date_obj = datetime.strptime(date_str, '%d/%m/%Y')
                            return date_obj.date().isoformat()
                    except ValueError:
                        continue
        
        return None
    
    def _extract_author(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the article author from an element."""
        # Try using the provided selector
        if "author" in self.selectors:
            author_element = element.select_one(self.selectors["author"])
            if author_element:
                return author_element.get_text(strip=True)
        
        # Try common author patterns
        for selector in [
            "[itemprop='author']",
            "[rel='author']",
            ".author",
            ".byline",
            ".entry-author",
            ".post-author",
            "meta[name='author']",
            ".meta-author",
        ]:
            author_element = element.select_one(selector)
            if author_element:
                if author_element.name == "meta" and author_element.has_attr("content"):
                    return author_element["content"]
                return author_element.get_text(strip=True)
        
        # Try to find author pattern in text
        author_patterns = [
            r'By\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})',
            r'Author[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})',
        ]
        
        for pattern in author_patterns:
            author_regex = re.compile(pattern)
            author_match = element.find(text=author_regex)
            
            if author_match:
                match = re.search(author_regex, author_match)
                if match:
                    return match.group(1)
        
        return None
    
    def _extract_excerpt(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the article excerpt/summary from an element."""
        # Try using the provided selector
        if "excerpt" in self.selectors:
            excerpt_element = element.select_one(self.selectors["excerpt"])
            if excerpt_element:
                return clean_text(excerpt_element.get_text(strip=True))
        
        # Try common excerpt patterns
        for selector in [
            "[itemprop='description']",
            "meta[name='description']",
            "meta[property='og:description']",
            ".excerpt",
            ".entry-summary",
            ".post-excerpt",
            ".summary",
            ".description",
            ".intro",
        ]:
            excerpt_element = element.select_one(selector)
            if excerpt_element:
                if excerpt_element.name == "meta" and excerpt_element.has_attr("content"):
                    return clean_text(excerpt_element["content"])
                return clean_text(excerpt_element.get_text(strip=True))
        
        # Try to find the first paragraph
        first_p = element.find("p")
        if first_p:
            text = first_p.get_text(strip=True)
            if len(text) > 20:  # Must be at least 20 chars to be considered an excerpt
                return clean_text(text)
        
        return None
    
    def _extract_content(self, element: bs4.element.Tag) -> Optional[str]:
        """Extract the article content from an element."""
        # Try using the provided selector
        if "content" in self.selectors:
            content_element = element.select_one(self.selectors["content"])
            if content_element:
                return clean_text(content_element.get_text(strip=True))
        
        # Try common content patterns
        for selector in [
            "[itemprop='articleBody']",
            ".entry-content",
            ".post-content",
            ".article-content",
            ".content",
            "article",
            ".post-body",
            "#content",
        ]:
            content_element = element.select_one(selector)
            if content_element:
                # Skip if this is just a container with no real content
                if len(content_element.get_text(strip=True)) < 100:
                    continue
                return clean_text(content_element.get_text(strip=True))
        
        # Try to find content by looking for a series of paragraphs
        paragraphs = element.find_all("p")
        if len(paragraphs) >= 3:
            content = " ".join(p.get_text(strip=True) for p in paragraphs if len(p.get_text(strip=True)) > 20)
            if content and len(content) > 200:  # Must be at least 200 chars to be considered content
                return clean_text(content)
        
        return None
    
    def _extract_featured_image(self, element: bs4.element.Tag, base_url: str) -> Optional[str]:
        """Extract the article featured image from an element."""
        # Try using the provided selector
        if "image" in self.selectors:
            img_element = element.select_one(self.selectors["image"])
            if img_element:
                if img_element.name == "img" and img_element.has_attr("src"):
                    src = img_element["src"]
                    # Make sure it's an absolute URL
                    if not src.startswith(("http://", "https://", "data:")):
                        src = urljoin(base_url, src)
                    return src
                elif img_element.name == "meta" and img_element.has_attr("content"):
                    src = img_element["content"]
                    if not src.startswith(("http://", "https://", "data:")):
                        src = urljoin(base_url, src)
                    return src
        
        # Try common image patterns
        for selector in [
            "meta[property='og:image']",
            "meta[name='twitter:image']",
            "[itemprop='image']",
            ".featured-image img",
            ".post-thumbnail img",
            ".entry-image img",
            "article img:first-of-type",
            ".wp-post-image",
        ]:
            img_element = element.select_one(selector)
            if img_element:
                if img_element.name == "img" and img_element.has_attr("src"):
                    src = img_element["src"]
                    # Make sure it's an absolute URL
                    if not src.startswith(("http://", "https://", "data:")):
                        src = urljoin(base_url, src)
                    return src
                elif img_element.name == "meta" and img_element.has_attr("content"):
                    src = img_element["content"]
                    if not src.startswith(("http://", "https://", "data:")):
                        src = urljoin(base_url, src)
                    return src
        
        # Try to find any image
        img = element.find("img", src=True)
        if img:
            src = img["src"]
            # Make sure it's an absolute URL
            if not src.startswith(("http://", "https://", "data:")):
                src = urljoin(base_url, src)
            return src
        
        return None
    
    def _extract_content_images(self, element: bs4.element.Tag, base_url: str) -> List[str]:
        """Extract all images from the article content."""
        images = []
        
        # Try using the provided selector
        if "content" in self.selectors:
            content_element = element.select_one(self.selectors["content"])
            if content_element:
                for img in content_element.find_all("img", src=True):
                    src = img["src"]
                    # Skip data URLs and small icons
                    if src.startswith("data:") or "icon" in src.lower():
                        continue
                    # Make sure it's an absolute URL
                    if not src.startswith(("http://", "https://")):
                        src = urljoin(base_url, src)
                    images.append(src)
                return images
        
        # Try common content patterns
        for selector in [
            "[itemprop='articleBody']",
            ".entry-content",
            ".post-content",
            ".article-content",
            ".content",
            "article",
        ]:
            content_element = element.select_one(selector)
            if content_element:
                for img in content_element.find_all("img", src=True):
                    src = img["src"]
                    # Skip data URLs and small icons
                    if src.startswith("data:") or "icon" in src.lower():
                        continue
                    # Make sure it's an absolute URL
                    if not src.startswith(("http://", "https://")):
                        src = urljoin(base_url, src)
                    images.append(src)
                return images
        
        # Try to find all images
        for img in element.find_all("img", src=True):
            src = img["src"]
            # Skip data URLs and small icons
            if src.startswith("data:") or "icon" in src.lower():
                continue
            # Make sure it's an absolute URL
            if not src.startswith(("http://", "https://")):
                src = urljoin(base_url, src)
            images.append(src)
        
        return images
    
    def _extract_categories(self, element: bs4.element.Tag) -> List[str]:
        """Extract article categories/tags from an element."""
        categories = []
        
        # Try using the provided selector
        if "categories" in self.selectors:
            cat_elements = element.select(self.selectors["categories"])
            if cat_elements:
                for cat in cat_elements:
                    category = cat.get_text(strip=True)
                    if category:
                        categories.append(category)
                return categories
        
        # Try common category patterns
        for selector in [
            "[itemprop='keywords']",
            "[rel='category']",
            ".category",
            ".tag",
            ".categories",
            ".tags",
            ".post-category",
            ".post-tag",
            "meta[property='article:tag']",
        ]:
            cat_elements = element.select(selector)
            if cat_elements:
                for cat in cat_elements:
                    if cat.name == "meta" and cat.has_attr("content"):
                        category = cat["content"]
                    else:
                        category = cat.get_text(strip=True)
                    if category and category not in categories:
                        categories.append(category)
        
        return categories
    
    def _extract_metadata(self, element: bs4.element.Tag) -> Dict[str, str]:
        """Extract article metadata from an element."""
        metadata = {}
        
        # Try to find Open Graph and Twitter Card metadata
        meta_tags = element.find_all("meta")
        for meta in meta_tags:
            # Open Graph properties
            if meta.has_attr("property") and meta.has_attr("content"):
                prop = meta["property"]
                if prop.startswith("og:") or prop.startswith("article:"):
                    key = prop.split(":")[-1]
                    metadata[key] = meta["content"]
            
            # Twitter Card properties
            if meta.has_attr("name") and meta.has_attr("content"):
                name = meta["name"]
                if name.startswith("twitter:"):
                    key = name.split(":")[-1]
                    metadata[key] = meta["content"]
                elif name in ["author", "description", "keywords"]:
                    metadata[name] = meta["content"]
        
        # Extract reading time if available
        for selector in [".reading-time", ".read-time", "[itemprop='timeRequired']"]:
            time_element = element.select_one(selector)
            if time_element:
                metadata["reading_time"] = time_element.get_text(strip=True)
                break
        
        # Extract word count
        if "content" in self.selectors:
            content_element = element.select_one(self.selectors["content"])
            if content_element:
                text = content_element.get_text(strip=True)
                word_count = len(text.split())
                metadata["word_count"] = str(word_count)
                
                # Estimate reading time if not already found
                if "reading_time" not in metadata:
                    minutes = max(1, round(word_count / 200))  # Assume 200 words per minute
                    metadata["reading_time"] = f"{minutes} min read"
        
        return metadata
    
    def _extract_item(self, element: bs4.element.Tag) -> Dict[str, Any]:
        """
        Extract data from a single element.
        
        This is required by the BaseScraper interface but for ContentScraper,
        we use more specialized methods.
        
        Args:
            element: Element to extract data from
            
        Returns:
            Extracted data as a dictionary
        """
        # For content scraper, this depends on whether it's an article page or listing
        from urllib.parse import urlparse
        base_url = f"{urlparse(self.urls[0]).scheme}://{urlparse(self.urls[0]).netloc}"
        
        return self._extract_article_listing(element, base_url)
    
    def transform_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform the extracted content data.
        
        This method can perform additional transformations specific to
        content data.
        
        Args:
            data: Raw extracted content data
            
        Returns:
            Transformed content data
        """
        transformed_data = []
        
        for article in data:
            # Skip empty articles
            if not article or not article.get("title"):
                continue
                
            # Clean up article title
            if article.get("title"):
                article["title"] = article["title"].strip()
                
            # Ensure excerpt exists
            if not article.get("excerpt") and article.get("content"):
                article["excerpt"] = summarize_text(article["content"], self.summary_length)
                
            # Format date consistently
            if article.get("date"):
                try:
                    # Try to parse and standardize date
                    # If it's already ISO format, keep it as is
                    if not re.match(r'\d{4}-\d{2}-\d{2}', article["date"]):
                        for fmt in [
                            '%d %B %Y', '%B %d, %Y', '%B %d %Y',
                            '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y'
                        ]:
                            try:
                                date_obj = datetime.strptime(article["date"], fmt)
                                article["date"] = date_obj.date().isoformat()
                                break
                            except ValueError:
                                continue
                except Exception:
                    # Keep the original date if parsing fails
                    pass
                
            # Make sure URLs are absolute
            if article.get("url") and not article["url"].startswith(("http://", "https://")):
                from urllib.parse import urlparse
                base_url = f"{urlparse(self.urls[0]).scheme}://{urlparse(self.urls[0]).netloc}"
                article["url"] = urljoin(base_url, article["url"])
            
            # Add timestamp
            article["scraped_at"] = datetime.now().isoformat()
            
            transformed_data.append(article)
            
        return transformed_data
    
    def validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate the content data.
        
        Args:
            data: Data to validate
            
        Returns:
            Validated data
        """
        validated_data = []
        
        for article in data:
            # Validate required fields
            if not article.get("title"):
                self.logger.warning("Skipping article without a title")
                continue
                
            # Add default values for missing fields
            if "url" not in article:
                article["url"] = None
                
            if "date" not in article:
                article["date"] = None
                
            if "author" not in article:
                article["author"] = None
                
            if "excerpt" not in article:
                article["excerpt"] = None
                
            if "content" not in article:
                article["content"] = None
                
            if "image" not in article:
                article["image"] = None
                
            if "categories" not in article:
                article["categories"] = []
                
            validated_data.append(article)
            
        return validated_data
    
    def generate_rss(self, data: List[Dict[str, Any]], title: str, description: str, link: str) -> str:
        """
        Generate an RSS feed from the scraped content.
        
        Args:
            data: List of articles
            title: Feed title
            description: Feed description
            link: Feed link
            
        Returns:
            RSS feed XML as a string
        """
        import datetime
        from xml.sax.saxutils import escape
        
        rss = [
            '<?xml version="1.0" encoding="UTF-8" ?>',
            '<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">',
            '<channel>',
            f'<title>{escape(title)}</title>',
            f'<description>{escape(description)}</description>',
            f'<link>{escape(link)}</link>',
            f'<atom:link href="{escape(link)}" rel="self" type="application/rss+xml" />',
            f'<lastBuildDate>{datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")}</lastBuildDate>',
        ]
        
        for article in data:
            if not article.get("title") or not article.get("url"):
                continue
                
            pub_date = ""
            if article.get("date"):
                try:
                    date_obj = datetime.datetime.fromisoformat(article["date"])
                    pub_date = date_obj.strftime("%a, %d %b %Y %H:%M:%S +0000")
                except ValueError:
                    pub_date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
            else:
                pub_date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S +0000")
            
            rss.append('<item>')
            rss.append(f'<title>{escape(article["title"])}</title>')
            rss.append(f'<link>{escape(article["url"])}</link>')
            rss.append(f'<guid>{escape(article["url"])}</guid>')
            
            if article.get("author"):
                rss.append(f'<author>{escape(article["author"])}</author>')
                
            if pub_date:
                rss.append(f'<pubDate>{pub_date}</pubDate>')
                
            if article.get("excerpt"):
                rss.append(f'<description>{escape(article["excerpt"])}</description>')
                
            if article.get("content"):
                rss.append(f'<content:encoded><![CDATA[{article["content"]}]]></content:encoded>')
                
            if article.get("categories"):
                for category in article["categories"]:
                    rss.append(f'<category>{escape(category)}</category>')
                    
            if article.get("image"):
                rss.append(f'<enclosure url="{escape(article["image"])}" type="image/jpeg" />')
                
            rss.append('</item>')
            
        rss.append('</channel>')
        rss.append('</rss>')
        
        return '\n'.join(rss)