#!/usr/bin/env python3
"""
Basic Usage Example for Web Scraper Pro

This script demonstrates how to use the Web Scraper Pro package programmatically.
It shows basic setup, configuration, and execution of scraping tasks.
"""

import os
import sys
import json
from pprint import pprint

# Add the src directory to the Python path if running from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Import the web scraper package
from webscraper import (
    EcommerceScraper, BusinessDirectoryScraper, ContentScraper,
    get_storage_handler, configure_logging
)

# Set up logging
configure_logging(level="INFO", log_to_console=True)


def example_ecommerce_scraper():
    """
    Example of using the EcommerceScraper to extract product information.
    """
    print("\n--- E-commerce Scraper Example ---\n")
    
    # Example URLs (replace with actual e-commerce URLs)
    urls = [
        "https://example.com/products",
        "https://example.com/products/page/2"
    ]
    
    # Define CSS selectors for extracting data
    selectors = {
        "product_container": ".product-item",
        "name": ".product-title",
        "price": ".price",
        "availability": ".stock-status",
        "images": ".product-image img",
        "rating": ".rating",
        "review_count": ".review-count"
    }
    
    # Define output configuration
    output_config = {
        "format": "json",
        "path": "data/products.json",
        "options": {
            "indent": 2,
            "ensure_ascii": False
        }
    }
    
    # Create the scraper
    scraper = EcommerceScraper(
        urls=urls,
        selectors=selectors,
        output_config=output_config,
        delay=2,
        retries=3,
        timeout=30,
        extract_reviews=True,
        extract_images=True
    )
    
    print(f"Initialized scraper for {len(urls)} URLs")
    print(f"Selectors: {', '.join(selectors.keys())}")
    
    # Run the scraper (commented out as this is just an example)
    # results = scraper.scrape()
    
    # Instead of actually scraping, let's use some sample data
    results = [
        {
            "name": "Example Product 1",
            "price": 29.99,
            "currency": "$",
            "availability": "In Stock",
            "images": ["https://example.com/images/product1.jpg"],
            "reviews": {"rating": 4.5, "count": 27}
        },
        {
            "name": "Example Product 2",
            "price": 49.99,
            "currency": "$",
            "availability": "Out of Stock",
            "images": ["https://example.com/images/product2.jpg"],
            "reviews": {"rating": 4.0, "count": 14}
        }
    ]
    
    print(f"Extracted {len(results)} products")
    
    # Print first product
    if results:
        print("\nSample product:")
        pprint(results[0])
    
    # Save the results
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Use the storage handler directly
    json_handler = get_storage_handler("json")
    json_handler.save(results, "data/products.json", indent=2, ensure_ascii=False)
    
    print(f"\nSaved {len(results)} products to data/products.json")


def example_business_directory_scraper():
    """
    Example of using the BusinessDirectoryScraper to extract business information.
    """
    print("\n--- Business Directory Scraper Example ---\n")
    
    # Example URLs (replace with actual business directory URLs)
    urls = [
        "https://example.com/directory",
        "https://example.com/directory/page/2"
    ]
    
    # Define CSS selectors for extracting data
    selectors = {
        "business_container": ".business-listing",
        "name": ".business-name",
        "address": ".address",
        "phone": ".phone",
        "email": ".email",
        "website": ".website",
        "categories": ".business-category"
    }
    
    # Define output configuration
    output_config = {
        "format": "csv",
        "path": "data/businesses.csv",
        "options": {
            "encoding": "utf-8",
            "index": False
        }
    }
    
    # Create the scraper
    scraper = BusinessDirectoryScraper(
        urls=urls,
        selectors=selectors,
        output_config=output_config,
        delay=3,
        retries=3,
        timeout=30,
        extract_social_media=True,
        validate_emails=True,
        validate_phones=True
    )
    
    print(f"Initialized scraper for {len(urls)} URLs")
    print(f"Selectors: {', '.join(selectors.keys())}")
    
    # Run the scraper (commented out as this is just an example)
    # results = scraper.scrape()
    
    # Instead of actually scraping, let's use some sample data
    results = [
        {
            "name": "Example Business 1",
            "address": "123 Main St, Anytown, CA 12345",
            "phone": "555-123-4567",
            "email": "contact@example1.com",
            "website": "https://example1.com",
            "categories": ["Restaurant", "Italian"],
            "social_media": {
                "facebook": "https://facebook.com/example1",
                "twitter": "https://twitter.com/example1"
            }
        },
        {
            "name": "Example Business 2",
            "address": "456 Oak Ave, Somewhere, NY 67890",
            "phone": "555-987-6543",
            "email": "info@example2.com",
            "website": "https://example2.com",
            "categories": ["Retail", "Clothing"],
            "social_media": {
                "facebook": "https://facebook.com/example2",
                "instagram": "https://instagram.com/example2"
            }
        }
    ]
    
    print(f"Extracted {len(results)} businesses")
    
    # Print first business
    if results:
        print("\nSample business:")
        pprint(results[0])
    
    # Save the results
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Use the storage handler directly
    csv_handler = get_storage_handler("csv")
    csv_handler.save(results, "data/businesses.csv", encoding="utf-8", index=False)
    
    print(f"\nSaved {len(results)} businesses to data/businesses.csv")


def example_content_scraper():
    """
    Example of using the ContentScraper to extract articles and blog posts.
    """
    print("\n--- Content Scraper Example ---\n")
    
    # Example URLs (replace with actual content site URLs)
    urls = [
        "https://example.com/blog",
        "https://example.com/news"
    ]
    
    # Define CSS selectors for extracting data
    selectors = {
        "article_container": "article, .post",
        "title": "h1, .article-title",
        "date": "time, .published-date",
        "author": ".author, .byline",
        "content": ".article-content, .entry-content",
        "excerpt": ".excerpt, .summary",
        "image": ".featured-image img",
        "categories": ".category, .tag"
    }
    
    # Define output configuration
    output_config = {
        "format": "excel",
        "path": "data/articles.xlsx",
        "options": {
            "sheet_name": "Articles",
            "freeze_panes": [1, 0],
            "autofilter": True
        }
    }
    
    # Create the scraper
    scraper = ContentScraper(
        urls=urls,
        selectors=selectors,
        output_config=output_config,
        delay=2,
        retries=3,
        timeout=30,
        extract_images=True,
        extract_metadata=True,
        generate_summary=True,
        extract_keywords=True,
        follow_next_page=True,
        max_pages=3
    )
    
    print(f"Initialized scraper for {len(urls)} URLs")
    print(f"Selectors: {', '.join(selectors.keys())}")
    
    # Run the scraper (commented out as this is just an example)
    # results = scraper.scrape()
    
    # Instead of actually scraping, let's use some sample data
    results = [
        {
            "title": "Example Article 1",
            "date": "2023-05-15",
            "author": "John Doe",
            "content": "This is the full content of the article. It includes multiple paragraphs of text...",
            "excerpt": "This is a summary of the article that gives a brief overview of the content.",
            "image": "https://example.com/images/article1.jpg",
            "categories": ["Technology", "AI"],
            "keywords": ["ai", "machine learning", "technology"],
            "url": "https://example.com/blog/article1"
        },
        {
            "title": "Example Article 2",
            "date": "2023-05-10",
            "author": "Jane Smith",
            "content": "Another article with different content. This one discusses web development...",
            "excerpt": "A brief overview of web development trends in 2023.",
            "image": "https://example.com/images/article2.jpg",
            "categories": ["Web Development", "JavaScript"],
            "keywords": ["javascript", "web", "development", "trends"],
            "url": "https://example.com/blog/article2"
        }
    ]
    
    print(f"Extracted {len(results)} articles")
    
    # Print first article
    if results:
        print("\nSample article:")
        # Print a subset of fields for readability
        sample = {k: results[0][k] for k in ["title", "date", "author", "excerpt", "categories", "keywords"]}
        pprint(sample)
    
    # Save the results
    if not os.path.exists("data"):
        os.makedirs("data")
    
    # Use the storage handler directly
    excel_handler = get_storage_handler("excel")
    excel_handler.save(results, "data/articles.xlsx", sheet_name="Articles", freeze_panes=[1, 0], autofilter=True)
    
    print(f"\nSaved {len(results)} articles to data/articles.xlsx")
    
    # Generate RSS feed
    print("\nGenerating RSS feed...")
    
    # In a real implementation, we would use the scraper's generate_rss method
    # Here we'll just demonstrate creating an RSS file
    rss_content = scraper.generate_rss(
        results,
        title="Example Blog Feed",
        description="Latest articles from Example Blog",
        link="https://example.com/feed"
    )
    
    with open("data/feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_content)
    
    print("RSS feed saved to data/feed.xml")


if __name__ == "__main__":
    print("Web Scraper Pro - Basic Usage Examples")
    print("======================================")
    
    # Run all examples
    example_ecommerce_scraper()
    example_business_directory_scraper()
    example_content_scraper()
    
    print("\nAll examples completed!")