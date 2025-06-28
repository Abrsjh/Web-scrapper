# Web Scraper

A professional-grade web scraping and automation tool designed for data extraction services.

## Features

- Modular architecture with pluggable components
- Multiple scraping backends (BeautifulSoup/Requests, Selenium/Playwright)
- Concurrent scraping with asyncio and threading
- Data processing with Pandas
- Multiple output formats (CSV, JSON, Excel, Database)
- Command-line interface with Click
- Scheduling and automation with APScheduler
- Comprehensive logging and error handling
- Proxy and user-agent rotation
- Rate limiting and retry logic

## Use Cases

- E-commerce Price Monitoring
- Business Lead Generation
- Content Aggregation

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/web-scraper-pro.git
cd web-scraper-pro

# Install dependencies
pip install -r requirements.txt

# Optional: Install in development mode
pip install -e .
```

## Usage

### Basic Usage

```bash
# Run a scraping job with a configuration file
python -m webscraper scrape --config config/ecommerce.yaml

# Schedule a recurring job
python -m webscraper schedule --config config/ecommerce.yaml --cron "0 */6 * * *"

# List all scheduled jobs
python -m webscraper list-jobs

# Export data to a different format
python -m webscraper export-data --input data/products.csv --output data/products.json
```

### Configuration

Configure your scraping jobs using YAML or JSON files. See `config/examples` for sample configurations.

```yaml
# Example configuration for an e-commerce scraper
scraper:
  type: ecommerce
  urls:
    - https://example.com/products
  selectors:
    product_name: .product-title
    price: .price-now
  output:
    format: csv
    path: ./data/products.csv
```

## Development

### Project Structure

```
web_scraper/
├── config/             # Configuration files
├── data/               # Data output directory
├── logs/               # Log files
├── src/                # Source code
│   ├── scrapers/       # Scraper modules
│   ├── storage/        # Data storage modules
│   ├── processors/     # Data processing modules
│   ├── utils/          # Utility modules
│   ├── schedulers/     # Scheduling modules
│   └── cli/            # CLI interface
├── tests/              # Test modules
└── examples/           # Example use cases
```

### Running Tests

```bash
# Run all tests
pytest

# Run tests with coverage
pytest --cov=webscraper
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.