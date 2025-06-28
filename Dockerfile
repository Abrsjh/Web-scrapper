# Web Scraper Pro - Dockerfile
# This Dockerfile creates a container for running the web scraper

FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app:$PYTHONPATH"

# Install system dependencies (for Chrome/Firefox if needed for Selenium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Chrome for Selenium (if needed)
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
#     && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
#     && apt-get update \
#     && apt-get install -y --no-install-recommends google-chrome-stable \
#     && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files
COPY . .

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/config

# Make sure the package is installed in development mode
RUN pip install -e .

# Create a non-root user to run the scraper
RUN useradd -m scraper
RUN chown -R scraper:scraper /app
USER scraper

# Set entry point
ENTRYPOINT ["python", "-m", "webscraper"]

# Default command (can be overridden)
CMD ["--help"]