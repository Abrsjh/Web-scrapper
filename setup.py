from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = fh.read().splitlines()

setup(
    name="webscraper",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="A professional-grade web scraping and automation tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/web-scraper-pro",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/web-scraper-pro/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Internet :: WWW/HTTP :: Indexing/Search",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "webscraper=webscraper.cli.main:main",
        ],
    },
    include_package_data=True,
    zip_safe=False,
)