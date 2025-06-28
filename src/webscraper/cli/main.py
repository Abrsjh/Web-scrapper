"""
CLI Main Module

This module provides the command-line interface for the web scraper.
"""

import logging
import os
import sys
from typing import Any, Dict, List, Optional

import click

from ..utils.config import ConfigManager, create_default_config
from ..utils.exceptions import WebScraperError


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


@click.group()
@click.version_option(version='0.1.0', prog_name='Web Scraper Pro')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.option('--quiet', '-q', is_flag=True, help='Suppress all output except errors')
@click.option('--log-file', '-l', type=str, help='Log to file')
@click.pass_context
def cli(ctx: click.Context, verbose: bool, quiet: bool, log_file: Optional[str]) -> None:
    """
    Web Scraper Pro - A professional-grade web scraping and automation tool.
    
    This tool provides commands for scraping websites, scheduling scraping jobs,
    and exporting data in various formats.
    """
    # Set up context object with common options
    ctx.ensure_object(dict)
    ctx.obj['verbose'] = verbose
    ctx.obj['quiet'] = quiet
    
    # Configure logging level based on options
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    elif quiet:
        logging.getLogger().setLevel(logging.ERROR)
    
    # Add file handler if log file is specified
    if log_file:
        try:
            # Ensure directory exists
            log_dir = os.path.dirname(os.path.abspath(log_file))
            os.makedirs(log_dir, exist_ok=True)
            
            # Add file handler
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            logging.getLogger().addHandler(file_handler)
            
            ctx.obj['log_file'] = log_file
        except Exception as e:
            logger.error(f"Failed to set up logging to file {log_file}: {str(e)}")


@cli.command()
@click.option('--config', '-c', required=True, type=click.Path(exists=True), help='Path to configuration file')
@click.option('--output', '-o', type=click.Path(), help='Path to output file (overrides config)')
@click.option('--format', '-f', type=str, help='Output format (overrides config)')
@click.option('--proxy', '-p', type=str, help='Proxy URL')
@click.option('--user-agent', '-u', type=str, help='User agent string')
@click.option('--delay', '-d', type=int, help='Delay between requests in seconds')
@click.option('--timeout', '-t', type=int, help='Request timeout in seconds')
@click.option('--retries', '-r', type=int, help='Number of retry attempts')
@click.option('--concurrent', type=int, help='Maximum number of concurrent requests')
@click.pass_context
def scrape(
    ctx: click.Context,
    config: str,
    output: Optional[str] = None,
    format: Optional[str] = None,
    proxy: Optional[str] = None,
    user_agent: Optional[str] = None,
    delay: Optional[int] = None,
    timeout: Optional[int] = None,
    retries: Optional[int] = None,
    concurrent: Optional[int] = None
) -> None:
    """
    Scrape websites based on configuration.
    
    This command loads the specified configuration file and runs the scraper
    with the given options. Options specified on the command line override
    those in the configuration file.
    """
    try:
        # Load configuration
        config_manager = ConfigManager(config)
        scraper_config = config_manager.get_config()
        
        # Override configuration with command-line options
        if output:
            config_manager.set('scraper.output.path', output)
        
        if format:
            config_manager.set('scraper.output.format', format)
        
        if proxy:
            config_manager.set('scraper.proxy', proxy)
        
        if user_agent:
            config_manager.set('scraper.user_agent', user_agent)
        
        if delay is not None:
            config_manager.set('scraper.delay', delay)
        
        if timeout is not None:
            config_manager.set('scraper.timeout', timeout)
        
        if retries is not None:
            config_manager.set('scraper.retries', retries)
        
        if concurrent is not None:
            config_manager.set('scraper.max_concurrent', concurrent)
        
        # Get updated configuration
        scraper_config = config_manager.get_config()
        
        # Initialize scraper
        from ..scrapers import get_scraper
        scraper = get_scraper(
            scraper_config['scraper']['type'],
            scraper_config['scraper']
        )
        
        # Display scraper info
        if ctx.obj['verbose']:
            click.echo(f"Scraper: {scraper.__class__.__name__}")
            click.echo(f"URLs: {len(scraper.urls)}")
            click.echo(f"Output: {scraper_config['scraper'].get('output', {}).get('format', 'None')} -> "
                     f"{scraper_config['scraper'].get('output', {}).get('path', 'None')}")
        
        # Run scraper
        click.echo("Starting scraper...")
        results = scraper.scrape()
        
        # Display results
        click.echo(f"Scraping completed. Extracted {len(results)} items.")
        
        # Display report
        report = scraper.get_report()
        if ctx.obj['verbose']:
            click.echo("\nScraping Report:")
            for key, value in report.items():
                click.echo(f"  {key}: {value}")
    
    except WebScraperError as e:
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--config', '-c', required=True, type=click.Path(exists=True), help='Path to configuration file')
@click.option('--cron', required=True, type=str, help='Cron expression for scheduling')
@click.option('--name', '-n', required=True, type=str, help='Name of the job')
@click.option('--email', '-e', type=str, help='Email address for notifications')
@click.option('--webhook', '-w', type=str, help='Webhook URL for notifications')
@click.pass_context
def schedule(
    ctx: click.Context,
    config: str,
    cron: str,
    name: str,
    email: Optional[str] = None,
    webhook: Optional[str] = None
) -> None:
    """
    Schedule a scraping job.
    
    This command schedules a scraping job to run at specified intervals
    using a cron expression. It can also send notifications via email
    or webhook when the job completes.
    """
    try:
        # Import scheduler
        from ..schedulers import JobScheduler
        
        # Initialize scheduler
        scheduler = JobScheduler()
        
        # Schedule job
        job_id = scheduler.schedule_job(
            name=name,
            config_path=config,
            cron_expression=cron,
            email=email,
            webhook=webhook
        )
        
        click.echo(f"Job scheduled successfully with ID: {job_id}")
        click.echo(f"Cron expression: {cron}")
        if email:
            click.echo(f"Email notifications: {email}")
        if webhook:
            click.echo(f"Webhook notifications: {webhook}")
        
    except WebScraperError as e:
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


@cli.command(name='list-jobs')
@click.pass_context
def list_jobs(ctx: click.Context) -> None:
    """
    List all scheduled jobs.
    
    This command displays all currently scheduled scraping jobs
    with their details.
    """
    try:
        # Import scheduler
        from ..schedulers import JobScheduler
        
        # Initialize scheduler
        scheduler = JobScheduler()
        
        # Get jobs
        jobs = scheduler.get_jobs()
        
        if not jobs:
            click.echo("No scheduled jobs found.")
            return
        
        # Display jobs
        click.echo(f"Scheduled jobs ({len(jobs)}):")
        for job in jobs:
            click.echo(f"\nJob ID: {job['id']}")
            click.echo(f"Name: {job['name']}")
            click.echo(f"Cron: {job['cron_expression']}")
            click.echo(f"Config: {job['config_path']}")
            click.echo(f"Next run: {job['next_run_time']}")
            
            if job.get('email'):
                click.echo(f"Email: {job['email']}")
            
            if job.get('webhook'):
                click.echo(f"Webhook: {job['webhook']}")
        
    except WebScraperError as e:
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


@cli.command(name='export-data')
@click.option('--input', '-i', required=True, type=click.Path(exists=True), help='Path to input file')
@click.option('--output', '-o', required=True, type=click.Path(), help='Path to output file')
@click.option('--format', '-f', required=True, type=str, help='Output format (csv, json, excel, db)')
@click.pass_context
def export_data(ctx: click.Context, input: str, output: str, format: str) -> None:
    """
    Export data to a different format.
    
    This command exports data from one format to another,
    such as converting CSV to JSON or JSON to Excel.
    """
    try:
        # Determine input format from file extension
        _, input_ext = os.path.splitext(input)
        input_ext = input_ext.lower()[1:]  # Remove the dot
        
        if input_ext not in ['csv', 'json', 'xlsx', 'xls', 'db', 'sqlite']:
            raise ValueError(f"Unsupported input format: {input_ext}")
        
        # Normalize format names
        format_map = {
            'csv': 'csv',
            'json': 'json',
            'xlsx': 'excel',
            'xls': 'excel',
            'excel': 'excel',
            'db': 'database',
            'sqlite': 'database',
            'database': 'database'
        }
        
        input_format = format_map.get(input_ext, input_ext)
        output_format = format_map.get(format, format)
        
        # Import storage handlers
        from ..storage import get_storage_handler
        
        # Initialize handlers
        input_handler = get_storage_handler(input_format)
        output_handler = get_storage_handler(output_format)
        
        # Load data
        click.echo(f"Loading data from {input}...")
        data = input_handler.load(input)
        
        # Save data
        click.echo(f"Exporting {len(data)} records to {output}...")
        output_handler.save(data, output)
        
        click.echo(f"Data exported successfully to {output}")
        
    except WebScraperError as e:
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


@cli.command(name='create-config')
@click.option('--output', '-o', required=True, type=click.Path(), help='Path to output configuration file')
@click.option('--type', '-t', required=True, type=click.Choice(['ecommerce', 'business', 'content']), help='Type of scraper')
@click.pass_context
def create_config(ctx: click.Context, output: str, type: str) -> None:
    """
    Create a default configuration file.
    
    This command creates a default configuration file for the specified
    scraper type. The configuration can be modified as needed.
    """
    try:
        # Create configuration
        config = create_default_config(output, type)
        
        click.echo(f"Created default {type} configuration at {output}")
        
    except WebScraperError as e:
        logger.error(str(e))
        click.echo(f"Error: {str(e)}", err=True)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unexpected error")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


def main() -> None:
    """
    Main entry point for the CLI.
    """
    try:
        cli(obj={})
    except Exception as e:
        logger.exception("Unexpected error in CLI")
        click.echo(f"Unexpected error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    main()