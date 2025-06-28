"""
Job Scheduler Module

This module provides job scheduling functionality using APScheduler.
"""

import json
import logging
import os
import smtplib
import sqlite3
import tempfile
import time
import uuid
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import requests
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from ..utils.config import ConfigManager
from ..utils.exceptions import SchedulingError


logger = logging.getLogger(__name__)


class JobScheduler:
    """
    Job scheduler for web scraping tasks.
    
    This class provides functionality for scheduling web scraping jobs
    using APScheduler with cron expressions.
    """
    
    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the job scheduler.
        
        Args:
            db_path: Path to the SQLite database file for job persistence
                    (default: ~/.webscraper/jobs.db)
        """
        # Set up database path
        if db_path is None:
            home_dir = str(Path.home())
            db_dir = os.path.join(home_dir, '.webscraper')
            os.makedirs(db_dir, exist_ok=True)
            db_path = os.path.join(db_dir, 'jobs.db')
        
        self.db_path = db_path
        
        # Set up database tables if they don't exist
        self._setup_database()
        
        # Set up APScheduler
        self.scheduler = BackgroundScheduler(
            jobstores={
                'default': SQLAlchemyJobStore(url=f'sqlite:///{db_path}')
            },
            executors={
                'default': ThreadPoolExecutor(20)
            },
            job_defaults={
                'coalesce': False,
                'max_instances': 3
            }
        )
        
        # Start the scheduler
        self.scheduler.start()
        
        logger.info(f"Job scheduler initialized with database at {db_path}")
    
    def _setup_database(self) -> None:
        """
        Set up the database tables for job management.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create job_metadata table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_metadata (
            job_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            config_path TEXT NOT NULL,
            cron_expression TEXT NOT NULL,
            email TEXT,
            webhook TEXT,
            created_at TEXT NOT NULL,
            last_run TEXT,
            last_status TEXT,
            metadata TEXT
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def _save_job_metadata(
        self,
        job_id: str,
        name: str,
        config_path: str,
        cron_expression: str,
        email: Optional[str] = None,
        webhook: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Save job metadata to the database.
        
        Args:
            job_id: Unique ID of the job
            name: Name of the job
            config_path: Path to the configuration file
            cron_expression: Cron expression for scheduling
            email: Email address for notifications (default: None)
            webhook: Webhook URL for notifications (default: None)
            metadata: Additional metadata (default: None)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Convert metadata to JSON if provided
        metadata_json = json.dumps(metadata) if metadata else None
        
        # Insert or replace job metadata
        cursor.execute(
            '''
            INSERT OR REPLACE INTO job_metadata
            (job_id, name, config_path, cron_expression, email, webhook, created_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                job_id,
                name,
                config_path,
                cron_expression,
                email,
                webhook,
                datetime.now().isoformat(),
                metadata_json
            )
        )
        
        conn.commit()
        conn.close()
    
    def _update_job_status(
        self,
        job_id: str,
        status: str
    ) -> None:
        """
        Update job status in the database.
        
        Args:
            job_id: Unique ID of the job
            status: Status of the job ('success', 'failure', etc.)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Update job status
        cursor.execute(
            '''
            UPDATE job_metadata
            SET last_run = ?, last_status = ?
            WHERE job_id = ?
            ''',
            (
                datetime.now().isoformat(),
                status,
                job_id
            )
        )
        
        conn.commit()
        conn.close()
    
    def _execute_scraping_job(
        self,
        job_id: str,
        config_path: str,
        email: Optional[str] = None,
        webhook: Optional[str] = None
    ) -> None:
        """
        Execute a scraping job.
        
        Args:
            job_id: Unique ID of the job
            config_path: Path to the configuration file
            email: Email address for notifications (default: None)
            webhook: Webhook URL for notifications (default: None)
        """
        try:
            logger.info(f"Executing job {job_id} with config {config_path}")
            
            # Load configuration
            config_manager = ConfigManager(config_path)
            scraper_config = config_manager.get_config()
            
            # Initialize scraper
            from ..scrapers import get_scraper
            scraper = get_scraper(
                scraper_config['scraper']['type'],
                scraper_config['scraper']
            )
            
            # Run scraper
            start_time = time.time()
            results = scraper.scrape()
            end_time = time.time()
            
            # Get report
            report = scraper.get_report()
            report['duration'] = end_time - start_time
            
            # Update job status
            self._update_job_status(job_id, 'success')
            
            # Send notifications
            self._send_notifications(
                job_id=job_id,
                status='success',
                results=results,
                report=report,
                email=email,
                webhook=webhook
            )
            
            logger.info(f"Job {job_id} completed successfully")
            
        except Exception as e:
            logger.error(f"Error executing job {job_id}: {str(e)}")
            
            # Update job status
            self._update_job_status(job_id, 'failure')
            
            # Send failure notifications
            self._send_notifications(
                job_id=job_id,
                status='failure',
                error=str(e),
                email=email,
                webhook=webhook
            )
    
    def _send_notifications(
        self,
        job_id: str,
        status: str,
        results: Optional[List[Dict[str, Any]]] = None,
        report: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
        email: Optional[str] = None,
        webhook: Optional[str] = None
    ) -> None:
        """
        Send notifications for job completion or failure.
        
        Args:
            job_id: Unique ID of the job
            status: Status of the job ('success', 'failure')
            results: Scraping results (default: None)
            report: Scraping report (default: None)
            error: Error message (default: None)
            email: Email address for notifications (default: None)
            webhook: Webhook URL for notifications (default: None)
        """
        # Get job metadata
        job_metadata = self._get_job_metadata(job_id)
        
        if not job_metadata:
            logger.warning(f"Job metadata not found for job {job_id}")
            return
        
        # Send email notification if configured
        if email:
            try:
                self._send_email_notification(
                    email=email,
                    job_id=job_id,
                    job_name=job_metadata['name'],
                    status=status,
                    results=results,
                    report=report,
                    error=error
                )
            except Exception as e:
                logger.error(f"Error sending email notification for job {job_id}: {str(e)}")
        
        # Send webhook notification if configured
        if webhook:
            try:
                self._send_webhook_notification(
                    webhook=webhook,
                    job_id=job_id,
                    job_name=job_metadata['name'],
                    status=status,
                    results=results,
                    report=report,
                    error=error
                )
            except Exception as e:
                logger.error(f"Error sending webhook notification for job {job_id}: {str(e)}")
    
    def _send_email_notification(
        self,
        email: str,
        job_id: str,
        job_name: str,
        status: str,
        results: Optional[List[Dict[str, Any]]] = None,
        report: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Send an email notification.
        
        Args:
            email: Email address to send to
            job_id: Unique ID of the job
            job_name: Name of the job
            status: Status of the job ('success', 'failure')
            results: Scraping results (default: None)
            report: Scraping report (default: None)
            error: Error message (default: None)
        """
        # Get email configuration from environment variables
        smtp_server = os.environ.get('WEBSCRAPER_SMTP_SERVER', 'smtp.gmail.com')
        smtp_port = int(os.environ.get('WEBSCRAPER_SMTP_PORT', '587'))
        smtp_username = os.environ.get('WEBSCRAPER_SMTP_USERNAME')
        smtp_password = os.environ.get('WEBSCRAPER_SMTP_PASSWORD')
        
        if not smtp_username or not smtp_password:
            logger.warning("SMTP credentials not configured, skipping email notification")
            return
        
        # Create message
        msg = MIMEMultipart()
        msg['From'] = smtp_username
        msg['To'] = email
        
        if status == 'success':
            msg['Subject'] = f"Web Scraper Job '{job_name}' Completed Successfully"
            
            # Create email body
            body = f"""
            <html>
            <body>
                <h2>Web Scraper Job Completed Successfully</h2>
                <p><strong>Job ID:</strong> {job_id}</p>
                <p><strong>Job Name:</strong> {job_name}</p>
                <p><strong>Status:</strong> {status}</p>
                <p><strong>Timestamp:</strong> {datetime.now().isoformat()}</p>
                
                <h3>Report</h3>
                <ul>
            """
            
            if report:
                for key, value in report.items():
                    body += f"<li><strong>{key}:</strong> {value}</li>"
            
            body += """
                </ul>
                
                <p>The full results are attached to this email.</p>
            </body>
            </html>
            """
            
            # Attach results as JSON file
            if results:
                # Create temporary file for results
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                    json.dump(results, temp_file, indent=2)
                    temp_file_path = temp_file.name
                
                # Attach file
                with open(temp_file_path, 'r') as f:
                    attachment = MIMEText(f.read())
                    attachment.add_header('Content-Disposition', 'attachment', filename='results.json')
                    msg.attach(attachment)
                
                # Clean up temporary file
                os.unlink(temp_file_path)
        
        else:
            msg['Subject'] = f"Web Scraper Job '{job_name}' Failed"
            
            # Create email body
            body = f"""
            <html>
            <body>
                <h2>Web Scraper Job Failed</h2>
                <p><strong>Job ID:</strong> {job_id}</p>
                <p><strong>Job Name:</strong> {job_name}</p>
                <p><strong>Status:</strong> {status}</p>
                <p><strong>Timestamp:</strong> {datetime.now().isoformat()}</p>
                
                <h3>Error</h3>
                <pre>{error}</pre>
            </body>
            </html>
            """
        
        # Attach body to message
        msg.attach(MIMEText(body, 'html'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        
        logger.info(f"Email notification sent to {email} for job {job_id}")
    
    def _send_webhook_notification(
        self,
        webhook: str,
        job_id: str,
        job_name: str,
        status: str,
        results: Optional[List[Dict[str, Any]]] = None,
        report: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Send a webhook notification.
        
        Args:
            webhook: Webhook URL
            job_id: Unique ID of the job
            job_name: Name of the job
            status: Status of the job ('success', 'failure')
            results: Scraping results (default: None)
            report: Scraping report (default: None)
            error: Error message (default: None)
        """
        # Prepare data
        data = {
            'job_id': job_id,
            'job_name': job_name,
            'status': status,
            'timestamp': datetime.now().isoformat()
        }
        
        if status == 'success':
            data['report'] = report
            
            # Include results if not too large
            if results and len(json.dumps(results)) <= 1024 * 1024:  # 1MB limit
                data['results'] = results
            else:
                data['results_summary'] = {
                    'count': len(results) if results else 0,
                    'message': 'Results too large to include in webhook notification'
                }
        else:
            data['error'] = error
        
        # Send webhook
        response = requests.post(
            webhook,
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Webhook notification sent to {webhook} for job {job_id}")
        else:
            logger.warning(f"Webhook notification failed for job {job_id}: {response.status_code} {response.text}")
    
    def _get_job_metadata(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job metadata from the database.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            Job metadata or None if not found
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute(
            '''
            SELECT * FROM job_metadata
            WHERE job_id = ?
            ''',
            (job_id,)
        )
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            metadata = dict(row)
            
            # Parse JSON metadata if present
            if metadata.get('metadata'):
                metadata['metadata'] = json.loads(metadata['metadata'])
            
            return metadata
        
        return None
    
    def schedule_job(
        self,
        name: str,
        config_path: str,
        cron_expression: str,
        email: Optional[str] = None,
        webhook: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Schedule a scraping job.
        
        Args:
            name: Name of the job
            config_path: Path to the configuration file
            cron_expression: Cron expression for scheduling
            email: Email address for notifications (default: None)
            webhook: Webhook URL for notifications (default: None)
            metadata: Additional metadata (default: None)
            
        Returns:
            Unique ID of the scheduled job
            
        Raises:
            SchedulingError: If the job cannot be scheduled
        """
        try:
            # Validate config path
            if not os.path.exists(config_path):
                error_msg = f"Configuration file not found: {config_path}"
                logger.error(error_msg)
                raise SchedulingError(error_msg)
            
            # Validate email if provided
            if email:
                from ..utils.validators import is_valid_email
                if not is_valid_email(email):
                    error_msg = f"Invalid email address: {email}"
                    logger.error(error_msg)
                    raise SchedulingError(error_msg)
            
            # Validate webhook if provided
            if webhook:
                from ..utils.validators import is_valid_url
                if not is_valid_url(webhook):
                    error_msg = f"Invalid webhook URL: {webhook}"
                    logger.error(error_msg)
                    raise SchedulingError(error_msg)
            
            # Generate job ID
            job_id = str(uuid.uuid4())
            
            # Save job metadata
            self._save_job_metadata(
                job_id=job_id,
                name=name,
                config_path=config_path,
                cron_expression=cron_expression,
                email=email,
                webhook=webhook,
                metadata=metadata
            )
            
            # Schedule job with APScheduler
            self.scheduler.add_job(
                func=self._execute_scraping_job,
                trigger=CronTrigger.from_crontab(cron_expression),
                id=job_id,
                args=[job_id, config_path, email, webhook],
                replace_existing=True
            )
            
            logger.info(f"Scheduled job {job_id} with cron expression '{cron_expression}'")
            return job_id
            
        except Exception as e:
            if isinstance(e, SchedulingError):
                raise
            
            error_msg = f"Failed to schedule job: {str(e)}"
            logger.error(error_msg)
            raise SchedulingError(error_msg) from e
    
    def get_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all scheduled jobs.
        
        Returns:
            List of job metadata
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM job_metadata')
        rows = cursor.fetchall()
        conn.close()
        
        jobs = []
        for row in rows:
            job = dict(row)
            
            # Parse JSON metadata if present
            if job.get('metadata'):
                job['metadata'] = json.loads(job['metadata'])
            
            # Get next run time
            ap_job = self.scheduler.get_job(job['job_id'])
            if ap_job:
                job['next_run_time'] = ap_job.next_run_time.isoformat() if ap_job.next_run_time else None
            else:
                job['next_run_time'] = None
            
            jobs.append(job)
        
        return jobs
    
    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific job by ID.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            Job metadata or None if not found
        """
        job = self._get_job_metadata(job_id)
        
        if job:
            # Get next run time
            ap_job = self.scheduler.get_job(job_id)
            if ap_job:
                job['next_run_time'] = ap_job.next_run_time.isoformat() if ap_job.next_run_time else None
            else:
                job['next_run_time'] = None
        
        return job
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a scheduled job.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            True if the job was deleted, False otherwise
        """
        # Check if job exists
        if not self._get_job_metadata(job_id):
            return False
        
        # Remove job from APScheduler
        self.scheduler.remove_job(job_id)
        
        # Remove job from database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute(
            '''
            DELETE FROM job_metadata
            WHERE job_id = ?
            ''',
            (job_id,)
        )
        
        conn.commit()
        conn.close()
        
        logger.info(f"Deleted job {job_id}")
        return True
    
    def run_job_now(self, job_id: str) -> bool:
        """
        Run a scheduled job immediately.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            True if the job was run, False otherwise
        """
        # Check if job exists
        job = self._get_job_metadata(job_id)
        if not job:
            return False
        
        # Run job
        self._execute_scraping_job(
            job_id=job_id,
            config_path=job['config_path'],
            email=job['email'],
            webhook=job['webhook']
        )
        
        logger.info(f"Ran job {job_id} immediately")
        return True
    
    def pause_job(self, job_id: str) -> bool:
        """
        Pause a scheduled job.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            True if the job was paused, False otherwise
        """
        # Check if job exists
        if not self._get_job_metadata(job_id):
            return False
        
        # Pause job
        self.scheduler.pause_job(job_id)
        
        logger.info(f"Paused job {job_id}")
        return True
    
    def resume_job(self, job_id: str) -> bool:
        """
        Resume a paused job.
        
        Args:
            job_id: Unique ID of the job
            
        Returns:
            True if the job was resumed, False otherwise
        """
        # Check if job exists
        if not self._get_job_metadata(job_id):
            return False
        
        # Resume job
        self.scheduler.resume_job(job_id)
        
        logger.info(f"Resumed job {job_id}")
        return True
    
    def update_job(
        self,
        job_id: str,
        name: Optional[str] = None,
        config_path: Optional[str] = None,
        cron_expression: Optional[str] = None,
        email: Optional[str] = None,
        webhook: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Update a scheduled job.
        
        Args:
            job_id: Unique ID of the job
            name: New name of the job (default: None)
            config_path: New path to the configuration file (default: None)
            cron_expression: New cron expression for scheduling (default: None)
            email: New email address for notifications (default: None)
            webhook: New webhook URL for notifications (default: None)
            metadata: New additional metadata (default: None)
            
        Returns:
            True if the job was updated, False otherwise
            
        Raises:
            SchedulingError: If the job cannot be updated
        """
        try:
            # Check if job exists
            job = self._get_job_metadata(job_id)
            if not job:
                return False
            
            # Validate config path if provided
            if config_path and not os.path.exists(config_path):
                error_msg = f"Configuration file not found: {config_path}"
                logger.error(error_msg)
                raise SchedulingError(error_msg)
            
            # Validate email if provided
            if email:
                from ..utils.validators import is_valid_email
                if not is_valid_email(email):
                    error_msg = f"Invalid email address: {email}"
                    logger.error(error_msg)
                    raise SchedulingError(error_msg)
            
            # Validate webhook if provided
            if webhook:
                from ..utils.validators import is_valid_url
                if not is_valid_url(webhook):
                    error_msg = f"Invalid webhook URL: {webhook}"
                    logger.error(error_msg)
                    raise SchedulingError(error_msg)
            
            # Update job metadata
            new_name = name if name is not None else job['name']
            new_config_path = config_path if config_path is not None else job['config_path']
            new_cron_expression = cron_expression if cron_expression is not None else job['cron_expression']
            new_email = email if email is not None else job['email']
            new_webhook = webhook if webhook is not None else job['webhook']
            new_metadata = metadata if metadata is not None else job.get('metadata')
            
            self._save_job_metadata(
                job_id=job_id,
                name=new_name,
                config_path=new_config_path,
                cron_expression=new_cron_expression,
                email=new_email,
                webhook=new_webhook,
                metadata=new_metadata
            )
            
            # Update job schedule if cron expression changed
            if cron_expression:
                self.scheduler.reschedule_job(
                    job_id=job_id,
                    trigger=CronTrigger.from_crontab(cron_expression)
                )
            
            # Update job arguments if config path, email, or webhook changed
            if config_path or email is not None or webhook is not None:
                self.scheduler.modify_job(
                    job_id=job_id,
                    args=[job_id, new_config_path, new_email, new_webhook]
                )
            
            logger.info(f"Updated job {job_id}")
            return True
            
        except Exception as e:
            if isinstance(e, SchedulingError):
                raise
            
            error_msg = f"Failed to update job: {str(e)}"
            logger.error(error_msg)
            raise SchedulingError(error_msg) from e
    
    def shutdown(self, wait: bool = True) -> None:
        """
        Shut down the scheduler.
        
        Args:
            wait: Whether to wait for running jobs to complete (default: True)
        """
        self.scheduler.shutdown(wait=wait)
        logger.info("Scheduler shut down")