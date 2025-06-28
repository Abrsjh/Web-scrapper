"""
Validators Module

This module provides validation functions for common data types.
"""

import logging
import re
from typing import Optional, Union
from urllib.parse import urlparse

# Logger
logger = logging.getLogger(__name__)


def is_valid_email(email: str) -> bool:
    """
    Validate an email address.
    
    Args:
        email: Email address to validate
        
    Returns:
        True if the email is valid, False otherwise
    """
    if not email or not isinstance(email, str):
        return False
    
    # Basic email regex pattern
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Check if the email matches the pattern
    if not re.match(pattern, email):
        return False
    
    # Check for common invalid patterns
    invalid_patterns = [
        r'@example\.com$',
        r'@test\.com$',
        r'@sample\.com$',
        r'@invalid\.com$',
    ]
    
    for invalid_pattern in invalid_patterns:
        if re.search(invalid_pattern, email, re.IGNORECASE):
            return False
    
    return True


def is_valid_phone(
    phone: str,
    country_code: Optional[str] = None,
    min_length: int = 7,
    max_length: int = 15
) -> bool:
    """
    Validate a phone number.
    
    Args:
        phone: Phone number to validate
        country_code: Country code for validation (default: None)
        min_length: Minimum length of phone number (default: 7)
        max_length: Maximum length of phone number (default: 15)
        
    Returns:
        True if the phone number is valid, False otherwise
    """
    if not phone or not isinstance(phone, str):
        return False
    
    # Remove all non-digit characters except +
    digits = re.sub(r'[^\d+]', '', phone)
    
    # Check length
    if len(digits) < min_length or len(digits) > max_length:
        return False
    
    # Check if it's a valid international format
    if digits.startswith('+'):
        # International format should have at least 8 characters (e.g., +1234567)
        if len(digits) < 8:
            return False
    
    # Check country code if provided
    if country_code:
        country_codes = {
            'US': ['+1', '1'],
            'UK': ['+44', '44'],
            'CA': ['+1', '1'],
            'AU': ['+61', '61'],
            'IN': ['+91', '91'],
            'DE': ['+49', '49'],
            'FR': ['+33', '33'],
            'JP': ['+81', '81'],
            'BR': ['+55', '55'],
            'RU': ['+7', '7'],
        }
        
        if country_code in country_codes:
            valid_codes = country_codes[country_code]
            if digits.startswith('+'):
                # Check if it starts with the correct country code
                if not any(digits.startswith(code) for code in valid_codes if code.startswith('+')):
                    return False
            else:
                # Check if it starts with the correct country code without +
                if not any(digits.startswith(code) for code in valid_codes if not code.startswith('+')):
                    # If it doesn't start with country code, it might be a local number
                    # which should still have a valid length
                    if len(digits) < min_length:
                        return False
    
    # Check for obviously fake numbers
    fake_patterns = [
        r'^0{7,}$',  # All zeros
        r'^1{7,}$',  # All ones
        r'^(.)\1{6,}$',  # Same digit repeated
        r'^12345\d*$',  # Sequential digits
    ]
    
    for pattern in fake_patterns:
        if re.match(pattern, digits):
            return False
    
    return True


def is_valid_url(url: str) -> bool:
    """
    Validate a URL.
    
    Args:
        url: URL to validate
        
    Returns:
        True if the URL is valid, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    # Parse URL
    try:
        parsed = urlparse(url)
        
        # Check if scheme and netloc are present
        if not parsed.scheme or not parsed.netloc:
            return False
        
        # Check if scheme is valid
        if parsed.scheme not in ['http', 'https', 'ftp']:
            return False
        
        # Check if domain is valid
        domain = parsed.netloc
        if not re.match(r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$', domain):
            return False
        
        # Check for localhost or IP addresses
        if domain == 'localhost' or re.match(r'^\d+\.\d+\.\d+\.\d+$', domain):
            return False
        
        # Check TLD
        tld = domain.split('.')[-1]
        if len(tld) < 2:
            return False
        
        return True
    
    except Exception as e:
        logger.debug(f"URL validation error: {str(e)}")
        return False


def is_valid_date(date_str: str, formats: Optional[list] = None) -> bool:
    """
    Validate a date string.
    
    Args:
        date_str: Date string to validate
        formats: List of date formats to try (default: None)
        
    Returns:
        True if the date is valid, False otherwise
    """
    if not date_str or not isinstance(date_str, str):
        return False
    
    if not formats:
        formats = [
            '%Y-%m-%d',  # ISO format
            '%Y/%m/%d',
            '%m/%d/%Y',
            '%d/%m/%Y',
            '%m-%d-%Y',
            '%d-%m-%Y',
            '%b %d, %Y',  # Month name format
            '%d %b %Y',
            '%B %d, %Y',
            '%d %B %Y',
        ]
    
    from datetime import datetime
    
    for fmt in formats:
        try:
            datetime.strptime(date_str, fmt)
            return True
        except ValueError:
            continue
    
    return False


def is_valid_ip(ip: str) -> bool:
    """
    Validate an IP address.
    
    Args:
        ip: IP address to validate
        
    Returns:
        True if the IP address is valid, False otherwise
    """
    if not ip or not isinstance(ip, str):
        return False
    
    # IPv4 pattern
    ipv4_pattern = r'^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$'
    match = re.match(ipv4_pattern, ip)
    
    if match:
        # Check each octet
        for octet in match.groups():
            if int(octet) > 255:
                return False
        return True
    
    # IPv6 pattern (simplified)
    ipv6_pattern = r'^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$'
    if re.match(ipv6_pattern, ip):
        return True
    
    return False


def is_valid_credit_card(card_number: str) -> bool:
    """
    Validate a credit card number using the Luhn algorithm.
    
    Args:
        card_number: Credit card number to validate
        
    Returns:
        True if the credit card number is valid, False otherwise
    """
    if not card_number or not isinstance(card_number, str):
        return False
    
    # Remove all non-digit characters
    digits = re.sub(r'[^\d]', '', card_number)
    
    # Check length
    if len(digits) < 13 or len(digits) > 19:
        return False
    
    # Luhn algorithm
    total = 0
    reverse = digits[::-1]
    
    for i, digit in enumerate(reverse):
        n = int(digit)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    
    return total % 10 == 0


def is_valid_text(
    text: str,
    min_length: int = 1,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None
) -> bool:
    """
    Validate a text string.
    
    Args:
        text: Text to validate
        min_length: Minimum length of text (default: 1)
        max_length: Maximum length of text (default: None)
        pattern: Regex pattern to match (default: None)
        
    Returns:
        True if the text is valid, False otherwise
    """
    if not isinstance(text, str):
        return False
    
    # Check length
    if len(text) < min_length:
        return False
    
    if max_length is not None and len(text) > max_length:
        return False
    
    # Check pattern if provided
    if pattern and not re.match(pattern, text):
        return False
    
    return True


def is_valid_json(json_str: str) -> bool:
    """
    Validate a JSON string.
    
    Args:
        json_str: JSON string to validate
        
    Returns:
        True if the JSON is valid, False otherwise
    """
    if not json_str or not isinstance(json_str, str):
        return False
    
    import json
    
    try:
        json.loads(json_str)
        return True
    except ValueError:
        return False


def is_valid_numeric(
    value: Union[str, int, float],
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None
) -> bool:
    """
    Validate a numeric value.
    
    Args:
        value: Value to validate
        min_value: Minimum allowed value (default: None)
        max_value: Maximum allowed value (default: None)
        
    Returns:
        True if the value is a valid number within range, False otherwise
    """
    # Convert string to number if needed
    if isinstance(value, str):
        try:
            if '.' in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            return False
    
    # Check if it's a number
    if not isinstance(value, (int, float)):
        return False
    
    # Check range if provided
    if min_value is not None and value < min_value:
        return False
    
    if max_value is not None and value > max_value:
        return False
    
    return True