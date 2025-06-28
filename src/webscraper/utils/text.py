"""
Text Processing Module

This module provides utilities for processing and cleaning text.
"""

import logging
import re
from typing import List, Optional

# Logger
logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace and normalizing characters.
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    
    # Replace multiple whitespace with single space
    cleaned = re.sub(r'\s+', ' ', text)
    
    # Remove leading and trailing whitespace
    cleaned = cleaned.strip()
    
    # Replace non-breaking spaces with regular spaces
    cleaned = cleaned.replace('\xa0', ' ')
    
    # Normalize quotes
    cleaned = cleaned.replace('"', '"').replace('"', '"')
    cleaned = cleaned.replace(''', "'").replace(''', "'")
    
    # Remove control characters
    cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', cleaned)
    
    return cleaned


def clean_html(html_text: str) -> str:
    """
    Clean HTML text by removing tags and entities.
    
    Args:
        html_text: HTML text to clean
        
    Returns:
        Plain text without HTML tags or entities
    """
    if not html_text:
        return ""
    
    # Try to use BeautifulSoup if available
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')
        text = soup.get_text()
        return clean_text(text)
    except ImportError:
        # Fallback to regex-based cleaning if BeautifulSoup is not available
        pass
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', html_text)
    
    # Remove HTML entities
    text = re.sub(r'&[a-zA-Z]+;', ' ', text)
    text = re.sub(r'&#\d+;', ' ', text)
    
    # Clean the resulting text
    return clean_text(text)


def summarize_text(text: str, max_length: int = 150, prefer_sentences: bool = True) -> str:
    """
    Generate a summary of the text.
    
    Args:
        text: Text to summarize
        max_length: Maximum length of the summary in characters
        prefer_sentences: Whether to try to keep complete sentences
        
    Returns:
        Summarized text
    """
    if not text:
        return ""
    
    # Clean the text first
    cleaned_text = clean_text(text)
    
    # If text is already shorter than max_length, return it as is
    if len(cleaned_text) <= max_length:
        return cleaned_text
    
    if prefer_sentences:
        # Try to find sentence boundaries
        sentences = re.split(r'(?<=[.!?])\s+', cleaned_text)
        
        summary = ""
        for sentence in sentences:
            if len(summary) + len(sentence) + 1 <= max_length:
                summary += sentence + " "
            else:
                break
        
        summary = summary.strip()
        
        # If we couldn't include at least one sentence, fall back to simple truncation
        if not summary:
            summary = cleaned_text[:max_length].rstrip()
            
            # Try to avoid cutting off in the middle of a word
            if len(summary) < len(cleaned_text) and summary[-1] != ' ' and cleaned_text[len(summary)] != ' ':
                # Find the last space
                last_space = summary.rfind(' ')
                if last_space > 0:
                    summary = summary[:last_space]
            
            summary += "..."
        
        return summary
    else:
        # Simple truncation
        summary = cleaned_text[:max_length].rstrip()
        
        # Try to avoid cutting off in the middle of a word
        if len(summary) < len(cleaned_text) and summary[-1] != ' ' and cleaned_text[len(summary)] != ' ':
            # Find the last space
            last_space = summary.rfind(' ')
            if last_space > 0:
                summary = summary[:last_space]
        
        summary += "..."
        
        return summary


def extract_keywords(text: str, max_keywords: int = 5) -> List[str]:
    """
    Extract keywords from text.
    
    This is a simple implementation that extracts the most frequent words
    after removing stop words.
    
    Args:
        text: Text to extract keywords from
        max_keywords: Maximum number of keywords to extract
        
    Returns:
        List of keywords
    """
    if not text:
        return []
    
    # Clean the text
    cleaned_text = clean_text(text.lower())
    
    # Define common English stop words
    stop_words = {
        'a', 'an', 'the', 'and', 'or', 'but', 'if', 'because', 'as', 'what',
        'which', 'this', 'that', 'these', 'those', 'then', 'just', 'so', 'than',
        'such', 'both', 'through', 'about', 'for', 'is', 'of', 'while', 'during',
        'to', 'from', 'in', 'on', 'by', 'at', 'with', 'about', 'against', 'between',
        'into', 'through', 'during', 'before', 'after', 'above', 'below', 'up',
        'down', 'out', 'off', 'over', 'under', 'again', 'further', 'then', 'once',
        'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both',
        'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
        'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will',
        'don', 'should', 'now', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours',
        'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him',
        'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself',
        'they', 'them', 'their', 'theirs', 'themselves', 'am', 'is', 'are', 'was',
        'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does',
        'did', 'doing', 'would', 'should', 'could', 'ought', 'm', 're', 've', 'll',
        'd', 't'
    }
    
    # Split into words
    words = re.findall(r'\b\w+\b', cleaned_text)
    
    # Remove stop words and short words
    filtered_words = [word for word in words if word not in stop_words and len(word) > 2]
    
    # Count word frequencies
    word_freq = {}
    for word in filtered_words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    # Sort by frequency
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    
    # Extract the top keywords
    keywords = [word for word, freq in sorted_words[:max_keywords]]
    
    return keywords


def extract_entities(text: str) -> dict:
    """
    Extract named entities from text (people, organizations, locations).
    
    This is a simple implementation that uses regex patterns to extract
    common entity types.
    
    Args:
        text: Text to extract entities from
        
    Returns:
        Dictionary with entity types as keys and lists of entities as values
    """
    if not text:
        return {}
    
    entities = {
        'people': [],
        'organizations': [],
        'locations': [],
        'dates': [],
        'emails': [],
        'urls': [],
        'phone_numbers': []
    }
    
    # Extract emails
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    entities['emails'] = re.findall(email_pattern, text)
    
    # Extract URLs
    url_pattern = r'https?://[^\s]+'
    entities['urls'] = re.findall(url_pattern, text)
    
    # Extract phone numbers (simple pattern)
    phone_pattern = r'\b(?:\+\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b'
    entities['phone_numbers'] = re.findall(phone_pattern, text)
    
    # Extract dates (simple patterns)
    date_patterns = [
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',  # MM/DD/YYYY
        r'\b\d{4}-\d{1,2}-\d{1,2}\b',     # YYYY-MM-DD
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]* \d{1,2},? \d{4}\b'  # Month DD, YYYY
    ]
    
    all_dates = []
    for pattern in date_patterns:
        all_dates.extend(re.findall(pattern, text, re.IGNORECASE))
    
    entities['dates'] = all_dates
    
    # Try to use more advanced NLP if available
    try:
        import nltk
        from nltk import ne_chunk, pos_tag, word_tokenize
        from nltk.tree import Tree
        
        # Download necessary resources if not already downloaded
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt', quiet=True)
        
        try:
            nltk.data.find('taggers/maxent_treebank_pos_tagger')
        except LookupError:
            nltk.download('averaged_perceptron_tagger', quiet=True)
        
        try:
            nltk.data.find('chunkers/maxent_ne_chunker')
        except LookupError:
            nltk.download('maxent_ne_chunker', quiet=True)
        
        try:
            nltk.data.find('corpora/words')
        except LookupError:
            nltk.download('words', quiet=True)
        
        # Tokenize, POS tag, and NE chunk
        tokens = word_tokenize(text)
        pos_tags = pos_tag(tokens)
        ne_tree = ne_chunk(pos_tags)
        
        # Extract named entities
        for chunk in ne_tree:
            if isinstance(chunk, Tree):
                entity_text = ' '.join([token for token, pos in chunk.leaves()])
                if chunk.label() == 'PERSON':
                    entities['people'].append(entity_text)
                elif chunk.label() == 'ORGANIZATION':
                    entities['organizations'].append(entity_text)
                elif chunk.label() == 'GPE' or chunk.label() == 'LOCATION':
                    entities['locations'].append(entity_text)
        
    except (ImportError, ModuleNotFoundError):
        # Fallback to simple regex patterns if NLTK is not available
        logger.debug("NLTK not available, using regex patterns for entity extraction")
        
        # Simple patterns for people (Mr./Ms./Dr. followed by capitalized words)
        people_patterns = [
            r'Mr\.\s+[A-Z][a-z]+(?: [A-Z][a-z]+)*',
            r'Ms\.\s+[A-Z][a-z]+(?: [A-Z][a-z]+)*',
            r'Mrs\.\s+[A-Z][a-z]+(?: [A-Z][a-z]+)*',
            r'Dr\.\s+[A-Z][a-z]+(?: [A-Z][a-z]+)*',
            r'Prof\.\s+[A-Z][a-z]+(?: [A-Z][a-z]+)*',
        ]
        
        for pattern in people_patterns:
            entities['people'].extend(re.findall(pattern, text))
        
        # Simple pattern for organizations (consecutive capitalized words ending with Inc, Corp, etc.)
        org_pattern = r'(?:[A-Z][a-z]*\s+)+(?:Inc|Corp|Corporation|LLC|Company|Ltd|Limited|Association|Foundation|Institute)'
        entities['organizations'].extend(re.findall(org_pattern, text))
        
        # Simple pattern for locations (common location words)
        location_indicators = [
            r'in [A-Z][a-z]+(?:,\s+[A-Z][a-z]+)*',
            r'at [A-Z][a-z]+(?:,\s+[A-Z][a-z]+)*',
            r'from [A-Z][a-z]+(?:,\s+[A-Z][a-z]+)*',
            r'to [A-Z][a-z]+(?:,\s+[A-Z][a-z]+)*',
        ]
        
        for pattern in location_indicators:
            matches = re.findall(pattern, text)
            # Extract just the location part
            for match in matches:
                parts = match.split(' ', 1)
                if len(parts) > 1:
                    entities['locations'].append(parts[1])
    
    # Remove duplicates while preserving order
    for entity_type in entities:
        seen = set()
        entities[entity_type] = [x for x in entities[entity_type] if not (x in seen or seen.add(x))]
    
    return entities


def word_count(text: str) -> int:
    """
    Count the number of words in text.
    
    Args:
        text: Text to count words in
        
    Returns:
        Number of words
    """
    if not text:
        return 0
    
    # Clean the text
    cleaned_text = clean_text(text)
    
    # Split into words and count
    words = re.findall(r'\b\w+\b', cleaned_text)
    
    return len(words)


def estimate_reading_time(text: str, words_per_minute: int = 200) -> int:
    """
    Estimate reading time in minutes.
    
    Args:
        text: Text to estimate reading time for
        words_per_minute: Average reading speed in words per minute
        
    Returns:
        Estimated reading time in minutes
    """
    count = word_count(text)
    minutes = max(1, round(count / words_per_minute))
    
    return minutes