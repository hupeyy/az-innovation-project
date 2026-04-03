import hashlib
from datetime import datetime
import time
import random

def validate_article(article):
    """Checks if an article has the bare minimum data to be valid."""
    required_fields = ['url', 'title', 'publishedAt']
    
    for field in required_fields:
        if not article.get(field):
            return f"Validation error: Missing required field '{field}'"
            
    if 'source' not in article or not article['source'].get('name'):
        return "Validation error: Article source or source name missing"
        
    return None

def generate_article_id(url):
    """Generate a deterministic integer ID from the article URL."""
    return int(hashlib.md5(url.encode()).hexdigest()[:15], 16)

def parse_news(data, request_id, logger):
    """
    Parses and validates the NewsAPI response.
    Always returns a 3-tuple: (rows, entities, error)
    """
    # 1. Guard against None or non-dict input
    if not isinstance(data, dict):
        return None, None, f'Invalid response: expected dict, got {type(data).__name__}'

    articles = data.get('articles')
    if not articles:
        return None, None, 'No articles key found in response'

    valid_rows = []
    extracted_entities = []
    
    for article in articles:
        # 2. Row-level validation
        err = validate_article(article)
        if err:
            logger.warning(f"Skipping invalid article: {err}")
            continue
            
        # 3. Transformation
        try:
            row = {
                'id':           generate_article_id(article['url']),
                'request_id':   request_id,
                'source_name':  article['source']['name'],
                'author':       article.get('author'),
                'title':        article['title'],
                'description':  article.get('description'),
                'url':          article['url'],
                'image_url':    article.get('urlToImage'),
                'published_at': article['publishedAt'],
                'content':      article.get('content'),
            }
            valid_rows.append(row)

            source = article.get('source', {}).get('name')
            extracted_entities.append({
                'id':           int(time.time() * 1000000) + random.randint(1000, 9999),
                'request_id':   request_id,
                'entity_type':  'source',
                'entity_value': str(source)
            })

        except Exception as e:
            logger.error(f"Failed to transform article: {e}")
            continue

    if not valid_rows:
        return None, None, 'No valid articles were found after validation'

    return valid_rows, extracted_entities, None