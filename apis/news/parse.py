import hashlib
from datetime import datetime, timezone

def generate_article_id(url):
    """Generate a deterministic integer ID from the article URL."""
    return int(hashlib.md5(url.encode()).hexdigest()[:15], 16)

def parse_news(data, request_id, logger):
    """
    NewsAPI returns a list of articles — we store one row per article.

    Returns:
        tuple: (list of rows, error_message)
    """
    try:
        articles = data['articles']

        if not articles:
            return None, 'No articles returned in response'

        rows = []
        for article in articles:
            rows.append({
                'id':           generate_article_id(article['url']),  # <-- ADDED
                'request_id':   request_id,
                'source_name':  article['source']['name'],
                'author':       article.get('author'),          # nullable
                'title':        article['title'],
                'description':  article.get('description'),     # nullable
                'url':          article['url'],
                'image_url':    article.get('urlToImage'),      # nullable
                'published_at': article['publishedAt'],
                'content':      article.get('content'),         # nullable
            })

        return rows, None

    except KeyError as e:
        return None, f'Missing expected field in API response: {e}'
    except (TypeError, ValueError) as e:
        return None, f'Data type error while parsing response: {e}'