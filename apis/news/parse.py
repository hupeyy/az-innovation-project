from datetime import datetime, timezone

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
                'request_id':   request_id,
                'source_name':  article['source']['name'],
                'author':       article.get('author', 'Unknown'),
                'title':        article['title'],
                'url':          article['url'],
                'published_at': article['publishedAt'],
                'content':      article.get('content', ''),
            })

        return rows, None

    except KeyError as e:
        return None, f'Missing expected field in API response: {e}'
    except (TypeError, ValueError) as e:
        return None, f'Data type error while parsing response: {e}'