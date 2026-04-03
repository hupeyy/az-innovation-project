# parse.py
import hashlib
import time

def validate_quote(quote):
    """Checks if the quote contains all necessary data fields."""
    required_fields = ['01. symbol', '05. price', '06. volume', '07. latest trading day']
    
    for field in required_fields:
        if not quote.get(field):
            return f"Validation error: Missing required field '{field}'"
    return None

def parse_stock(data, request_id, logger):
    # 1. Guard against None or non-dict input
    if not isinstance(data, dict):
        return None, None, f'Invalid response: expected dict, got {type(data).__name__}'

    # 2. Check for API-level errors (Alpha Vantage returns 200 OK for errors)
    if "Note" in data:
        return None, None, f"API Limit reached: {data['Note']}"
    if "Error Message" in data:
        return None, None, f"API Error: {data['Error Message']}"

    quote = data.get('Global Quote', {})
    if not quote:
        return None, None, 'No "Global Quote" found in response'

    # 3. Row-level validation
    err = validate_quote(quote)
    if err:
        return None, None, err

    try:
        # 4. Transformation
        row = {
            'id':         int(hashlib.md5(f"{quote['01. symbol']}{quote['07. latest trading day']}".encode()).hexdigest()[:15], 16),
            'request_id': request_id,
            'symbol':     quote['01. symbol'],
            'price':      float(quote['05. price']),
            'volume':     int(quote['06. volume']),
            'latest_day': quote['07. latest trading day'],
        }

        # 5. Extract Entity
        extracted_entities = [{
            'id':           int(time.time() * 1000000) + int(hashlib.md5(quote['01. symbol'].encode()).hexdigest()[:5], 16),
            'request_id':   request_id,
            'entity_type':  'stock_symbol',
            'entity_value': quote['01. symbol']
        }]

        return [row], extracted_entities, None
    except ValueError as ve:
        return None, None, f"Data type conversion error: {ve}"
    except Exception as e:
        return None, None, f"Failed to parse Alpha Vantage data: {e}"