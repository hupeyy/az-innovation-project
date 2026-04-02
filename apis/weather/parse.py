from datetime import datetime, timezone
import apis.weather.config as config
import time
import random

def validate_weather(data):
    """Checks for logical consistency and existence of required fields."""
    try:
        if 'main' not in data or 'coord' not in data or 'sys' not in data:
            return "Missing mandatory top-level sections (main/coord/sys)"
            
        required_fields = {
            'temp_min': data['main'], 
            'temp_max': data['main'], 
            'humidity': data['main'],
            'lat': data['coord'],
            'lon': data['coord']
        }
        
        for field, parent in required_fields.items():
            if parent.get(field) is None:
                return f"Validation error: Required field '{field}' is missing or None"
            if not isinstance(parent.get(field), (int, float)):
                return f"Validation error: Field '{field}' is not a valid number"
                
        return None
    except Exception as e:
        return f"Validation logic crashed: {str(e)}"

def parse_weather(data, request_id, logger):
    """
    Parses and validates the weather API response.
    Returns: (row, [entities], error_message)
    """
    # 1. Validation
    validation_error = validate_weather(data)
    if validation_error:
        return None, None, validation_error

    # 2. Transformation
    try:
        weather_row = {
            'id':         data['id'],
            'request_id': request_id,
            'city_name':  data['name'],
            'country':    data['sys']['country'],
            'units':      config.UNITS,
            'latitude':   float(data['coord']['lat']),
            'longitude':  float(data['coord']['lon']),
            'temp_min':   float(data['main']['temp_min']),
            'temp_max':   float(data['main']['temp_max']),
            'humidity':   int(data['main']['humidity']),
            'wind_speed': float(data['wind'].get('speed', 0.0)),
            'sunrise':    datetime.fromtimestamp(data['sys']['sunrise'], tz=timezone.utc).isoformat(),
            'sunset':     datetime.fromtimestamp(data['sys']['sunset'], tz=timezone.utc).isoformat(),
        }

        # 3. Entity Extraction (Adding the required 'id' to avoid BigQuery schema errors)
        entities = [{
            'id':           int(time.time() * 1000000) + random.randint(1, 999),
            'request_id':   request_id,
            'entity_type':  'country',
            'entity_value': str(data['sys']['country'])
        }]
        
        return weather_row, entities, None

    except KeyError as e:
        return None, None, f'Missing expected field during transformation: {e}'
    except (TypeError, ValueError) as e:
        return None, None, f'Data type error while parsing response: {e}'