from datetime import datetime, timezone
import apis.weather.config as config

def parse_weather(data, request_id, logger):
    try:
        weather_row = {
            'id':         data['id'],
            'request_id': request_id,
            'city_name':  data['name'],
            'country':    data['sys']['country'],
            'units':      config.UNITS,
            'latitude':   data['coord']['lat'],
            'longitude':  data['coord']['lon'],
            'temp_min':   data['main']['temp_min'],
            'temp_max':   data['main']['temp_max'],
            'humidity':   data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'sunrise':    datetime.fromtimestamp(
                              data['sys']['sunrise'], tz=timezone.utc
                          ).isoformat(),
            'sunset':     datetime.fromtimestamp(
                              data['sys']['sunset'], tz=timezone.utc
                          ).isoformat(),
        }
        return weather_row, None
    except KeyError as e:
        return None, f'Missing expected field in API response: {e}'
    except (TypeError, ValueError) as e:
        return None, f'Data type error while parsing response: {e}'