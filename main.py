from dotenv import load_dotenv
import os
import requests

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')

response = requests.get(
    'https://api.openweathermap.org/data/2.5/weather',
    params={
        'lat': 44.34,
        'lon': 10.99,
        'appid': OPENWEATHER_API_KEY,
        'units': 'imperial'
    }
)

if response.status_code == 200:
    data = response.json()
    
    print(f"City: {data['name']}, {data['sys']['country']}")
    print(f"Temp: {data['main']['temp']}")
    print(f"Feels like: {data['main']['feels_like']}")
    print(f"Humidity: {data['main']['humidity']}%")
    print(f"Weather: {data['weather'][0]['description']}")
    print(f"Wind speed: {data['wind']['speed']}")
else:
    print(f'Error: {response.status_code}')