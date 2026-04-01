from pipeline.runner import run_pipeline
import apis.weather.pipeline as weather_api

if __name__ == '__main__':
    run_pipeline(weather_api)
