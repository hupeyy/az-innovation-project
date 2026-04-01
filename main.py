from pipeline.runner import run_pipeline
import apis.weather.pipeline as weather_api
import apis.news.pipeline    as news_api

if __name__ == '__main__':
    run_pipeline(weather_api)
    run_pipeline(news_api)