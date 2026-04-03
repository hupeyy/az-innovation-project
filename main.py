from config import validate_env

from apis.weather.pipeline import get_api_meta as weather_meta
from apis.news.pipeline    import get_api_meta as news_meta
import apis.weather.pipeline as weather_api
import apis.news.pipeline    as news_api
import apis.alpha_vantage.pipeline as alpha_vantage_api
from pipeline.runner import run_pipeline

def main():
    validate_env()
    run_pipeline(weather_api)
    run_pipeline(news_api)
    run_pipeline(alpha_vantage_api)


if __name__ == '__main__':
    main()