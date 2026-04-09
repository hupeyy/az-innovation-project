from bq.client import get_bq_client
from config import BIGQUERY_PROJECT_ID, DATASET_ID
from pipeline.runner import run_pipeline
import apis.weather.pipeline as weather_api
import apis.news.pipeline as news_api

AVAILABLE_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_latest_weather",
            "description": "Get the most recent weather data",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_recent_news",
            "description": "Get recent news headlines",
            "parameters": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of headlines to return",
                        "default": 5
                    }
                },
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_data_pipeline",
            "description": "Trigger a data pipeline to fetch fresh data",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "enum": ["weather", "news", "stocks"],
                        "description": "Which pipeline to run"
                    }
                },
                "required": ["pipeline_name"]
            }
        }
    }
]


async def execute_tool(tool_name: str, args: dict):
    """Execute the requested tool and return results."""
    
    if tool_name == "query_latest_weather":
        return query_latest_weather()
    
    elif tool_name == "query_recent_news":
        return query_recent_news(args.get("limit", 5))
    
    elif tool_name == "run_data_pipeline":
        return run_data_pipeline(args["pipeline_name"])
    
    else:
        return {"error": f"Unknown tool: {tool_name}"}


def query_latest_weather():
    client = get_bq_client()
    query = f"""
        SELECT temperature, description, humidity
        FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.weather_data`
        ORDER BY request_id DESC
        LIMIT 1
    """
    result = list(client.query(query).result())
    
    if result:
        row = result[0]
        return {
            "temperature": row.temperature,
            "description": row.description,
            "humidity": row.humidity
        }
    return {"error": "No weather data found"}


def query_recent_news(limit=5):
    client = get_bq_client()
    query = f"""
        SELECT title, source_name, url, published_at
        FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.news_data`
        ORDER BY published_at DESC
        LIMIT {limit}
    """
    results = list(client.query(query).result())
    
    return [
        {
            "title": row.title,
            "source": row.source_name,
            "url": row.url,
            "published_at": row.published_at
        }
        for row in results
    ]


def run_data_pipeline(pipeline_name: str):
    pipeline_map = {
        "weather": weather_api,
        "news": news_api,
        # add others
    }
    
    if pipeline_name not in pipeline_map:
        return {"error": f"Unknown pipeline: {pipeline_name}"}
    
    try:
        run_pipeline(pipeline_map[pipeline_name])
        return {"status": "success", "message": f"{pipeline_name} pipeline completed"}
    except Exception as e:
        return {"status": "error", "message": str(e)}