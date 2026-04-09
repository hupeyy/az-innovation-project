from interface.openclaw.client import query_openclaw
from interface.openclaw.tools import AVAILABLE_TOOLS, execute_tool
from interface.slack.client import post_message
import json

async def handle_slack_message(message: str, channel: str, user: str):
    """
    Main entry point for processing Slack messages with OpenClaw.
    """
    
    # Build conversation context
    system_prompt = """
    You are an assistant for the AZ Data Pipeline system.
    
    You can help users:
    - Check weather, news, and stock data
    - Run data pipelines
    - Query historical data
    - Monitor brand mentions
    - Generate reports
    
    When users ask questions, use the available tools to fetch data,
    then provide clear, friendly responses.
    """
    
    # Call OpenClaw with tool definitions
    response = query_openclaw(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ],
        tools=AVAILABLE_TOOLS
    )
    
    # Check if OpenClaw wants to use a tool
    if response.get("tool_calls"):
        results = []
        
        for tool_call in response["tool_calls"]:
            tool_name = tool_call["function"]["name"]
            tool_args = json.loads(tool_call["function"]["arguments"])
            
            # Execute the tool
            result = await execute_tool(tool_name, tool_args)
            results.append(result)
        
        # Send tool results back to OpenClaw for final response
        final_response = query_openclaw(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": message},
                {"role": "assistant", "content": "", "tool_calls": response["tool_calls"]},
                {"role": "tool", "content": json.dumps(results)}
            ]
        )
        
        answer = final_response["content"]
    else:
        answer = response["content"]
    
    # Send response to Slack
    post_message(answer, channel=channel)