"""
Model Serving Utilities

This module provides utilities for interacting with Databricks model serving endpoints.

Best Practices Applied:
- Proper logging for debugging and monitoring
- Clear error messages with helpful guidance
- Type hints for better code documentation
"""

import logging
from typing import Any
from mlflow.deployments import get_deploy_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("model-serving-utils")


def _query_endpoint(
    endpoint_name: str, 
    messages: list[dict[str, str]], 
    max_tokens: int
) -> list[dict[str, Any]]:
    """
    Call a Databricks model serving endpoint.
    
    This function handles both:
    1. Databricks foundation/external model endpoints (chat task type)
    2. Databricks agent serving endpoints (conversational agent schema)
    
    Args:
        endpoint_name: Name of the model serving endpoint
        messages: List of message dictionaries with 'role' and 'content' keys
        max_tokens: Maximum number of tokens for the response
        
    Returns:
        List of response message dictionaries
        
    Raises:
        Exception: If the endpoint response format is not recognized
    """
    logger.debug(f"Querying endpoint: {endpoint_name}")
    logger.debug(f"Number of messages: {len(messages)}")
    logger.debug(f"Max tokens: {max_tokens}")
    
    try:
        client = get_deploy_client('databricks')
        
        res = client.predict(
            endpoint=endpoint_name,
            inputs={
                'messages': messages, 
                'max_tokens': max_tokens
            },
        )
        
        logger.debug(f"Response keys: {res.keys() if isinstance(res, dict) else 'not a dict'}")
        
        # Handle agent serving endpoint response format
        if "messages" in res:
            logger.debug("Detected agent serving endpoint response format")
            return res["messages"]
        
        # Handle foundation/external model endpoint response format  
        elif "choices" in res:
            logger.debug("Detected foundation model endpoint response format")
            return [res["choices"][0]["message"]]
        
        else:
            logger.error(f"Unexpected response format: {res.keys() if isinstance(res, dict) else type(res)}")
            raise Exception(
                "Unexpected response format from endpoint. "
                "This app supports:\n"
                "1) Databricks foundation model or external model endpoints with the chat task type "
                "(see https://docs.databricks.com/aws/en/machine-learning/model-serving/score-foundation-models#chat-completion-model-query)\n"
                "2) Databricks agent serving endpoints that implement the conversational agent schema "
                "(see https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent)"
            )
            
    except Exception as e:
        logger.error(f"Error querying endpoint {endpoint_name}: {str(e)}")
        raise


def query_endpoint(
    endpoint_name: str, 
    messages: list[dict[str, str]], 
    max_tokens: int
) -> dict[str, Any]:
    """
    Query a model serving endpoint and return the last message.
    
    This is a convenience wrapper around _query_endpoint that returns
    just the final response message.
    
    Args:
        endpoint_name: Name of the model serving endpoint
        messages: List of message dictionaries with 'role' and 'content' keys
        max_tokens: Maximum number of tokens for the response
        
    Returns:
        The last message dictionary from the response
        
    Example:
        >>> response = query_endpoint("my-agent", [{"role": "user", "content": "Hello"}], 1024)
        >>> print(response["content"])
        "Hello! How can I help you today?"
    """
    all_messages = _query_endpoint(endpoint_name, messages, max_tokens)
    
    if not all_messages:
        logger.warning("Received empty response from endpoint")
        return {"role": "assistant", "content": "I received an empty response. Please try again."}
    
    last_message = all_messages[-1]
    logger.debug(f"Returning last message with role: {last_message.get('role', 'unknown')}")
    
    return last_message
