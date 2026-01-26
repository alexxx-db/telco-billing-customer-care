"""
Telco Billing Customer Care - Dash Chatbot Application

This application provides a web interface for interacting with the
Telco Billing AI Assistant agent deployed on Databricks.

Configuration:
- SERVING_ENDPOINT: Set in app.yaml, the name of the model serving endpoint
- MAX_TOKENS: Optional, defaults to 1024 for detailed billing explanations

Best Practices Applied:
- Proper logging for debugging and monitoring
- Environment variable configuration
- Clean error messages for missing configuration
"""

import os
import logging
import dash
import dash_bootstrap_components as dbc
from DatabricksChatbot import DatabricksChatbot

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telco-billing-app")

# Load configuration from environment variables
serving_endpoint = os.getenv('SERVING_ENDPOINT')
max_tokens = int(os.getenv('MAX_TOKENS', '1024'))  # Default: 1024 for detailed responses

# Validate required configuration
if not serving_endpoint:
    logger.error("SERVING_ENDPOINT environment variable is not set")
    raise ValueError(
        "SERVING_ENDPOINT must be set in app.yaml or as an environment variable. "
        "This should be the name of your deployed model serving endpoint."
    )

logger.info(f"Starting Telco Billing Chatbot App")
logger.info(f"Serving Endpoint: {serving_endpoint}")
logger.info(f"Max Tokens: {max_tokens}")

# Initialize the Dash app with a clean theme
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.FLATLY],
    title="Billing AI Assistant"
)

# Create the chatbot component with configuration
chatbot = DatabricksChatbot(
    app=app, 
    endpoint_name=serving_endpoint, 
    height='600px',
    max_tokens=max_tokens
)

# Define the app layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(chatbot.layout, width={'size': 8, 'offset': 2})
    ])
], fluid=True)

# Server reference for WSGI deployment
server = app.server

if __name__ == '__main__':
    logger.info("Starting development server...")
    app.run(debug=True, host='0.0.0.0', port=8050)
