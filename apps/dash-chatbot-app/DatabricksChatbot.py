"""
Databricks Chatbot Component for Dash

This module provides a reusable chatbot component for Dash applications
that connects to Databricks model serving endpoints.

Best Practices Applied:
- Proper logging instead of print statements
- Configurable max_tokens (increased from 128 to 1024)
- Error handling with user-friendly messages
- Auto-scrolling chat history
"""

import logging
import dash
from dash import html, Input, Output, State, dcc
import dash_bootstrap_components as dbc
from model_serving_utils import query_endpoint

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("databricks-chatbot")


class DatabricksChatbot:
    """
    A reusable chatbot component for Dash applications.
    
    This component provides:
    - Chat interface with message history
    - Connection to Databricks model serving endpoints
    - Auto-scrolling to latest messages
    - Clear chat functionality
    - Typing indicators
    
    Args:
        app: The Dash application instance
        endpoint_name: The name of the Databricks model serving endpoint
        height: CSS height for the chat container (default: '600px')
        max_tokens: Maximum tokens for model responses (default: 1024)
    """
    
    # Best Practice: Increased from 128 to 1024 for detailed billing explanations
    DEFAULT_MAX_TOKENS = 1024
    
    def __init__(self, app, endpoint_name, height='600px', max_tokens=None):
        self.app = app
        self.endpoint_name = endpoint_name
        self.height = height
        self.max_tokens = max_tokens or self.DEFAULT_MAX_TOKENS
        self.layout = self._create_layout()
        self._create_callbacks()
        self._add_custom_css()
        
        logger.info(f"Chatbot initialized with endpoint: {endpoint_name}, max_tokens: {self.max_tokens}")

    def _create_layout(self):
        """Create the chat interface layout."""
        return html.Div([
            html.H2('Billing AI Assistant', className='chat-title mb-3'),
            html.Div([
                "Welcome to the Billing Support Assistant. I can help you with:",
                html.Ul([
                    html.Li("Understanding your bill and charges"),
                    html.Li("Payment options and autopay setup"),
                    html.Li("Plan information and comparisons"),
                    html.Li("Billing-related FAQs"),
                ], style={'fontSize': '14px', 'marginTop': '8px'}),
                html.Small([
                    "For detailed billing information, please have your customer ID ready. ",
                    html.A("See documentation", href="https://docs.databricks.com/aws/en/generative-ai/agent-framework/chat-app", target="_blank"),
                    " for more features."
                ], style={'color': '#666'})
            ], className='mb-3', style={'fontSize': '14px', 'color': '#444'}),
            dbc.Card([
                dbc.CardBody([
                    html.Div(id='chat-history', className='chat-history'),
                ], className='d-flex flex-column chat-body')
            ], className='chat-card mb-3'),
            dbc.InputGroup([
                dbc.Input(
                    id='user-input', 
                    placeholder='Type your billing question here...', 
                    type='text',
                    debounce=True
                ),
                dbc.Button('Send', id='send-button', color='success', n_clicks=0, className='ms-2'),
                dbc.Button('Clear', id='clear-button', color='danger', n_clicks=0, className='ms-2'),
            ], className='mb-3'),
            dcc.Store(id='assistant-trigger'),
            dcc.Store(id='chat-history-store'),
            dcc.Store(id='max-tokens-store', data={'max_tokens': self.max_tokens}),
            html.Div(id='dummy-output', style={'display': 'none'}),
        ], className='d-flex flex-column chat-container p-3')

    def _create_callbacks(self):
        """Set up Dash callbacks for chat functionality."""
        
        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Output('user-input', 'value'),
            Output('assistant-trigger', 'data'),
            Input('send-button', 'n_clicks'),
            Input('user-input', 'n_submit'),
            State('user-input', 'value'),
            State('chat-history-store', 'data'),
            prevent_initial_call=True
        )
        def update_chat(send_clicks, user_submit, user_input, chat_history):
            """Handle user message submission."""
            if not user_input or not user_input.strip():
                return dash.no_update, dash.no_update, dash.no_update, dash.no_update

            user_input = user_input.strip()
            logger.info(f"User message received: {user_input[:50]}...")
            
            chat_history = chat_history or []
            chat_history.append({'role': 'user', 'content': user_input})
            chat_display = self._format_chat_display(chat_history)
            chat_display.append(self._create_typing_indicator())

            return chat_history, chat_display, '', {'trigger': True}

        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('assistant-trigger', 'data'),
            State('chat-history-store', 'data'),
            State('max-tokens-store', 'data'),
            prevent_initial_call=True
        )
        def process_assistant_response(trigger, chat_history, token_config):
            """Process the assistant's response from the model endpoint."""
            if not trigger or not trigger.get('trigger'):
                return dash.no_update, dash.no_update

            chat_history = chat_history or []
            if (not chat_history or not isinstance(chat_history[-1], dict)
                    or 'role' not in chat_history[-1]
                    or chat_history[-1]['role'] != 'user'):
                return dash.no_update, dash.no_update

            max_tokens = token_config.get('max_tokens', self.DEFAULT_MAX_TOKENS) if token_config else self.DEFAULT_MAX_TOKENS

            try:
                logger.info(f"Calling model endpoint with max_tokens={max_tokens}")
                assistant_response = self._call_model_endpoint(chat_history, max_tokens)
                chat_history.append({
                    'role': 'assistant',
                    'content': assistant_response
                })
                logger.info("Assistant response received successfully")
            except Exception as e:
                error_message = f"I apologize, but I encountered an error processing your request. Please try again."
                logger.error(f"Error calling model endpoint: {str(e)}")
                chat_history.append({
                    'role': 'assistant',
                    'content': error_message
                })

            chat_display = self._format_chat_display(chat_history)
            return chat_history, chat_display

        @self.app.callback(
            Output('chat-history-store', 'data', allow_duplicate=True),
            Output('chat-history', 'children', allow_duplicate=True),
            Input('clear-button', 'n_clicks'),
            prevent_initial_call=True
        )
        def clear_chat(n_clicks):
            """Clear the chat history."""
            if n_clicks:
                logger.info("Chat history cleared")
                return [], []
            return dash.no_update, dash.no_update

    def _call_model_endpoint(self, messages, max_tokens=None):
        """
        Call the model serving endpoint.
        
        Args:
            messages: List of chat messages
            max_tokens: Maximum tokens for the response
            
        Returns:
            The assistant's response content
        """
        if max_tokens is None:
            max_tokens = self.max_tokens
            
        try:
            logger.debug(f"Sending request to endpoint: {self.endpoint_name}")
            response = query_endpoint(self.endpoint_name, messages, max_tokens)
            return response["content"]
        except Exception as e:
            logger.error(f"Model endpoint error: {str(e)}")
            raise

    def _format_chat_display(self, chat_history):
        """Format chat history for display."""
        return [
            html.Div([
                html.Div(msg['content'],
                         className=f"chat-message {msg['role']}-message")
            ], className=f"message-container {msg['role']}-container")
            for msg in chat_history if isinstance(msg, dict) and 'role' in msg
        ]

    def _create_typing_indicator(self):
        """Create a typing indicator element."""
        return html.Div([
            html.Div(className='chat-message assistant-message typing-message',
                     children=[
                         html.Div(className='typing-dot'),
                         html.Div(className='typing-dot'),
                         html.Div(className='typing-dot')
                     ])
        ], className='message-container assistant-container')

    def _add_custom_css(self):
        """Add custom CSS styles for the chat interface."""
        custom_css = '''
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        body {
            font-family: 'DM Sans', sans-serif;
            background-color: #F9F7F4; /* Oat Light */
        }
        .chat-container {
            max-width: 800px;
            margin: 0 auto;
            background-color: #FFFFFF;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            height: 100vh;
            display: flex;
            flex-direction: column;
        }
        .chat-title {
            font-size: 24px;
            font-weight: 700;
            color: #1B3139; /* Navy 800 */
            text-align: center;
        }
        .chat-card {
            border: none;
            background-color: #EEEDE9; /* Oat Medium */
            flex-grow: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }
        .chat-body {
            flex-grow: 1;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }
        .chat-history {
            flex-grow: 1;
            overflow-y: auto;
            padding: 15px;
        }
        .message-container {
            display: flex;
            margin-bottom: 15px;
        }
        .user-container {
            justify-content: flex-end;
        }
        .chat-message {
            max-width: 80%;
            padding: 10px 15px;
            border-radius: 20px;
            font-size: 16px;
            line-height: 1.4;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .user-message {
            background-color: #FF3621; /* Databricks Orange 600 */
            color: white;
        }
        .assistant-message {
            background-color: #1B3139; /* Databricks Navy 800 */
            color: white;
        }
        .typing-message {
            background-color: #2D4550; /* Lighter shade of Navy 800 */
            color: #EEEDE9; /* Oat Medium */
            display: flex;
            justify-content: center;
            align-items: center;
            min-width: 60px;
        }
        .typing-dot {
            width: 8px;
            height: 8px;
            background-color: #EEEDE9; /* Oat Medium */
            border-radius: 50%;
            margin: 0 3px;
            animation: typing-animation 1.4s infinite ease-in-out;
        }
        .typing-dot:nth-child(1) { animation-delay: 0s; }
        .typing-dot:nth-child(2) { animation-delay: 0.2s; }
        .typing-dot:nth-child(3) { animation-delay: 0.4s; }
        @keyframes typing-animation {
            0% { transform: translateY(0px); }
            50% { transform: translateY(-5px); }
            100% { transform: translateY(0px); }
        }
        #user-input {
            border-radius: 20px;
            border: 1px solid #DCE0E2; /* Databricks Gray - Lines */
        }
        #send-button, #clear-button {
            border-radius: 20px;
            width: 100px;
        }
        #send-button {
            background-color: #00A972; /* Databricks Green 600 */
            border-color: #00A972;
        }
        #clear-button {
            background-color: #98102A; /* Databricks Maroon 600 */
            border-color: #98102A;
        }
        .input-group {
            flex-wrap: nowrap;
        }
        '''
        self.app.index_string = self.app.index_string.replace(
            '</head>',
            f'<style>{custom_css}</style></head>'
        )

        # Add auto-scroll behavior
        self.app.clientside_callback(
            """
            function(children) {
                var chatHistory = document.getElementById('chat-history');
                if(chatHistory) {
                    chatHistory.scrollTop = chatHistory.scrollHeight;
                }
                return '';
            }
            """,
            Output('dummy-output', 'children'),
            Input('chat-history', 'children'),
            prevent_initial_call=True
        )
