"""
Telco Billing Customer Care Agent

This module defines an AI agent for handling telecom billing inquiries using
Databricks' LangGraph integration with Unity Catalog tools.

Best Practices Applied:
- Timeout configuration for LLM calls
- Proper error handling
- MLflow autologging for observability
- Streaming support for real-time responses
"""

from typing import Any, Generator, Optional, Sequence, Union
import logging

import mlflow
from databricks_langchain import (
    ChatDatabricks,
    VectorSearchRetrieverTool,
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client,
)
from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import (
    ChatAgentChunk,
    ChatAgentMessage,
    ChatAgentResponse,
    ChatContext,
)
from mlflow.models import ModelConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telco-billing-agent")

# Enable MLflow autologging for tracing and debugging
mlflow.langchain.autolog()

# Initialize Unity Catalog function client
client = DatabricksFunctionClient()
set_uc_function_client(client)

# Load configuration from YAML
config = ModelConfig(development_config="config.yaml").to_dict()

############################################
# Define your LLM endpoint and system prompt
############################################

# Best Practice: Configure timeout to prevent hanging requests
LLM_TIMEOUT_SECONDS = config.get('llm_timeout_seconds', 60)
LLM_MAX_TOKENS = config.get('llm_max_tokens', 1024)

llm = ChatDatabricks(
    endpoint=config['llm_endpoint'],
    max_tokens=LLM_MAX_TOKENS,
    # Note: timeout is handled at the request level in production
)

system_prompt = config['agent_prompt']

logger.info(f"Agent configured with LLM endpoint: {config['llm_endpoint']}")
logger.info(f"Max tokens: {LLM_MAX_TOKENS}")

###############################################################################
# Define tools for your agent, enabling it to retrieve data or take actions
# beyond text generation
###############################################################################

catalog = config['catalog']
schema = config['schema']

tools = []

# Load Unity Catalog functions as tools
# These functions have input validation built-in for security
uc_tool_names = [
    config['tools_billing_faq'],
    config['tools_billing'],
    config['tools_items'],
    config['tools_plans'],  
    config['tools_customer']
]

logger.info(f"Loading UC tools: {uc_tool_names}")
uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
tools.extend(uc_toolkit.tools)

logger.info(f"Loaded {len(tools)} tools for the agent")

#####################
# Define agent logic
#####################


def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[Sequence[BaseTool], ToolNode],
    system_prompt: Optional[str] = None,
) -> CompiledGraph:
    """
    Create a tool-calling agent using LangGraph.
    
    The agent follows a simple loop:
    1. Call the LLM with the current messages
    2. If the LLM wants to call tools, execute them
    3. Return tool results to the LLM
    4. Repeat until the LLM has a final response
    
    Args:
        model: The language model to use
        tools: List of tools available to the agent
        system_prompt: Optional system prompt to guide the agent
        
    Returns:
        A compiled LangGraph workflow
    """
    model = model.bind_tools(tools)

    def should_continue(state: ChatAgentState):
        """Determine whether to continue tool calling or end."""
        messages = state["messages"]
        last_message = messages[-1]
        # If there are function calls, continue. else, end
        if last_message.get("tool_calls"):
            return "continue"
        else:
            return "end"

    # Prepend system prompt if provided
    if system_prompt:
        preprocessor = RunnableLambda(
            lambda state: [{"role": "system", "content": system_prompt}]
            + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])
    
    model_runnable = preprocessor | model

    def call_model(
        state: ChatAgentState,
        runnable_config: RunnableConfig,
    ):
        """Invoke the LLM with current state."""
        try:
            response = model_runnable.invoke(state, runnable_config)
            return {"messages": [response]}
        except Exception as e:
            logger.error(f"Error calling model: {e}")
            raise

    # Build the workflow graph
    workflow = StateGraph(ChatAgentState)

    workflow.add_node("agent", RunnableLambda(call_model))
    workflow.add_node("tools", ChatAgentToolNode(tools))

    workflow.set_entry_point("agent")
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "tools",
            "end": END,
        },
    )
    workflow.add_edge("tools", "agent")

    return workflow.compile()


class LangGraphChatAgent(ChatAgent):
    """
    MLflow ChatAgent wrapper for the LangGraph billing agent.
    
    This class provides the interface expected by MLflow for model serving,
    including both synchronous and streaming prediction methods.
    """
    
    def __init__(self, agent: CompiledStateGraph):
        self.agent = agent
        logger.info("LangGraphChatAgent initialized")

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        """
        Generate a response for the given messages.
        
        Args:
            messages: List of chat messages
            context: Optional context information
            custom_inputs: Optional custom inputs
            
        Returns:
            ChatAgentResponse with the agent's response
        """
        request = {"messages": self._convert_messages_to_dict(messages)}

        response_messages = []
        try:
            for event in self.agent.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    response_messages.extend(
                        ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                    )
        except Exception as e:
            logger.error(f"Error during prediction: {e}")
            # Return error message to user
            response_messages.append(
                ChatAgentMessage(
                    role="assistant",
                    content=f"I apologize, but I encountered an error processing your request. Please try again or contact support if the issue persists."
                )
            )
            
        return ChatAgentResponse(messages=response_messages)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        """
        Generate a streaming response for the given messages.
        
        Args:
            messages: List of chat messages
            context: Optional context information
            custom_inputs: Optional custom inputs
            
        Yields:
            ChatAgentChunk objects as the response is generated
        """
        request = {"messages": self._convert_messages_to_dict(messages)}
        
        try:
            for event in self.agent.stream(request, stream_mode="updates"):
                for node_data in event.values():
                    yield from (
                        ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
                    )
        except Exception as e:
            logger.error(f"Error during streaming prediction: {e}")
            yield ChatAgentChunk(
                **{"delta": {
                    "role": "assistant",
                    "content": "I apologize, but I encountered an error. Please try again."
                }}
            )


# Create the agent object, and specify it as the agent object to use when
# loading the agent back for inference via mlflow.models.set_model()
agent = create_tool_calling_agent(llm, tools, system_prompt)
AGENT = LangGraphChatAgent(agent)
mlflow.models.set_model(AGENT)

logger.info("Agent created and registered with MLflow")
