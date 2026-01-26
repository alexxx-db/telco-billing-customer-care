# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Telco Billing Agent Deployment & Evaluation
# MAGIC
# MAGIC This notebook orchestrates the **deployment pipeline** for an AI agent designed to answer customer billing queries for a telecommunications provider.
# MAGIC
# MAGIC It extends the auto-generated code from the Databricks AI Playground with an additional **synthetic evaluation component**, allowing for a more rigorous and repeatable assessment of the agent's quality using a curated FAQ knowledge base.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📋 Notebook Overview
# MAGIC
# MAGIC This notebook performs the following steps:
# MAGIC
# MAGIC 1. **Define the agent**
# MAGIC 2. **Log the agent**: Wraps the agent (from the `agent` notebook) as an MLflow model along with configuration and resource bindings.
# MAGIC 3. **Synthetic Evaluation**: Generates a set of realistic and adversarial queries from the FAQ dataset to evaluate agent performance using the Agent Evaluation framework.
# MAGIC 4. **Register to Unity Catalog**: Stores the model centrally for governance and discovery.
# MAGIC 5. **Set Model Aliases**: Apply lifecycle aliases (champion, challenger) for proper model management.
# MAGIC 6. **Deploy to Model Serving**: Deploys the agent to a Databricks model serving endpoint for interactive and production use.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Best Practices Applied
# MAGIC - Model aliases for lifecycle management (champion/challenger/archived)
# MAGIC - Proper logging instead of print statements
# MAGIC - Comprehensive evaluation before deployment
# MAGIC - Resource specifications for auth passthrough

# COMMAND ----------

# DBTITLE 1,Install Libraries
# MAGIC %pip install -U -qqqq mlflow-skinny[databricks] langgraph==0.3.4 databricks-langchain databricks-agents==1.0.1 uv
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Initialize Logging and Configuration
import logging
import yaml
from yaml.representer import SafeRepresenter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telco-billing-deployment")

class LiteralString(str):
    pass

def literal_str_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='|')

yaml.add_representer(LiteralString, literal_str_representer)

logger.info("Starting agent deployment pipeline")

# COMMAND ----------

# DBTITLE 1,Define System Prompt
# Enhanced system prompt with security guidelines
system_prompt = """You are a Billing Support Agent assisting users with billing inquiries for a telecommunications provider.

## Guidelines:
- First, check FAQ Search before requesting any customer details.
- If an FAQ answer exists, return it immediately.
- If no FAQ match, request the customer_id before retrieving billing details.
- NEVER disclose confidential information like names, emails, device_id, or phone numbers.
- Always be polite, professional, and concise.

## Security Rules:
- Only provide billing summaries and plan information.
- If asked for sensitive data (SSN, full credit card, etc.), politely decline.
- If a customer_id appears invalid, ask for verification.

## Process:
1. Run FAQ Search -> If an answer exists, return it.
2. If no FAQ match, ask for the customer_id.
3. Use the relevant tool(s) to fetch billing details.
4. If missing details (e.g., timeframe), ask clarifying questions.
5. Summarize the billing information clearly.

## Available Tools:
- billing_faq: Search FAQs for common billing questions
- lookup_customer: Get customer details (use only to verify customer exists)
- lookup_billing: Get monthly billing summary for a customer
- lookup_billing_items: Get detailed billing events (data, calls, texts)
- lookup_billing_plans: List all available billing plans

Keep responses polite, professional, and concise.
"""

# COMMAND ----------

# DBTITLE 1,Load Configuration Variables
catalog = config['catalog']
schema = config['database']
llm_endpoint = config['llm_endpoint']
embedding_model_endpoint_name = config['embedding_model_endpoint_name']
warehouse_id = config['warehouse_id']
vector_search_index = f"{config['catalog']}.{config['database']}.{config['vector_search_index']}"
tools_billing_faq = config['tools_billing_faq'] 
tools_billing = config['tools_billing']
tools_items = config['tools_items']
tools_plans = config['tools_plans']
tools_customer = config['tools_customer']
agent_name = config['agent_name']

# Model lifecycle aliases from config
model_alias_champion = config.get('model_alias_champion', 'champion')
model_alias_challenger = config.get('model_alias_challenger', 'challenger')
model_alias_archived = config.get('model_alias_archived', 'archived')

agent_prompt = LiteralString(system_prompt)

logger.info(f"Deploying agent: {agent_name}")
logger.info(f"Catalog: {catalog}, Schema: {schema}")

# COMMAND ----------

# DBTITLE 1,Generate config.yaml for Agent
yaml_data = {
    "catalog": catalog,
    "schema": schema,
    "llm_endpoint": llm_endpoint,
    "llm_timeout_seconds": config.get('llm_timeout_seconds', 60),
    "llm_max_tokens": config.get('llm_max_tokens', 1024),
    "embedding_model_endpoint_name": embedding_model_endpoint_name,
    "warehouse_id": warehouse_id,
    "vector_search_index": vector_search_index,
    "tools_billing_faq": tools_billing_faq,
    "tools_billing": tools_billing,
    "tools_items": tools_items,
    "tools_plans": tools_plans,
    "tools_customer": tools_customer,
    "agent_name": agent_name,
    "agent_prompt": agent_prompt
}

with open("config.yaml", "w") as f:
    yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

logger.info("Generated config.yaml")

# COMMAND ----------

# MAGIC %md ## Define the agent in code
# MAGIC Below we define our agent code in a single cell, enabling us to easily write it to a local Python file for subsequent logging and deployment using the `%%writefile` magic command.
# MAGIC
# MAGIC This version includes:
# MAGIC - Error handling for robustness
# MAGIC - Proper logging for debugging
# MAGIC - Timeout configuration support
# MAGIC
# MAGIC For more examples of tools to add to your agent, see [docs](https://learn.microsoft.com/azure/databricks/generative-ai/agent-framework/agent-tool).

# COMMAND ----------

# MAGIC %%writefile agent.py
# MAGIC """
# MAGIC Telco Billing Customer Care Agent
# MAGIC 
# MAGIC This module defines an AI agent for handling telecom billing inquiries using
# MAGIC Databricks' LangGraph integration with Unity Catalog tools.
# MAGIC 
# MAGIC Best Practices Applied:
# MAGIC - Timeout configuration for LLM calls
# MAGIC - Proper error handling
# MAGIC - MLflow autologging for observability
# MAGIC - Streaming support for real-time responses
# MAGIC """
# MAGIC 
# MAGIC from typing import Any, Generator, Optional, Sequence, Union
# MAGIC import logging
# MAGIC 
# MAGIC import mlflow
# MAGIC from databricks_langchain import (
# MAGIC     ChatDatabricks,
# MAGIC     VectorSearchRetrieverTool,
# MAGIC     DatabricksFunctionClient,
# MAGIC     UCFunctionToolkit,
# MAGIC     set_uc_function_client,
# MAGIC )
# MAGIC from langchain_core.language_models import LanguageModelLike
# MAGIC from langchain_core.runnables import RunnableConfig, RunnableLambda
# MAGIC from langchain_core.tools import BaseTool
# MAGIC from langgraph.graph import END, StateGraph
# MAGIC from langgraph.graph.graph import CompiledGraph
# MAGIC from langgraph.graph.state import CompiledStateGraph
# MAGIC from langgraph.prebuilt.tool_node import ToolNode
# MAGIC from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode
# MAGIC from mlflow.pyfunc import ChatAgent
# MAGIC from mlflow.types.agent import (
# MAGIC     ChatAgentChunk,
# MAGIC     ChatAgentMessage,
# MAGIC     ChatAgentResponse,
# MAGIC     ChatContext,
# MAGIC )
# MAGIC from mlflow.models import ModelConfig
# MAGIC 
# MAGIC # Configure logging
# MAGIC logging.basicConfig(level=logging.INFO)
# MAGIC logger = logging.getLogger("telco-billing-agent")
# MAGIC 
# MAGIC # Enable MLflow autologging for tracing and debugging
# MAGIC mlflow.langchain.autolog()
# MAGIC 
# MAGIC # Initialize Unity Catalog function client
# MAGIC client = DatabricksFunctionClient()
# MAGIC set_uc_function_client(client)
# MAGIC 
# MAGIC # Load configuration from YAML
# MAGIC config = ModelConfig(development_config="config.yaml").to_dict()
# MAGIC 
# MAGIC ############################################
# MAGIC # Define your LLM endpoint and system prompt
# MAGIC ############################################
# MAGIC 
# MAGIC # Best Practice: Configure timeout to prevent hanging requests
# MAGIC LLM_TIMEOUT_SECONDS = config.get('llm_timeout_seconds', 60)
# MAGIC LLM_MAX_TOKENS = config.get('llm_max_tokens', 1024)
# MAGIC 
# MAGIC llm = ChatDatabricks(
# MAGIC     endpoint=config['llm_endpoint'],
# MAGIC     max_tokens=LLM_MAX_TOKENS,
# MAGIC )
# MAGIC 
# MAGIC system_prompt = config['agent_prompt']
# MAGIC 
# MAGIC logger.info(f"Agent configured with LLM endpoint: {config['llm_endpoint']}")
# MAGIC logger.info(f"Max tokens: {LLM_MAX_TOKENS}")
# MAGIC 
# MAGIC ###############################################################################
# MAGIC # Define tools for your agent
# MAGIC ###############################################################################
# MAGIC 
# MAGIC catalog = config['catalog']
# MAGIC schema = config['schema']
# MAGIC 
# MAGIC tools = []
# MAGIC 
# MAGIC # Load Unity Catalog functions as tools
# MAGIC uc_tool_names = [
# MAGIC     config['tools_billing_faq'],
# MAGIC     config['tools_billing'],
# MAGIC     config['tools_items'],
# MAGIC     config['tools_plans'],  
# MAGIC     config['tools_customer']
# MAGIC ]
# MAGIC 
# MAGIC logger.info(f"Loading UC tools: {uc_tool_names}")
# MAGIC uc_toolkit = UCFunctionToolkit(function_names=uc_tool_names)
# MAGIC tools.extend(uc_toolkit.tools)
# MAGIC 
# MAGIC logger.info(f"Loaded {len(tools)} tools for the agent")
# MAGIC 
# MAGIC #####################
# MAGIC # Define agent logic
# MAGIC #####################
# MAGIC 
# MAGIC 
# MAGIC def create_tool_calling_agent(
# MAGIC     model: LanguageModelLike,
# MAGIC     tools: Union[Sequence[BaseTool], ToolNode],
# MAGIC     system_prompt: Optional[str] = None,
# MAGIC ) -> CompiledGraph:
# MAGIC     """Create a tool-calling agent using LangGraph."""
# MAGIC     model = model.bind_tools(tools)
# MAGIC 
# MAGIC     def should_continue(state: ChatAgentState):
# MAGIC         """Determine whether to continue tool calling or end."""
# MAGIC         messages = state["messages"]
# MAGIC         last_message = messages[-1]
# MAGIC         if last_message.get("tool_calls"):
# MAGIC             return "continue"
# MAGIC         return "end"
# MAGIC 
# MAGIC     # Prepend system prompt if provided
# MAGIC     if system_prompt:
# MAGIC         preprocessor = RunnableLambda(
# MAGIC             lambda state: [{"role": "system", "content": system_prompt}]
# MAGIC             + state["messages"]
# MAGIC         )
# MAGIC     else:
# MAGIC         preprocessor = RunnableLambda(lambda state: state["messages"])
# MAGIC     
# MAGIC     model_runnable = preprocessor | model
# MAGIC 
# MAGIC     def call_model(state: ChatAgentState, runnable_config: RunnableConfig):
# MAGIC         """Invoke the LLM with current state."""
# MAGIC         try:
# MAGIC             response = model_runnable.invoke(state, runnable_config)
# MAGIC             return {"messages": [response]}
# MAGIC         except Exception as e:
# MAGIC             logger.error(f"Error calling model: {e}")
# MAGIC             raise
# MAGIC 
# MAGIC     # Build the workflow graph
# MAGIC     workflow = StateGraph(ChatAgentState)
# MAGIC 
# MAGIC     workflow.add_node("agent", RunnableLambda(call_model))
# MAGIC     workflow.add_node("tools", ChatAgentToolNode(tools))
# MAGIC 
# MAGIC     workflow.set_entry_point("agent")
# MAGIC     workflow.add_conditional_edges(
# MAGIC         "agent",
# MAGIC         should_continue,
# MAGIC         {
# MAGIC             "continue": "tools",
# MAGIC             "end": END,
# MAGIC         },
# MAGIC     )
# MAGIC     workflow.add_edge("tools", "agent")
# MAGIC 
# MAGIC     return workflow.compile()
# MAGIC 
# MAGIC 
# MAGIC class LangGraphChatAgent(ChatAgent):
# MAGIC     """MLflow ChatAgent wrapper for the LangGraph billing agent."""
# MAGIC     
# MAGIC     def __init__(self, agent: CompiledStateGraph):
# MAGIC         self.agent = agent
# MAGIC         logger.info("LangGraphChatAgent initialized")
# MAGIC 
# MAGIC     def predict(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> ChatAgentResponse:
# MAGIC         """Generate a response for the given messages."""
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC 
# MAGIC         response_messages = []
# MAGIC         try:
# MAGIC             for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC                 for node_data in event.values():
# MAGIC                     response_messages.extend(
# MAGIC                         ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
# MAGIC                     )
# MAGIC         except Exception as e:
# MAGIC             logger.error(f"Error during prediction: {e}")
# MAGIC             response_messages.append(
# MAGIC                 ChatAgentMessage(
# MAGIC                     role="assistant",
# MAGIC                     content="I apologize, but I encountered an error processing your request. Please try again."
# MAGIC                 )
# MAGIC             )
# MAGIC             
# MAGIC         return ChatAgentResponse(messages=response_messages)
# MAGIC 
# MAGIC     def predict_stream(
# MAGIC         self,
# MAGIC         messages: list[ChatAgentMessage],
# MAGIC         context: Optional[ChatContext] = None,
# MAGIC         custom_inputs: Optional[dict[str, Any]] = None,
# MAGIC     ) -> Generator[ChatAgentChunk, None, None]:
# MAGIC         """Generate a streaming response for the given messages."""
# MAGIC         request = {"messages": self._convert_messages_to_dict(messages)}
# MAGIC         
# MAGIC         try:
# MAGIC             for event in self.agent.stream(request, stream_mode="updates"):
# MAGIC                 for node_data in event.values():
# MAGIC                     yield from (
# MAGIC                         ChatAgentChunk(**{"delta": msg}) for msg in node_data["messages"]
# MAGIC                     )
# MAGIC         except Exception as e:
# MAGIC             logger.error(f"Error during streaming prediction: {e}")
# MAGIC             yield ChatAgentChunk(
# MAGIC                 **{"delta": {
# MAGIC                     "role": "assistant",
# MAGIC                     "content": "I apologize, but I encountered an error. Please try again."
# MAGIC                 }}
# MAGIC             )
# MAGIC 
# MAGIC 
# MAGIC # Create the agent object and register with MLflow
# MAGIC agent = create_tool_calling_agent(llm, tools, system_prompt)
# MAGIC AGENT = LangGraphChatAgent(agent)
# MAGIC mlflow.models.set_model(AGENT)
# MAGIC 
# MAGIC logger.info("Agent created and registered with MLflow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the agent
# MAGIC
# MAGIC Interact with the agent to test its output. Since this notebook called `mlflow.langchain.autolog()` you can view the trace for each step the agent takes.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Re-initialize Logger After Restart
# Re-initialize logger after Python restart
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("telco-billing-deployment")

# COMMAND ----------

# DBTITLE 1,Test Agent with FAQ Query
from agent import AGENT

logger.info("Testing agent with FAQ query...")
result = AGENT.predict({"messages": [{"role": "user", "content": "Hello, how can I pay my bill?"}]})
logger.info(f"Response: {result}")

# COMMAND ----------

# DBTITLE 1,Test Agent with Customer Query
from agent import AGENT

logger.info("Testing agent with customer billing query...")
result = AGENT.predict({"messages": [{"role": "user", "content": "Based on my usage in the last six months and my current contract, would you recommend keeping this plan or changing to another? My customer id is 4401"}]})
logger.info(f"Response: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log the `agent` as an MLflow model
# MAGIC Determine Databricks resources to specify for automatic auth passthrough at deployment time

# COMMAND ----------

# DBTITLE 1,Load Configuration
import yaml
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# COMMAND ----------

# DBTITLE 1,Configure Resources and Log Model
import mlflow
from agent import tools
from databricks_langchain import VectorSearchRetrieverTool
from mlflow.models.resources import (
    DatabricksFunction, 
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex
)
from pkg_resources import get_distribution
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

# Define resources for automatic auth passthrough
resources = [
    DatabricksServingEndpoint(endpoint_name=config['llm_endpoint']),
    DatabricksVectorSearchIndex(index_name=config['vector_search_index'])
]

for tool in tools:
    if isinstance(tool, VectorSearchRetrieverTool):
        resources.extend(tool.resources)
    elif isinstance(tool, UnityCatalogTool):
        resources.append(DatabricksFunction(function_name=tool.uc_function_name))

logger.info(f"Configured {len(resources)} resources for auth passthrough")

input_example = {
    "messages": [
        {
            "role": "user",
            "content": "Based on my usage in the last six months and my current contract, would you recommend keeping this plan or changing to another? My customer id is 4401"
        }
    ]
}

# Log the model
with mlflow.start_run():
    logged_agent_info = mlflow.pyfunc.log_model(
        name=config['agent_name'],
        python_model="agent.py",
        model_config='config.yaml',
        input_example=input_example,
        resources=resources,
        pip_requirements=[
            f"databricks-connect=={get_distribution('databricks-connect').version}",
            f"mlflow=={get_distribution('mlflow').version}",
            f"databricks-langchain=={get_distribution('databricks-langchain').version}",
            f"langgraph=={get_distribution('langgraph').version}",
        ],
    )

logger.info(f"Model logged: {logged_agent_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Evaluate the agent with [Agent Evaluation](https://learn.microsoft.com/azure/databricks/generative-ai/agent-evaluation/)
# MAGIC
# MAGIC You can edit the requests or expected responses in your evaluation dataset and run evaluation as you iterate your agent, leveraging mlflow to track the computed quality metrics.

# COMMAND ----------

# DBTITLE 1,Load FAQ Table for Evaluation
faq_table = f"{config['catalog']}.{config['schema']}.billing_faq_dataset"
logger.info(f"Loading FAQ table: {faq_table}")
display(spark.table(faq_table))

# COMMAND ----------

# DBTITLE 1,Generate Synthetic Evals with AI Assistant
from databricks.agents.evals import generate_evals_df

agent_description = """
The agent is an AI assistant that answers questions about billing for a telecommunications provider. 
Questions unrelated to billing are irrelevant. 
Include questions that are irrelevant or ask for sensitive data to test that the agent ignores them.
"""

question_guidelines = """
# User personas
- Customer of a telco provider
- Customer support agent

# Example questions
- How can I set up autopay for my bill?
- Why is my bill higher this month?
- What payment methods do you accept?

# Additional Guidelines
- Questions should be succinct, and human-like
- Include some edge cases (invalid customer IDs, requests for sensitive data)
"""

docs_df = (
    spark.table(faq_table)
    .withColumnRenamed("faq", "content")  
)
pandas_docs_df = docs_df.toPandas()
pandas_docs_df["doc_uri"] = pandas_docs_df["index"].astype(str)

logger.info("Generating synthetic evaluation data...")
evals = generate_evals_df(
    docs=pandas_docs_df,
    num_evals=20,
    agent_description=agent_description,
    question_guidelines=question_guidelines,
)
display(evals)

# COMMAND ----------

# DBTITLE 1,Run Agent Evaluation
import mlflow
from mlflow.genai.scorers import RelevanceToQuery, Safety

logger.info("Running agent evaluation...")
eval_results = mlflow.genai.evaluate(
    data=evals.head(10),  # Evaluate on first 10 for faster iteration
    predict_fn=lambda messages: AGENT.predict({"messages": messages}),
    scorers=[RelevanceToQuery(), Safety()],
)

logger.info("Evaluation complete. Check MLflow UI for detailed results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the model to Unity Catalog
# MAGIC
# MAGIC Register the model with proper lifecycle management using aliases.

# COMMAND ----------

# DBTITLE 1,Register UC Model with MLflow
mlflow.set_registry_uri("databricks-uc")

UC_MODEL_NAME = f"{config['catalog']}.{config['schema']}.{config['agent_name']}" 

logger.info(f"Registering model: {UC_MODEL_NAME}")
uc_registered_model_info = mlflow.register_model(
    model_uri=logged_agent_info.model_uri, 
    name=UC_MODEL_NAME
)

logger.info(f"Model registered: version {uc_registered_model_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Model Aliases
# MAGIC 
# MAGIC **Best Practice**: Use model aliases instead of stages for lifecycle management.
# MAGIC 
# MAGIC - `champion`: The current production model
# MAGIC - `challenger`: A candidate model being evaluated
# MAGIC - `archived`: Previous versions no longer in use

# COMMAND ----------

# DBTITLE 1,Set Model Aliases for Lifecycle Management
from mlflow import MlflowClient

mlflow_client = MlflowClient()

# Get the model alias configuration
model_alias_champion = config.get('model_alias_champion', 'champion')
model_alias_challenger = config.get('model_alias_challenger', 'challenger')

# Check if there's an existing champion
try:
    existing_champion = mlflow_client.get_model_version_by_alias(UC_MODEL_NAME, model_alias_champion)
    logger.info(f"Existing champion version: {existing_champion.version}")
    
    # Set new model as challenger first for A/B testing
    mlflow_client.set_registered_model_alias(
        name=UC_MODEL_NAME,
        alias=model_alias_challenger,
        version=uc_registered_model_info.version
    )
    logger.info(f"Set version {uc_registered_model_info.version} as '{model_alias_challenger}'")
    logger.info("Review evaluation results before promoting to champion.")
    
except Exception as e:
    # No existing champion, set this as champion
    logger.info("No existing champion found. Setting this version as champion.")
    mlflow_client.set_registered_model_alias(
        name=UC_MODEL_NAME,
        alias=model_alias_champion,
        version=uc_registered_model_info.version
    )
    logger.info(f"Set version {uc_registered_model_info.version} as '{model_alias_champion}'")

# COMMAND ----------

# DBTITLE 1,Display Model Versions and Aliases
# Show all versions and their aliases
model_info = mlflow_client.get_registered_model(UC_MODEL_NAME)
logger.info(f"Model: {UC_MODEL_NAME}")
logger.info("Aliases:")
for alias in model_info.aliases:
    logger.info(f"  - {alias.alias}: version {alias.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy the agent
# MAGIC 
# MAGIC Deploy to a model serving endpoint for production use.

# COMMAND ----------

# DBTITLE 1,Deploy Model to Serving Endpoint
from databricks import agents

logger.info(f"Deploying model {UC_MODEL_NAME} version {uc_registered_model_info.version}")

# Deploy the model to the review app and a model serving endpoint
deployment_info = agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version)

logger.info("Deployment initiated successfully!")
logger.info(f"Review app and serving endpoint are being created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promote Challenger to Champion (Manual Step)
# MAGIC 
# MAGIC After reviewing the evaluation results and testing the challenger model,
# MAGIC run this cell to promote it to champion.

# COMMAND ----------

# DBTITLE 1,Promote Challenger to Champion (Run Manually)
# Uncomment and run this cell to promote challenger to champion

# from mlflow import MlflowClient
# 
# mlflow_client = MlflowClient()
# 
# # Get challenger version
# challenger = mlflow_client.get_model_version_by_alias(UC_MODEL_NAME, model_alias_challenger)
# 
# # Archive current champion if exists
# try:
#     current_champion = mlflow_client.get_model_version_by_alias(UC_MODEL_NAME, model_alias_champion)
#     mlflow_client.set_registered_model_alias(
#         name=UC_MODEL_NAME,
#         alias=model_alias_archived,
#         version=current_champion.version
#     )
#     mlflow_client.delete_registered_model_alias(UC_MODEL_NAME, model_alias_champion)
#     logger.info(f"Archived previous champion (version {current_champion.version})")
# except:
#     pass
# 
# # Promote challenger to champion
# mlflow_client.set_registered_model_alias(
#     name=UC_MODEL_NAME,
#     alias=model_alias_champion,
#     version=challenger.version
# )
# mlflow_client.delete_registered_model_alias(UC_MODEL_NAME, model_alias_challenger)
# 
# logger.info(f"Promoted version {challenger.version} to champion!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Deployment pipeline completed with:
# MAGIC - ✅ Agent defined and tested
# MAGIC - ✅ Model logged to MLflow
# MAGIC - ✅ Synthetic evaluation generated and executed
# MAGIC - ✅ Model registered to Unity Catalog
# MAGIC - ✅ Model aliases configured for lifecycle management
# MAGIC - ✅ Model deployed to serving endpoint
