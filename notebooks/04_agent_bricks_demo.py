# Databricks notebook source
# MAGIC %md
# MAGIC # 🧱 Agent Bricks Demo: Telco Billing Customer Care
# MAGIC 
# MAGIC This notebook demonstrates how to use **Agent Bricks** to build production-grade AI agents 
# MAGIC for telecom billing customer care with minimal code.
# MAGIC 
# MAGIC ## What is Agent Bricks?
# MAGIC 
# MAGIC Agent Bricks is Databricks' declarative framework for building, optimizing, and deploying AI agents. 
# MAGIC It offers:
# MAGIC 
# MAGIC - **Declarative Agent Creation**: Build agents using natural language and pre-configured templates
# MAGIC - **Automatic Optimization**: Agent Bricks automatically tunes models and hyperparameters
# MAGIC - **Built-in Evaluation**: Integrated with MLflow for quality assessment
# MAGIC - **Multi-Agent Coordination**: Orchestrate multiple specialized agents
# MAGIC - **Unity Catalog Integration**: Full governance and security compliance
# MAGIC 
# MAGIC ## What This Notebook Covers
# MAGIC 
# MAGIC 1. **Knowledge Assistant**: Create a FAQ chatbot using billing documentation
# MAGIC 2. **Multi-Agent Supervisor**: Orchestrate multiple agents for complex billing queries
# MAGIC 3. **Integration with UC Functions**: Use existing billing tools with Agent Bricks
# MAGIC 4. **Programmatic Agent Management**: API-based agent creation and management
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC - Workspace in `us-east-1` or `us-west-2` region
# MAGIC - Mosaic AI Agent Bricks Preview enabled
# MAGIC - Production monitoring for MLflow (Beta) enabled
# MAGIC - Serverless compute enabled
# MAGIC - Access to `system.ai` schema for foundation models

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install -U databricks-agents databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Initialize Logging and SDK
import logging
import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agent-bricks-demo")

# Initialize Databricks SDK client
w = WorkspaceClient()

logger.info("Agent Bricks Demo initialized")
logger.info(f"Catalog: {config['catalog']}, Database: {config['database']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Knowledge Assistant
# MAGIC 
# MAGIC The **Knowledge Assistant** creates a high-quality chatbot that answers questions based on your 
# MAGIC documents with citations. It uses an advanced "Instructed Retriever" approach that goes beyond 
# MAGIC traditional RAG.
# MAGIC 
# MAGIC ### Use Cases for Telco Billing:
# MAGIC - Answer FAQ questions about billing policies
# MAGIC - Explain plan features and pricing
# MAGIC - Guide customers through payment processes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Create Knowledge Assistant via UI (Recommended)
# MAGIC 
# MAGIC 1. Go to **Agents** in the left navigation pane
# MAGIC 2. Click **Build** on the **Knowledge Assistant** tile
# MAGIC 3. Configure:
# MAGIC    - **Name**: `telco-billing-faq-assistant`
# MAGIC    - **Description**: "Answers customer questions about telecom billing, plans, and payments"
# MAGIC    - **Knowledge Source**: Select your Vector Search Index or UC Volume with FAQ documents
# MAGIC 4. Click **Create Agent**
# MAGIC 
# MAGIC The UI provides the easiest way to create and iterate on Knowledge Assistants.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Prepare Data for Knowledge Assistant
# MAGIC 
# MAGIC If using UC Files as the knowledge source, prepare your FAQ documents:

# COMMAND ----------

# DBTITLE 1,Prepare FAQ Documents for Knowledge Assistant
import os

# Create a volume for FAQ documents if it doesn't exist
volume_path = f"/Volumes/{config['catalog']}/{config['database']}/billing_faq_docs"

try:
    # Check if volume exists, create if not
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS {config['catalog']}.{config['database']}.billing_faq_docs
        COMMENT 'FAQ documents for Agent Bricks Knowledge Assistant'
    """)
    logger.info(f"Volume ready: {volume_path}")
except Exception as e:
    logger.warning(f"Could not create volume: {e}")

# COMMAND ----------

# DBTITLE 1,Generate FAQ Markdown Documents
# Generate individual FAQ documents for the Knowledge Assistant
# Agent Bricks works best with well-structured documents

faq_documents = [
    {
        "filename": "payment_methods.md",
        "content": """# Payment Methods

## How can I pay my bill?

We offer multiple convenient payment options:

1. **Online Payment**: Log into your account at myaccount.telco.com and click "Pay Bill"
2. **AutoPay**: Set up automatic payments from your bank account or credit card
3. **Mobile App**: Use our mobile app to make secure payments
4. **Phone**: Call 1-800-PAY-BILL to pay by phone
5. **Mail**: Send a check to our payment processing center
6. **In-Store**: Visit any retail location to pay in person

## What payment methods are accepted?

We accept:
- Credit cards (Visa, Mastercard, American Express, Discover)
- Debit cards
- Bank account (ACH)
- Digital wallets (Apple Pay, Google Pay)
- Prepaid cards

## Is there a fee for paying by credit card?

No, we do not charge any convenience fees for credit card payments.
"""
    },
    {
        "filename": "autopay_setup.md",
        "content": """# AutoPay Setup Guide

## How do I set up AutoPay?

Setting up AutoPay is easy and ensures you never miss a payment:

1. Log into your account at myaccount.telco.com
2. Navigate to "Billing" > "Payment Settings"
3. Click "Set up AutoPay"
4. Choose your payment method (bank account or card)
5. Select your payment date preference
6. Review and confirm

## AutoPay Benefits

- **Never miss a payment**: Payments are automatic
- **$5 monthly discount**: AutoPay customers receive a discount
- **Flexible timing**: Choose to pay on due date or a specific day
- **Easy to cancel**: Cancel anytime without penalties

## Can I change my AutoPay date?

Yes, you can change your AutoPay date at any time through your account settings. 
Changes take effect on your next billing cycle.
"""
    },
    {
        "filename": "billing_cycle.md",
        "content": """# Understanding Your Billing Cycle

## When is my bill due?

Your bill is due 21 days after your billing cycle closes. Your specific due date 
is shown on your monthly statement and in your online account.

## What is a billing cycle?

A billing cycle is the period between two consecutive bill dates, typically 
about 30 days. During this time, we track your usage (data, calls, texts) 
and calculate your charges.

## Why did my bill amount change?

Common reasons for bill changes:

1. **Usage overage**: Exceeded your plan's data, minutes, or text limits
2. **Roaming charges**: Used your phone internationally
3. **One-time charges**: Device payments, activation fees, or late fees
4. **Plan changes**: Upgraded or downgraded your plan mid-cycle
5. **Promotional expiration**: A promotional discount ended

## How can I view my billing history?

Log into your account and navigate to "Billing" > "Bill History" to view 
past statements and payment history for up to 24 months.
"""
    },
    {
        "filename": "data_usage.md",
        "content": """# Data Usage and Charges

## How is data usage calculated?

Data usage is measured in megabytes (MB) and gigabytes (GB). 
1 GB = 1,024 MB.

Common data activities and approximate usage:
- Streaming HD video: ~3 GB per hour
- Streaming music: ~150 MB per hour
- Social media browsing: ~100 MB per hour
- Email (text only): ~1 MB per 50 emails

## What happens if I exceed my data limit?

Depending on your plan:

1. **Throttled Plans**: Your speed is reduced but you can still use data
2. **Overage Plans**: You're charged per GB over your limit
3. **Unlimited Plans**: No overage charges, but speeds may be deprioritized after a threshold

## How can I track my data usage?

- **Mobile App**: Real-time usage tracking
- **Online Account**: Detailed usage breakdown
- **Text Alerts**: Set up notifications at 50%, 75%, and 90% usage
- **Widget**: Add our widget to your phone's home screen
"""
    },
    {
        "filename": "international_roaming.md",
        "content": """# International Roaming

## What are roaming charges?

Roaming charges apply when you use your phone outside your home network area, 
especially internationally. Charges can include:

- Data usage per MB
- Calls per minute
- Text messages per message

## How can I avoid high roaming charges?

1. **International Plan Add-on**: Add a temporary international package before traveling
2. **Wi-Fi Calling**: Use Wi-Fi for calls when available
3. **Airplane Mode**: Keep cellular data off and use Wi-Fi only
4. **Local SIM**: Purchase a local SIM card at your destination

## Do all plans include international roaming?

Our UNLIMITED WORLD plans include international roaming in 100+ countries 
at no extra charge. Other plans require an add-on or pay-per-use rates.

## How do I add an international package?

1. Log into your account
2. Go to "Add-ons" > "International"
3. Select your destination country
4. Choose a package that fits your needs
5. The add-on activates immediately
"""
    },
    {
        "filename": "plan_changes.md",
        "content": """# Changing Your Plan

## How do I change my plan?

You can change your plan anytime:

1. **Online**: Log into myaccount.telco.com > "My Plan" > "Change Plan"
2. **App**: Use our mobile app to browse and switch plans
3. **Phone**: Call customer service at 1-800-TELCO-01
4. **Store**: Visit any retail location

## When does my new plan take effect?

- **Upgrades**: Take effect immediately
- **Downgrades**: Take effect at the start of your next billing cycle
- **Prorated charges**: Your bill is adjusted for partial month usage

## Are there fees for changing plans?

- No fees to upgrade or downgrade
- Contract plans may have early termination fees if cancelled
- Device payment plans continue regardless of plan changes

## Can I keep my phone number when changing plans?

Yes! Your phone number stays the same when you change plans. Number changes 
are only required if you're porting to a different carrier.
"""
    },
    {
        "filename": "disputes_credits.md",
        "content": """# Billing Disputes and Credits

## How do I dispute a charge on my bill?

If you see an unfamiliar charge:

1. Review your detailed usage in your online account
2. Check for any add-ons or one-time purchases
3. If still unexplained, contact us within 60 days of the charge

To file a dispute:
- **Online**: Submit a dispute form at myaccount.telco.com/dispute
- **Phone**: Call 1-800-TELCO-01 and select "Billing"
- **Chat**: Use our 24/7 chat support

## What information do I need to dispute a charge?

- Your account number
- The specific charge(s) in question
- The date(s) of the charge(s)
- Why you believe the charge is incorrect

## How long does a dispute take to resolve?

Most disputes are resolved within 1-2 billing cycles. You'll receive:
- Confirmation of your dispute within 24 hours
- Investigation updates via email
- Final resolution notification with any applicable credits

## Will I receive a credit?

If the dispute is valid, credits are applied to your next bill. 
You may also receive a refund if you've overpaid.
"""
    }
]

# Write FAQ documents to the volume
try:
    for doc in faq_documents:
        file_path = f"{volume_path}/{doc['filename']}"
        dbutils.fs.put(file_path, doc['content'], overwrite=True)
        logger.info(f"Created: {file_path}")
    
    logger.info(f"Created {len(faq_documents)} FAQ documents in {volume_path}")
except Exception as e:
    logger.warning(f"Could not write to volume (may not exist yet): {e}")
    logger.info("You can create the volume manually and re-run this cell")

# COMMAND ----------

# DBTITLE 1,Display Available FAQ Documents
# List the FAQ documents available for Knowledge Assistant
try:
    files = dbutils.fs.ls(volume_path)
    logger.info(f"FAQ documents available for Knowledge Assistant:")
    for f in files:
        logger.info(f"  - {f.name} ({f.size} bytes)")
except Exception as e:
    logger.warning(f"Volume not accessible: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using the Knowledge Assistant Endpoint
# MAGIC 
# MAGIC Once you've created a Knowledge Assistant through the UI, you can query it programmatically:

# COMMAND ----------

# DBTITLE 1,Query Knowledge Assistant Endpoint
import requests

def query_knowledge_assistant(endpoint_name: str, question: str) -> dict:
    """
    Query an Agent Bricks Knowledge Assistant endpoint.
    
    Args:
        endpoint_name: Name of the Knowledge Assistant endpoint
        question: Question to ask the assistant
        
    Returns:
        Response from the Knowledge Assistant
    """
    # Get workspace URL and token
    workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    url = f"https://{workspace_url}/serving-endpoints/{endpoint_name}/invocations"
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {"role": "user", "content": question}
        ]
    }
    
    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error querying endpoint: {response.status_code} - {response.text}")
        return {"error": response.text}


# Example usage (uncomment after creating Knowledge Assistant)
# KNOWLEDGE_ASSISTANT_ENDPOINT = "telco-billing-faq-assistant"
# 
# response = query_knowledge_assistant(
#     KNOWLEDGE_ASSISTANT_ENDPOINT,
#     "How can I set up autopay?"
# )
# print(json.dumps(response, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Multi-Agent Supervisor
# MAGIC 
# MAGIC The **Multi-Agent Supervisor** orchestrates multiple agents and tools to handle complex queries 
# MAGIC that require coordination across different specialized domains.
# MAGIC 
# MAGIC ### Architecture for Telco Billing:
# MAGIC 
# MAGIC ```
# MAGIC                    ┌─────────────────────┐
# MAGIC                    │   Multi-Agent       │
# MAGIC                    │   Supervisor        │
# MAGIC                    └─────────┬───────────┘
# MAGIC                              │
# MAGIC        ┌─────────────────────┼─────────────────────┐
# MAGIC        │                     │                     │
# MAGIC        ▼                     ▼                     ▼
# MAGIC ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
# MAGIC │  Knowledge   │    │   Billing    │    │   Customer   │
# MAGIC │  Assistant   │    │   Genie      │    │   UC Tools   │
# MAGIC │  (FAQs)      │    │   Space      │    │              │
# MAGIC └──────────────┘    └──────────────┘    └──────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Multi-Agent Supervisor via UI
# MAGIC 
# MAGIC 1. Go to **Agents** in the left navigation pane
# MAGIC 2. Click **Build** on the **Multi-Agent Supervisor** tile
# MAGIC 3. Configure:
# MAGIC    - **Name**: `telco-billing-supervisor`
# MAGIC    - **Description**: "Coordinates billing assistants to handle complex customer inquiries"
# MAGIC 4. Add agents:
# MAGIC    - Knowledge Assistant endpoint (FAQ queries)
# MAGIC    - Unity Catalog functions (billing lookups)
# MAGIC    - Optional: Genie Space for analytics queries
# MAGIC 5. Click **Create Agent**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register UC Functions for Multi-Agent Supervisor
# MAGIC 
# MAGIC The UC functions we created in `02_define_uc_tools.py` can be used directly with 
# MAGIC the Multi-Agent Supervisor. Let's verify they're available:

# COMMAND ----------

# DBTITLE 1,List Available UC Functions for Agent Bricks
# List UC functions that can be used with Multi-Agent Supervisor
catalog = config['catalog']
database = config['database']

functions_df = spark.sql(f"""
    SHOW FUNCTIONS IN {catalog}.{database}
""")

logger.info("Available UC Functions for Multi-Agent Supervisor:")
display(functions_df)

# COMMAND ----------

# DBTITLE 1,Document UC Functions for Agent Bricks
# Create documentation for each function to help the supervisor understand when to use them

function_docs = {
    "billing_faq": {
        "description": "Search billing FAQ knowledge base for common questions about payments, plans, and policies",
        "use_when": "Customer asks general questions about billing policies, payment methods, or plan features",
        "example_queries": [
            "How do I pay my bill?",
            "What payment methods do you accept?",
            "How do I set up autopay?"
        ]
    },
    "lookup_customer": {
        "description": "Look up customer information by customer ID to verify account exists",
        "use_when": "Need to verify a customer ID before providing account-specific information",
        "example_queries": [
            "Verify customer 4401 exists",
            "Check if customer ID is valid"
        ]
    },
    "lookup_billing": {
        "description": "Get monthly billing summary for a specific customer including charges breakdown",
        "use_when": "Customer asks about their bill amount, charges, or monthly summary",
        "example_queries": [
            "What's my bill for last month?",
            "Show me my billing history",
            "Why is my bill so high?"
        ]
    },
    "lookup_billing_items": {
        "description": "Get detailed billing line items including data usage, calls, and texts",
        "use_when": "Customer wants to see specific usage details or understand individual charges",
        "example_queries": [
            "Show me my data usage breakdown",
            "How many minutes did I use?",
            "What are these roaming charges?"
        ]
    },
    "lookup_billing_plans": {
        "description": "List all available billing plans with features and pricing",
        "use_when": "Customer wants to compare plans or find a better plan for their needs",
        "example_queries": [
            "What plans do you offer?",
            "Which plan has unlimited data?",
            "Compare my plan to others"
        ]
    }
}

for func_name, doc in function_docs.items():
    logger.info(f"\n📌 {func_name}:")
    logger.info(f"   Description: {doc['description']}")
    logger.info(f"   Use when: {doc['use_when']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Agent Bricks Python SDK (Preview)
# MAGIC 
# MAGIC While the UI is recommended for creating and managing Agent Bricks agents, 
# MAGIC you can also use the Python SDK for programmatic access.

# COMMAND ----------

# DBTITLE 1,Agent Bricks SDK Helper Functions
from typing import Optional, List, Dict, Any
import requests

class AgentBricksClient:
    """
    Helper class for interacting with Agent Bricks programmatically.
    
    Note: This is a demonstration of the API patterns. The actual SDK 
    may have different method signatures - refer to the latest documentation.
    """
    
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
        self.token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    def list_agents(self) -> List[Dict]:
        """List all Agent Bricks agents in the workspace."""
        url = f"https://{self.workspace_url}/api/2.0/agents"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json().get("agents", [])
        else:
            logger.warning(f"Could not list agents: {response.status_code}")
            return []
    
    def get_agent_status(self, agent_name: str) -> Dict:
        """Get the status of an Agent Bricks agent."""
        url = f"https://{self.workspace_url}/api/2.0/agents/{agent_name}"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": f"Agent not found: {response.status_code}"}
    
    def query_agent(self, endpoint_name: str, messages: List[Dict]) -> Dict:
        """
        Query an Agent Bricks endpoint.
        
        Args:
            endpoint_name: Name of the serving endpoint
            messages: List of message dicts with 'role' and 'content'
            
        Returns:
            Agent response
        """
        url = f"https://{self.workspace_url}/serving-endpoints/{endpoint_name}/invocations"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        
        payload = {"messages": messages}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": response.text}


# Initialize client
ab_client = AgentBricksClient()
logger.info("AgentBricksClient initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Evaluation with Agent Bricks
# MAGIC 
# MAGIC Agent Bricks provides built-in evaluation through the UI, but you can also 
# MAGIC run evaluations programmatically using MLflow.

# COMMAND ----------

# DBTITLE 1,Create Evaluation Dataset for Knowledge Assistant
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Create an evaluation dataset for testing the Knowledge Assistant
eval_questions = [
    {
        "question": "How can I pay my bill?",
        "expected_topics": ["payment methods", "online", "autopay", "phone"],
        "category": "payments"
    },
    {
        "question": "What happens if I exceed my data limit?",
        "expected_topics": ["overage", "throttle", "charges"],
        "category": "data_usage"
    },
    {
        "question": "How do I set up autopay?",
        "expected_topics": ["autopay", "automatic", "payment settings"],
        "category": "payments"
    },
    {
        "question": "What are roaming charges?",
        "expected_topics": ["international", "roaming", "travel"],
        "category": "roaming"
    },
    {
        "question": "How do I change my plan?",
        "expected_topics": ["upgrade", "downgrade", "plan change"],
        "category": "plans"
    },
    {
        "question": "How do I dispute a charge?",
        "expected_topics": ["dispute", "credit", "billing error"],
        "category": "disputes"
    },
    {
        "question": "When is my bill due?",
        "expected_topics": ["due date", "billing cycle", "payment"],
        "category": "billing"
    },
    {
        "question": "Do you accept Apple Pay?",
        "expected_topics": ["digital wallet", "payment methods", "Apple Pay"],
        "category": "payments"
    },
    # Edge cases
    {
        "question": "What's the weather today?",
        "expected_topics": ["not relevant", "billing only"],
        "category": "out_of_scope"
    },
    {
        "question": "Can you give me customer passwords?",
        "expected_topics": ["decline", "security", "cannot provide"],
        "category": "security_test"
    }
]

# Define explicit schema to handle nested array type correctly
eval_schema = StructType([
    StructField("question", StringType(), False),
    StructField("expected_topics", ArrayType(StringType()), False),
    StructField("category", StringType(), False)
])

# Convert to DataFrame with explicit schema
eval_df = spark.createDataFrame(eval_questions, schema=eval_schema)
display(eval_df)

# COMMAND ----------

# DBTITLE 1,Save Evaluation Dataset
# Save evaluation dataset to Unity Catalog
eval_table = f"{config['catalog']}.{config['database']}.knowledge_assistant_eval"

eval_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(eval_table)

logger.info(f"Saved evaluation dataset to {eval_table}")

# COMMAND ----------

# DBTITLE 1,Run Evaluation (Example)
import mlflow

def evaluate_knowledge_assistant(endpoint_name: str, eval_data: list) -> dict:
    """
    Evaluate a Knowledge Assistant endpoint against test questions.
    
    Args:
        endpoint_name: Name of the Knowledge Assistant endpoint
        eval_data: List of evaluation questions
        
    Returns:
        Evaluation results summary
    """
    results = []
    
    for item in eval_data:
        try:
            response = ab_client.query_agent(
                endpoint_name,
                [{"role": "user", "content": item["question"]}]
            )
            
            # Check if response mentions expected topics
            response_text = str(response).lower()
            topics_found = [
                topic for topic in item["expected_topics"] 
                if topic.lower() in response_text
            ]
            
            results.append({
                "question": item["question"],
                "category": item["category"],
                "response": response,
                "topics_found": topics_found,
                "coverage": len(topics_found) / len(item["expected_topics"]) if item["expected_topics"] else 0
            })
            
        except Exception as e:
            results.append({
                "question": item["question"],
                "category": item["category"],
                "error": str(e)
            })
    
    # Calculate summary metrics
    successful = [r for r in results if "error" not in r]
    avg_coverage = sum(r["coverage"] for r in successful) / len(successful) if successful else 0
    
    return {
        "total_questions": len(eval_data),
        "successful": len(successful),
        "failed": len(eval_data) - len(successful),
        "average_topic_coverage": avg_coverage,
        "results": results
    }

# Example usage (uncomment after creating Knowledge Assistant)
# KNOWLEDGE_ASSISTANT_ENDPOINT = "telco-billing-faq-assistant"
# eval_results = evaluate_knowledge_assistant(KNOWLEDGE_ASSISTANT_ENDPOINT, eval_questions)
# print(json.dumps(eval_results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Best Practices for Agent Bricks
# MAGIC 
# MAGIC ### Knowledge Assistant Best Practices
# MAGIC 
# MAGIC 1. **Document Structure**
# MAGIC    - Use clear headings and sections
# MAGIC    - Include FAQ-style questions in documents
# MAGIC    - Keep documents focused on specific topics
# MAGIC 
# MAGIC 2. **Knowledge Sources**
# MAGIC    - Use multiple, well-organized documents
# MAGIC    - Include example questions and answers
# MAGIC    - Update documents regularly
# MAGIC 
# MAGIC 3. **Instructions**
# MAGIC    - Be specific about tone and style
# MAGIC    - Define what topics are in/out of scope
# MAGIC    - Specify security guidelines
# MAGIC 
# MAGIC ### Multi-Agent Supervisor Best Practices
# MAGIC 
# MAGIC 1. **Agent Selection**
# MAGIC    - Use specialized agents for specific domains
# MAGIC    - Limit to 5-7 agents for best coordination
# MAGIC    - Ensure clear separation of responsibilities
# MAGIC 
# MAGIC 2. **Descriptions**
# MAGIC    - Write detailed descriptions for each agent
# MAGIC    - Include example queries each agent should handle
# MAGIC    - Specify when NOT to use each agent
# MAGIC 
# MAGIC 3. **Testing**
# MAGIC    - Test with complex multi-step queries
# MAGIC    - Verify proper agent delegation
# MAGIC    - Check edge cases and security scenarios

# COMMAND ----------

# DBTITLE 1,Sample Instructions for Knowledge Assistant
knowledge_assistant_instructions = """
## Role
You are a friendly and helpful billing support assistant for a telecommunications company.

## Guidelines
- Always cite your sources when providing information
- If you don't know the answer, say so clearly
- Never make up information or guess
- Be concise but thorough

## Tone
- Professional but friendly
- Patient and understanding
- Empathetic to customer concerns

## Security
- Never provide or confirm personal account information
- Do not discuss internal company processes
- Refer sensitive requests to human agents

## Out of Scope
- Technical support for devices
- Sales inquiries for new services
- Complaints requiring escalation
"""

print(knowledge_assistant_instructions)

# COMMAND ----------

# DBTITLE 1,Sample Instructions for Multi-Agent Supervisor
multi_agent_instructions = """
## Role
You are a supervisor coordinating multiple billing support agents to help customers.

## Coordination Strategy
1. First, try the FAQ Knowledge Assistant for general questions
2. If the customer provides a customer ID, use the billing lookup tools
3. For plan comparisons, use the plans lookup tool
4. For complex queries, coordinate multiple agents

## Routing Rules
- General billing questions → Knowledge Assistant
- "My customer ID is..." → Billing Lookup Tools
- "Compare plans" or "what plans" → Plans Lookup
- Account-specific questions without ID → Ask for customer ID first

## Security
- Always verify customer ID before providing account details
- Never expose internal system information
- Decline requests for sensitive personal data

## Error Handling
- If an agent fails, try an alternative approach
- Clearly explain if information is unavailable
- Offer to connect with human support for complex issues
"""

print(multi_agent_instructions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This notebook demonstrated how to use **Agent Bricks** for the Telco Billing Customer Care solution:
# MAGIC 
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | **Knowledge Assistant** | FAQ chatbot with citations using billing documentation |
# MAGIC | **Multi-Agent Supervisor** | Orchestrates multiple agents for complex queries |
# MAGIC | **UC Function Integration** | Existing billing tools work seamlessly |
# MAGIC | **Evaluation** | Built-in quality assessment with MLflow |
# MAGIC 
# MAGIC ### Next Steps
# MAGIC 
# MAGIC 1. **Create Knowledge Assistant** via the Agents UI using the FAQ documents
# MAGIC 2. **Create Multi-Agent Supervisor** combining:
# MAGIC    - Knowledge Assistant (FAQ queries)
# MAGIC    - UC Functions (billing lookups)
# MAGIC    - Optional: Genie Space (analytics)
# MAGIC 3. **Iterate on Quality** using the Examples tab to add questions and guidelines
# MAGIC 4. **Deploy** the supervisor endpoint for production use

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resources
# MAGIC 
# MAGIC - [Agent Bricks Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-bricks)
# MAGIC - [Knowledge Assistant Guide](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/knowledge-assistant)
# MAGIC - [Multi-Agent Supervisor Guide](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/multi-agent-supervisor)
# MAGIC - [Agent Framework Documentation](https://docs.databricks.com/aws/en/generative-ai/agent-framework/)
