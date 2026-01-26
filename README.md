<img src=https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo.png width="600px">


## Business Problem

Telecoms are leveraging AI to achieve first-point resolution on customer issues and unify fragmented customer data to proactively enhance customer engagement and retention. This solution leverages AI to analyze incoming customer communications, understanding context and sentiment to prepare tailored responses for agents. We have picked one of the common billing issues for telco customers. 

This industry solution accelerator enables the automation and personalization of telecom billing customer care by leveraging customer-specific data available within the data ecosystem.

The aim is to help telco providers scale customer service operations with an AI-powered billing agent that leverages:
- Billing history, customer profiles, and device data
- Unstructured FAQs embedded in a vector search index
- Human-in-the-loop evaluation and observability tools
- A deployable web interface for interactive usage

Designed as a human-in-the-loop solution, it empowers customer service agents to resolve billing queries faster and more accurately, improving CX and reducing call centre load.

<p align="center">
  <img src="./images/billing_assistant.jpg" alt="Billing Assistant Diagram" width="600"/>
</p>


---


## Authors
Kyra Wulffert <kyra.wulffert@databricks.com><br>
Sachin Patil <sachin.patil@databricks.com>

---

## Repository Structure

| Notebook | Description |
|----------|-------------|
| `000-config` | Central config for the accelerator with environment-aware settings and Databricks Secrets integration |
| `00_data_preparation` | Synthetic data generation using [Databricks Labs Data Generator](https://github.com/databrickslabs/dbldatagen). Simulates billing, device, and customer datasets. |
| `01_create_vector_search` | Builds the FAQ dataset and creates a vector search index using Databricks Vector Search with hybrid search support. |
| `02_define_uc_tools` | Defines functions as tools in Unity Catalog with input validation. These are callable by the agent to query customer, billing, and device information and retrieve relevant data from the vector search with FAQ. |
| `03_agent_deployment_and_evaluation` | Builds, logs, evaluates, registers, and deploys the agent to a model serving endpoint. Includes synthetic evaluation via the FAQ dataset and model alias management. |
| `04_agent_bricks_demo` | **NEW**: Demonstrates Agent Bricks for building production-grade AI agents with declarative configuration, automatic optimization, and multi-agent coordination. |
| `dash-chatbot-app/` | A simple Dash web app that lets users chat with the deployed agent using the Databricks Apps framework. |

---

## How to Use

Follow the notebooks in **numerical order** for a smooth end-to-end experience:

1. **[000-config]** – Set up your catalog, schema, endpoint names, and runtime parameters. Configure Databricks Secrets for sensitive values.
2. **[00_data_preparation]** – Generate synthetic datasets for billing, customers, and devices with Delta optimizations.
3. **[01_create_vector_search]** – Build the FAQ dataset, create a Delta table, and generate a vector search index with timeout protection.
4. **[02_define_uc_tools]** – Define tools that expose customer data to the agent with input validation for security.
5. **[03_agent_deployment_and_evaluation]** – Build and log the model to MLflow, run agent evaluation with a synthetic evaluation dataset, register the model to Unity Catalog with lifecycle aliases, and deploy it to a serving endpoint.
6. **[04_agent_bricks_demo]** – *(Optional)* Explore Agent Bricks for declarative agent creation with automatic optimization.
7. **[`dash-chatbot-app`]** – Launch the chatbot UI to interact with your agent.

---

## Agent Bricks Integration

This solution includes a demonstration of **Agent Bricks**, Databricks' declarative framework for building production-grade AI agents with minimal code.

### What is Agent Bricks?

Agent Bricks streamlines building AI agents by offering:

| Feature | Description |
|---------|-------------|
| **Knowledge Assistant** | Create high-quality FAQ chatbots with citations using your documents |
| **Multi-Agent Supervisor** | Orchestrate multiple specialized agents for complex queries |
| **Automatic Optimization** | Databricks automatically tunes models and hyperparameters |
| **Built-in Evaluation** | Integrated quality assessment through MLflow |
| **Unity Catalog Integration** | Full governance and security compliance |

### Agent Bricks Architecture for Telco Billing

```
                    ┌─────────────────────────┐
                    │   Multi-Agent           │
                    │   Supervisor            │
                    │   (telco-billing-       │
                    │    supervisor)          │
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────┐
│  Knowledge    │      │   Billing     │      │   UC Tools    │
│  Assistant    │      │   Genie       │      │               │
│  (FAQs)       │      │   Space       │      │ - lookup_     │
│               │      │  (Analytics)  │      │   billing     │
└───────────────┘      └───────────────┘      │ - lookup_     │
                                              │   customer    │
                                              │ - lookup_     │
                                              │   plans       │
                                              └───────────────┘
```

### Quick Start with Agent Bricks

1. **Prerequisites** (Workspace must have):
   - Region: `us-east-1` or `us-west-2`
   - Mosaic AI Agent Bricks Preview enabled
   - Production monitoring for MLflow (Beta) enabled
   - Serverless compute enabled

2. **Create Knowledge Assistant**:
   - Go to **Agents** → **Knowledge Assistant** → **Build**
   - Name: `telco-billing-faq-assistant`
   - Knowledge Source: UC Files volume or Vector Search Index
   - Add the FAQ documents from `04_agent_bricks_demo`

3. **Create Multi-Agent Supervisor**:
   - Go to **Agents** → **Multi-Agent Supervisor** → **Build**
   - Name: `telco-billing-supervisor`
   - Add subagents: Knowledge Assistant + UC Functions

4. **Iterate on Quality**:
   - Use the **Examples** tab to add test questions
   - Provide natural language guidelines
   - Agent Bricks automatically optimizes

### Benefits vs Custom Agent

| Aspect | Custom Agent (notebooks 00-03) | Agent Bricks (notebook 04) |
|--------|-------------------------------|---------------------------|
| Setup Time | Hours of code | Minutes in UI |
| Optimization | Manual tuning | Automatic |
| Evaluation | Custom scripts | Built-in |
| Multi-Agent | Code LangGraph | Declarative config |
| Maintenance | Code changes | UI updates |

**Recommendation**: Start with Agent Bricks for rapid prototyping, then use the custom agent approach when you need fine-grained control.

---

## Databricks Best Practices Applied

This solution accelerator implements the following Databricks best practices:

### Security
- **Databricks Secrets**: Sensitive configuration (warehouse_id, endpoints) loaded from Databricks Secrets
- **Input Validation**: All UC functions validate inputs to prevent SQL injection
- **Dynamic Paths**: User-specific paths generated dynamically, not hardcoded

### Unity Catalog Governance
- **Organizational Naming**: Catalog names follow organizational conventions (not personal names)
- **Group-Based Permissions**: Permissions granted to groups, not individuals
- **Model Aliases**: Lifecycle management using champion/challenger/archived aliases

### Performance
- **No Spark Caching with Delta**: Removed `.cache()` calls to enable Delta optimizations
- **Liquid Clustering**: Applied to invoice table for query performance
- **Hybrid Search**: Vector search uses hybrid mode combining keyword and semantic matching
- **Timeout Protection**: Vector search index creation has timeout to prevent infinite loops

### Code Quality
- **Proper Logging**: Uses Python `logging` module instead of `print` statements
- **Error Handling**: Comprehensive try/catch with meaningful error messages
- **Documentation**: Clear docstrings and comments throughout

---

## Highlights

- **End-to-end LLM agent lifecycle**: From data to deployment.
- **Evaluation-first approach**: Includes synthetic question generation and MLflow integration for benchmarking agent performance.
- **Built-in vector search**: FAQ retrieval using vector search index with hybrid search for better accuracy.
- **Fully governed**: Unity Catalog integration for tool and model registration with proper permissions.
- **Deployable UI**: Lightweight Dash app included for real-world usage and demoing.
- **Production-ready**: Implements security, performance, and governance best practices.
- **Agent Bricks Integration**: Declarative agent creation with automatic optimization and multi-agent coordination.

<p align="center">
  <img src="./images/chatbot.jpg" alt="Billing Assistant Diagram" width="600"/>
</p>

---

## Requirements

- Databricks workspace with Unity Catalog enabled
- Access to Databricks Vector Search & Serving Endpoints
- Installed: `databricks-sdk`, `databricks-vectorsearch`, `mlflow`, `dash`, `langchain`, etc.
- Cluster or SQL Warehouse to execute notebooks
- Recommended Databricks Runtime: 15.4 ML or later

### Agent Bricks Requirements (for notebook 04)

To use Agent Bricks features, your workspace must have:

- **Region**: `us-east-1` or `us-west-2`
- **Previews Enabled**:
  - Mosaic AI Agent Bricks Preview (Beta)
  - Production monitoring for MLflow (Beta)
  - Agent Framework: On-Behalf-Of-User Authorization (for Multi-Agent Supervisor)
- **Serverless compute** enabled
- Access to foundation models in `system.ai` schema
- `databricks-gte-large-en` embedding model with AI Guardrails and rate limits disabled

### Optional Prerequisites

For production deployment with full security:

```bash
# Create Databricks secret scope
databricks secrets create-scope telco-billing

# Store sensitive configuration
databricks secrets put-secret telco-billing warehouse-id --string-value "your-warehouse-id"
databricks secrets put-secret telco-billing llm-endpoint --string-value "databricks-claude-3-7-sonnet"
databricks secrets put-secret telco-billing embedding-endpoint --string-value "databricks-gte-large-en"
```

---

## Get Started

Start with `000-config` and move through each notebook step-by-step.  
This project is designed to be modular—feel free to extend tools, customize prompts, or connect new data sources.

---

## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE.md). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 

---

## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source]. All included or referenced third party libraries are subject to the licenses set forth below.

| Library | Description | License | Source |
|---------|-------------|---------|--------|
| dbldatagen | Databricks Labs Data Generator for synthetic data | Apache 2.0 | https://github.com/databrickslabs/dbldatagen |
| langchain | Framework for developing LLM applications | MIT | https://github.com/langchain-ai/langchain |
| langgraph | Library for building stateful LLM agents | MIT | https://github.com/langchain-ai/langgraph |
| mlflow | Platform for ML lifecycle management | Apache 2.0 | https://github.com/mlflow/mlflow |
| databricks-langchain | Databricks integrations for LangChain | Apache 2.0 | https://github.com/databricks/databricks-langchain |
| databricks-vectorsearch | Databricks Vector Search client | Databricks | https://docs.databricks.com/en/generative-ai/vector-search.html |
| databricks-agents | Databricks Agent Framework | Databricks | https://docs.databricks.com/en/generative-ai/agent-framework/ |
| dash | Framework for building web applications | MIT | https://github.com/plotly/dash |
| dash-bootstrap-components | Bootstrap components for Dash | Apache 2.0 | https://github.com/facultyai/dash-bootstrap-components |
| pydantic | Data validation library | MIT | https://github.com/pydantic/pydantic |

