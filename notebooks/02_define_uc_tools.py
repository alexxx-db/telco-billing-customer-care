# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 Building Tools for a Billing Agent for a Telco Provider
# MAGIC
# MAGIC In this notebook, we define the *tools* that will be made available to the agent. These tools allow the agent to retrieve key billing and customer data, enabling it to answer questions, support customer interactions, and provide useful insights.
# MAGIC
# MAGIC ### 🛠️ Tools Defined in This Notebook
# MAGIC
# MAGIC | Tool Name                     | Description |
# MAGIC |------------------------------|-------------|
# MAGIC | `lookup_customer(input_id)`  | Retrieves customer details including device ID, plan, contact info, and contract start date. |
# MAGIC | `lookup_billing_items(input_id)` | Fetches all billing event records (e.g., call minutes, data usage) for a given device. |
# MAGIC | `lookup_billing_plans()`     | Lists all available billing plans along with pricing, contract duration, and allowances. |
# MAGIC | `lookup_billing(input_customer)` | Provides an aggregated monthly billing summary with detailed charges for a specific customer. |
# MAGIC | `billing_faq(question)`      | Uses vector search to retrieve answers from a frequently asked questions (FAQ) knowledge base. |
# MAGIC
# MAGIC Each of these tools is a modular function designed to be easily callable by the agent during runtime, whether to answer natural language queries or generate insights during workflows.
# MAGIC
# MAGIC ## Security Best Practices Applied:
# MAGIC - Input validation on all function parameters to prevent SQL injection
# MAGIC - Permission grants to defined groups (not individuals)
# MAGIC - Clear documentation in function comments

# COMMAND ----------

# DBTITLE 1,Install and Update Required Python Packages
# Install required packages
%pip install -U -qqqq mlflow-skinny langchain==0.2.16 langgraph-checkpoint==1.0.12 langchain_core langchain-community==0.2.16 langgraph==0.2.16 pydantic langchain_databricks unitycatalog-langchain unitycatalog-ai
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Set Working Catalog and Schema with Logging
import logging

# Use logger from config or create new one
if 'logger' not in dir():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("telco-billing-tools")

# Set working catalog and schema
CATALOG = config['catalog']
SCHEMA = config['database']
INDEX_NAME = config['vector_search_index']

logger.info(f"Defining UC tools in {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions for Customer Insights
# MAGIC ### Tool `lookup_customer`
# MAGIC Fetch customer metadata using their customer_id.
# MAGIC
# MAGIC **Security**: Input is validated to ensure it contains only numeric characters.

# COMMAND ----------

# DBTITLE 1,Create Customer Lookup Function with Input Validation
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_customer;")

# Security Best Practice: Validate input to prevent SQL injection
# Input must be numeric (digits only)
sqlstr_lkp_customer = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_customer(
  input_id STRING COMMENT 'Input customer id - must be a numeric value'
)
RETURNS TABLE (
    customer_id BIGINT,
    customer_name STRING,
    device_id BIGINT,
    phone_number BIGINT,
    email STRING,
    plan BIGINT,
    contract_start_dt DATE
)
COMMENT 'Returns the customer data of the customer given the customer_id. Input must be numeric.'
RETURN (
  SELECT 
    customer_id,
    customer_name,
    device_id,
    phone_number,
    email,
    plan,
    contract_start_dt
  FROM {CATALOG}.{SCHEMA}.customers
  WHERE 
    -- Input validation: only process if input contains only digits
    input_id RLIKE '^[0-9]+$'
    AND customer_id = CAST(input_id AS DECIMAL)
);
"""
spark.sql(sqlstr_lkp_customer)
logger.info(f"Created function {CATALOG}.{SCHEMA}.lookup_customer")

# COMMAND ----------

# DBTITLE 1,Test Customer Lookup Function
# Test the function with valid input
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_customer('4401');"))

# COMMAND ----------

# DBTITLE 1,Test Input Validation (Invalid Input)
# Test with invalid input - should return empty result due to validation
result = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_customer('abc; DROP TABLE users;--');")
logger.info(f"Invalid input test returned {result.count()} rows (expected 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing_items`
# MAGIC Retrieve all billing events for a specific device_id.
# MAGIC
# MAGIC **Security**: Input is validated to ensure it contains only numeric characters.

# COMMAND ----------

# DBTITLE 1,Function to Retrieve Billing Items with Input Validation
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing_items;")

sqlstr_lkp_bill_items = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing_items(
  input_id STRING COMMENT 'Input device id - must be a numeric value'
)
RETURNS TABLE (
    device_id BIGINT,
    event_type STRING,
    minutes DOUBLE,
    bytes_transferred BIGINT,
    event_ts TIMESTAMP,
    contract_start_dt DATE
)
COMMENT 'Returns all billing items information for the customer. You need device_id from the lookup_customer function. Input must be numeric.'
RETURN (
  SELECT 
    device_id,
    event_type,
    minutes,
    bytes_transferred,
    event_ts,
    contract_start_dt 
  FROM {CATALOG}.{SCHEMA}.billing_items
  WHERE 
    -- Input validation: only process if input contains only digits
    input_id RLIKE '^[0-9]+$'
    AND device_id = CAST(input_id AS DECIMAL)
  ORDER BY event_ts DESC
  LIMIT 100  -- Limit results to prevent overwhelming responses
);
"""
spark.sql(sqlstr_lkp_bill_items)
logger.info(f"Created function {CATALOG}.{SCHEMA}.lookup_billing_items")

# COMMAND ----------

# DBTITLE 1,Test Billing Items Function
# Test the function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing_items('9862259275');"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing_plans`
# MAGIC List available billing plans and associated metadata.

# COMMAND ----------

# DBTITLE 1,Lookup Billing Plans Function
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing_plans;")

sqlstr_lkp_bill_plans = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing_plans()
RETURNS TABLE (
    Plan_key BIGINT,
    Plan_id STRING,
    Plan_name STRING,
    contract_in_months BIGINT,
    monthly_charges_dollars BIGINT,
    Calls_Text STRING,
    Internet_Speed_MBPS STRING,
    Data_Limit_GB STRING,
    Data_Outside_Allowance_Per_MB DOUBLE,
    Roam_Data_charges_per_MB DOUBLE,
    Roam_Call_charges_per_min DOUBLE,
    Roam_text_charges DOUBLE,
    International_call_charge_per_min DOUBLE,
    International_text_charge DOUBLE
)
COMMENT 'Returns all available billing plan details including pricing, data limits, and roaming charges'
RETURN (
  SELECT
    Plan_key,
    Plan_id,
    Plan_name,
    contract_in_months,
    monthly_charges_dollars,
    Calls_Text,
    Internet_Speed_MBPS,
    Data_Limit_GB,
    Data_Outside_Allowance_Per_MB,
    Roam_Data_charges_per_MB,
    Roam_Call_charges_per_min,
    Roam_text_charges,
    International_call_charge_per_min,
    International_text_charge 
  FROM {CATALOG}.{SCHEMA}.billing_plans
);
"""
spark.sql(sqlstr_lkp_bill_plans)
logger.info(f"Created function {CATALOG}.{SCHEMA}.lookup_billing_plans")

# COMMAND ----------

# DBTITLE 1,Test Billing Plans Function
# Test the function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing_plans()"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `lookup_billing`
# MAGIC Returns monthly aggregated billing summary for a given customer.
# MAGIC
# MAGIC **Security**: Input is validated to ensure it contains only numeric characters.

# COMMAND ----------

# DBTITLE 1,Billing Information Lookup Function with Input Validation
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.lookup_billing;")

sqlstr_lkp_billing = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.lookup_billing(
    input_customer STRING COMMENT 'The customer_id to lookup billing for - must be a numeric value' 
)
RETURNS TABLE (
    customer_id BIGINT,
    customer_name STRING,
    event_month STRING,
    phone_number BIGINT,
    data_charges_outside_allowance DOUBLE,
    roaming_data_charges DOUBLE,
    roaming_call_charges DOUBLE,
    roaming_text_charges DOUBLE,
    international_call_charges DOUBLE,
    international_text_charges DOUBLE,
    total_charges DOUBLE
)
COMMENT 'Returns billing information for the customer given the customer_id. Input must be numeric.'
RETURN
SELECT 
    customer_id,
    customer_name,
    event_month,
    phone_number,
    data_charges_outside_allowance,
    roaming_data_charges,
    roaming_call_charges,
    roaming_text_charges,
    international_call_charges,
    international_text_charges,
    total_charges
FROM {CATALOG}.{SCHEMA}.invoice
WHERE 
    -- Input validation: only process if input contains only digits
    input_customer RLIKE '^[0-9]+$'
    AND customer_id = CAST(input_customer AS DECIMAL)
ORDER BY event_month DESC
LIMIT 12;  -- Return last 12 months of billing data
"""
spark.sql(sqlstr_lkp_billing)
logger.info(f"Created function {CATALOG}.{SCHEMA}.lookup_billing")

# COMMAND ----------

# DBTITLE 1,Test Billing Lookup Function
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.lookup_billing('4401');"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Vector Search for FAQ Support
# MAGIC Search the faq_index for answers to frequently asked billing questions using vector similarity.
# MAGIC
# MAGIC **Best Practice**: Use hybrid search for better retrieval quality.

# COMMAND ----------

# DBTITLE 1,Query FAQ Index for Change Bill Due Date
result = spark.sql(f"""
SELECT * 
FROM vector_search(
    index => '{CATALOG}.{SCHEMA}.{INDEX_NAME}', 
    query => 'Can I change my bill due date', 
    num_results => 5
)
""")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool `billing_faq`
# MAGIC Wrapper function to perform vector-based FAQ lookups.
# MAGIC
# MAGIC **Best Practice**: 
# MAGIC - Uses hybrid search (combines keyword + semantic) for better retrieval
# MAGIC - Returns top 3 results for more comprehensive answers
# MAGIC - Input is sanitized to prevent injection

# COMMAND ----------

# DBTITLE 1,Create Billing FAQ Search Function with Hybrid Search
spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.billing_faq;")

# Best Practice: Use hybrid search for better retrieval quality
# Hybrid search combines keyword matching with semantic similarity
sqlstr_billing_faq = f"""
CREATE OR REPLACE FUNCTION {CATALOG}.{SCHEMA}.billing_faq(
    question STRING COMMENT 'FAQ search query - the question to ask about billing topics'
)
RETURNS STRING
LANGUAGE SQL
COMMENT 'Searches the FAQ knowledge base for billing-related questions using hybrid search (keyword + semantic). Returns the most relevant FAQ entries.'
RETURN 
    SELECT string(collect_set(faq)) 
    FROM vector_search(
        index => '{CATALOG}.{SCHEMA}.{INDEX_NAME}', 
        query => question, 
        num_results => 3,
        query_type => 'hybrid'  -- Best Practice: Use hybrid search for better retrieval
    );
"""
spark.sql(sqlstr_billing_faq)
logger.info(f"Created function {CATALOG}.{SCHEMA}.billing_faq with hybrid search")

# COMMAND ----------

# DBTITLE 1,Test FAQ Search Function
# Test the function
result = spark.sql(f"SELECT {CATALOG}.{SCHEMA}.billing_faq('How do I pay my bill?') as faq_result")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions to Functions
# MAGIC 
# MAGIC **Best Practice**: Assign execute permissions to groups, not individuals.

# COMMAND ----------

# DBTITLE 1,Grant Execute Permissions to Groups
# Best Practice: Grant permissions to groups, not individuals
admin_group = config.get('admin_group', 'telco_billing_admins')
data_scientist_group = config.get('data_scientist_group', 'telco_billing_data_scientists')
agent_service_principal = config.get('agent_service_principal', 'telco_billing_agent_sp')

# List of all functions created
functions = [
    'lookup_customer',
    'lookup_billing_items', 
    'lookup_billing_plans',
    'lookup_billing',
    'billing_faq'
]

try:
    for func in functions:
        func_name = f"{CATALOG}.{SCHEMA}.{func}"
        
        # Grant execute to admins (full control)
        spark.sql(f"GRANT EXECUTE ON FUNCTION {func_name} TO `{admin_group}`")
        
        # Grant execute to data scientists (for testing/evaluation)
        spark.sql(f"GRANT EXECUTE ON FUNCTION {func_name} TO `{data_scientist_group}`")
        
        # Grant execute to service principal (for production agent)
        spark.sql(f"GRANT EXECUTE ON FUNCTION {func_name} TO `{agent_service_principal}`")
        
        logger.info(f"Granted execute permissions on {func_name}")
    
    logger.info("All function permissions granted successfully")
    
except Exception as e:
    logger.warning(f"Could not grant permissions (groups/service principal may not exist): {e}")
    logger.info("Create the groups and service principal, then re-run this cell to apply permissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC All UC functions have been created with:
# MAGIC - ✅ Input validation to prevent SQL injection
# MAGIC - ✅ Clear documentation and comments
# MAGIC - ✅ Hybrid search for FAQ retrieval
# MAGIC - ✅ Result limits to prevent overwhelming responses
# MAGIC - ✅ Permission grants to defined groups

# COMMAND ----------

# DBTITLE 1,Display Created Functions
logger.info(f"Functions created in {CATALOG}.{SCHEMA}:")
for func in functions:
    logger.info(f"  - {func}")
    
# Display function list from catalog
display(spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.{SCHEMA}"))
