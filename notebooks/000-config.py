# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration Parameters
# MAGIC 
# MAGIC This notebook centralizes all configuration for the Telco Billing Customer Care solution.
# MAGIC 
# MAGIC ## Security Best Practices Applied:
# MAGIC - Sensitive values (warehouse_id, endpoints) retrieved from Databricks Secrets
# MAGIC - Dynamic user paths instead of hardcoded personal paths
# MAGIC - Environment-aware configuration (dev/staging/prod)
# MAGIC 
# MAGIC ## Prerequisites:
# MAGIC Create a secret scope and secrets before running:
# MAGIC ```
# MAGIC databricks secrets create-scope telco-billing
# MAGIC databricks secrets put-secret telco-billing warehouse-id --string-value "your-warehouse-id"
# MAGIC databricks secrets put-secret telco-billing llm-endpoint --string-value "databricks-claude-3-7-sonnet"
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Initialize Config Dictionary and Environment
import os
import logging

# Configure logging (Databricks best practice: use logging instead of print)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("telco-billing-config")

if 'config' not in locals():
    config = {}

# Environment configuration (set via cluster environment variable or default to 'dev')
# Options: 'dev', 'staging', 'prod'
config['environment'] = os.getenv('TELCO_BILLING_ENV', 'dev')
logger.info(f"Initializing configuration for environment: {config['environment']}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data Catalog configs
# MAGIC 
# MAGIC **Best Practice**: Use organizational naming conventions, not personal names.
# MAGIC Format: `{org}_{project}_{environment}` or `{project}_{environment}`

# COMMAND ----------

# DBTITLE 1,Set Catalog and Database Based on Environment
# Catalog and database - using organizational naming convention
# Best Practice: Assign ownership to groups, not individuals

_catalog_map = {
    'dev': 'telco_billing_dev',
    'staging': 'telco_billing_staging',
    'prod': 'telco_billing_prod'
}

config['catalog'] = _catalog_map.get(config['environment'], 'telco_billing_dev')
config['database'] = 'billing_customer_care'

logger.info(f"Using catalog: {config['catalog']}, database: {config['database']}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Security Configuration
# MAGIC 
# MAGIC **Best Practice**: Store sensitive values in Databricks Secrets, not in code.

# COMMAND ----------

# DBTITLE 1,Load Sensitive Configuration from Secrets
# Security Best Practice: Load sensitive values from Databricks Secrets
# Falls back to environment variables for local development, then to defaults for demo

def get_secret_or_default(scope, key, default_value, env_var=None):
    """
    Retrieve secret from Databricks Secrets, environment variable, or default.
    Priority: Secrets > Environment Variable > Default
    """
    try:
        # Try Databricks Secrets first
        return dbutils.secrets.get(scope=scope, key=key)
    except Exception:
        pass
    
    # Try environment variable
    if env_var and os.getenv(env_var):
        return os.getenv(env_var)
    
    # Fall back to default (for demo/dev environments)
    logger.warning(f"Using default value for {key}. Set up Databricks Secrets for production.")
    return default_value

# Secret scope name
SECRET_SCOPE = "telco-billing"

# Load sensitive configuration
config['warehouse_id'] = get_secret_or_default(
    SECRET_SCOPE, 'warehouse-id', 
    '148ccb90800933a1',  # Default for demo
    'DATABRICKS_WAREHOUSE_ID'
)

config['llm_endpoint'] = get_secret_or_default(
    SECRET_SCOPE, 'llm-endpoint',
    'databricks-claude-3-7-sonnet',
    'DATABRICKS_LLM_ENDPOINT'
)

config['embedding_model_endpoint_name'] = get_secret_or_default(
    SECRET_SCOPE, 'embedding-endpoint',
    'databricks-gte-large-en',
    'DATABRICKS_EMBEDDING_ENDPOINT'
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Dynamic Path Configuration
# MAGIC 
# MAGIC **Best Practice**: Use dynamic paths based on current user, not hardcoded personal paths.

# COMMAND ----------

# DBTITLE 1,Configure Dynamic Paths
# Get current user for dynamic path generation
try:
    current_user = spark.sql("SELECT current_user()").first()[0]
except Exception:
    current_user = os.getenv('USER', 'default_user')

# Dynamic data paths (not hardcoded to specific users)
config['user_data_path'] = f"dbfs:/Users/{current_user}/telco-billing/data/"
config['checkpoint_path'] = f"dbfs:/Users/{current_user}/telco-billing/checkpoints/"
config['current_user'] = current_user

logger.info(f"Data path configured for user: {current_user}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Key configurations for the data generation 

# COMMAND ----------

# DBTITLE 1,Set Data Generation Configuration Parameters
# Data Generation Configs
config['UNIQUE_CUSTOMERS'] = 50000
config['CUSTOMER_MIN_VALUE'] = 1000
config['DEVICE_MIN_VALUE'] = 1000000000
config['SUBSCRIBER_NUM_MIN_VALUE'] = 1000000000
config['UNIQUE_PLANS'] = 10  # Number of unique plans in the billing plan dataset
config['PLAN_MIN_VALUE'] = 1

config['AVG_EVENTS_PER_CUSTOMER'] = 10

# Spark configuration
config['shuffle_partitions_requested'] = 8
config['partitions_requested'] = 8

# Date range for data generation
config['NUM_DAYS'] = 365
config['start_dt'] = "2024-01-01 00:00:00"
config['end_dt'] = "2024-12-31 11:59:59"

# Data size limits (in bytes)
config['MB_100'] = 50000000  # Max bytes transferred
config['K_1'] = 100000       # Min bytes transferred

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agent configuration parameters

# COMMAND ----------

# DBTITLE 1,Set Agent Configuration Parameters
# Agent Configs
config['agent_name'] = 'ai_billing_agent'
config['VECTOR_SEARCH_ENDPOINT_NAME'] = 'vector-search-telco-billing'
config['vector_search_index'] = 'faq_indx1'

# LLM Configuration with timeouts (Best Practice: add timeouts to prevent hanging)
config['llm_timeout_seconds'] = 60
config['llm_max_tokens'] = 1024  # Increased from 128 for detailed responses

# Tools - using fully qualified names
config['tools_billing_faq'] = f"{config['catalog']}.{config['database']}.billing_faq"
config['tools_billing'] = f"{config['catalog']}.{config['database']}.lookup_billing"
config['tools_items'] = f"{config['catalog']}.{config['database']}.lookup_billing_items"
config['tools_plans'] = f"{config['catalog']}.{config['database']}.lookup_billing_plans"
config['tools_customer'] = f"{config['catalog']}.{config['database']}.lookup_customer"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Permission Groups Configuration
# MAGIC 
# MAGIC **Best Practice**: Define groups for access control, assign ownership to groups.

# COMMAND ----------

# DBTITLE 1,Define Permission Groups
# Permission groups for governance (Best Practice: assign to groups, not individuals)
config['admin_group'] = 'telco_billing_admins'
config['data_engineer_group'] = 'telco_billing_data_engineers'
config['data_scientist_group'] = 'telco_billing_data_scientists'
config['agent_service_principal'] = 'telco_billing_agent_sp'

# Model lifecycle aliases (Best Practice: use aliases instead of stages)
config['model_alias_champion'] = 'champion'
config['model_alias_challenger'] = 'challenger'
config['model_alias_archived'] = 'archived'

# COMMAND ----------

# DBTITLE 1,Validate Configuration
# Validate required configuration
required_configs = ['catalog', 'database', 'warehouse_id', 'llm_endpoint']
missing = [key for key in required_configs if not config.get(key)]
if missing:
    raise ValueError(f"Missing required configuration: {missing}")

logger.info("Configuration validation completed successfully")
logger.info(f"Catalog: {config['catalog']}, Database: {config['database']}")
logger.info(f"Environment: {config['environment']}")
