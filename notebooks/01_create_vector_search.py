# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Create and Test Vector Search Index from Billing FAQ Data
# MAGIC
# MAGIC This notebook walks through the end-to-end process of preparing a **vector search index** based on a synthetic FAQ dataset related to telecom billing.
# MAGIC
# MAGIC We use Databricks Vector Search to enable high-accuracy semantic retrieval of FAQ entries, forming the foundation for downstream use cases such as retrieval-augmented generation (RAG) and agent-based question answering.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📌 Key Steps in This Notebook
# MAGIC
# MAGIC 1. **Generate a Billing FAQ Dataset**  
# MAGIC    Create a synthetic dataset of frequently asked billing questions and answers, and save it as a Delta table in Unity Catalog.
# MAGIC
# MAGIC 2. **Configure & Create Vector Search Index**  
# MAGIC    - Enable change data feed on the Delta table  
# MAGIC    - Create a delta sync index using a specified embedding model  
# MAGIC    - Wait for the index to become ready (with timeout protection)
# MAGIC
# MAGIC 3. **Test the Vector Search**  
# MAGIC    Run a similarity search using a sample query (`"Can I change my bill due date?"`) and return the top matching FAQ entries.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧰 Requirements
# MAGIC
# MAGIC - Unity Catalog enabled and configured
# MAGIC - Databricks Vector Search endpoint (configured and shared)
# MAGIC - A valid embedding model endpoint (e.g., `databricks-gte-large-en`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Best Practices Applied
# MAGIC - Timeout protection to prevent infinite loops
# MAGIC - Proper logging instead of print statements
# MAGIC - Error handling with meaningful messages
# MAGIC - Hybrid search for better retrieval quality

# COMMAND ----------

# DBTITLE 1,Update Databricks SDK and Vector Search Library
# MAGIC %pip install -U --quiet databricks-sdk==0.28.0 databricks-vectorsearch 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Setup Vector Search Index Configuration with Logging
import logging
import time

# Use logger from config or create new one
if 'logger' not in dir():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("telco-billing-vector-search")

CATALOG = config['catalog']
SCHEMA = config['database']
VS_INDX = config['vector_search_index']

VECTOR_SEARCH_ENDPOINT_NAME = config['VECTOR_SEARCH_ENDPOINT_NAME']
embedding_model_endpoint_name = config['embedding_model_endpoint_name']
vs_index_fullname = f"{CATALOG}.{SCHEMA}.{VS_INDX}"

logger.info(f"Vector Search Configuration:")
logger.info(f"  Endpoint: {VECTOR_SEARCH_ENDPOINT_NAME}")
logger.info(f"  Index: {vs_index_fullname}")
logger.info(f"  Embedding Model: {embedding_model_endpoint_name}")

# COMMAND ----------

# DBTITLE 1,Create Billing FAQ Dataset and Save as Delta Table
import pandas as pd
from pyspark.sql import SparkSession

# Create a Pandas DataFrame from the FAQ data
# NOTE: In production, this should be expanded with more comprehensive FAQs
faq_data = [
    (1, "Q: How is my bill calculated? A: Your bill includes your monthly plan fee, additional charges for extra services, taxes, and any applicable discounts. A detailed breakdown is available in your MyTelco account."),
    (2, "Q: Why is my bill higher than usual? A: Your bill may be higher due to extra data usage, international calls, roaming charges, or a recent plan change. Check the usage details in the MyTelco app."),
    (3, "Q: What happens if I don't pay my bill on time? A: Late payments may incur a penalty and could lead to service suspension. Reconnection fees may apply. Set up autopay to avoid these issues."),
    (4, "Q: How can I set up autopay for my bill? A: Enable autopay by logging into your MyTelco account, navigating to the billing section, and selecting 'Set Up Autopay'."),
    (5, "Q: How do I dispute a charge on my bill? A: If you believe there's an incorrect charge, contact customer support within 30 days of receiving your bill. Provide supporting details for review."),
    (6, "Q: Can I get a refund for overcharges? A: Refunds for overcharges may be available depending on the case. Contact customer support for a review, and any approved refunds will be credited to your next bill."),
    (7, "Q: Why was I charged a late payment fee? A: Late fees apply when payments are made after the due date. Check your bill for the due date and ensure timely payments to avoid these charges."),
    (8, "Q: How do I check my data usage? A: You can check your real-time data usage in the MyTelco app or by logging into your account online."),
    (9, "Q: Why do I see a roaming charge on my bill? A: Roaming charges apply when using your phone outside your network's coverage. Check your plan settings or enable roaming notifications to avoid unexpected charges."),
    (10, "Q: Can I change my bill due date? A: Yes, you can request a bill due date change by contacting customer support or modifying it in your account settings."),
    # Additional FAQs for better coverage
    (11, "Q: How do I view my bill online? A: Log into your MyTelco account and navigate to the Billing section. You can view current and past bills, download PDF statements, and see payment history."),
    (12, "Q: What payment methods do you accept? A: We accept credit cards, debit cards, bank transfers, and digital wallets. You can also pay at authorized retail locations."),
    (13, "Q: How do I update my payment method? A: Go to your MyTelco account settings, select Payment Methods, and add or update your preferred payment option."),
    (14, "Q: What is a prorated charge? A: Prorated charges occur when you change your plan mid-billing cycle. You're charged proportionally for the days you used each plan."),
    (15, "Q: How do I cancel my service? A: To cancel, contact customer support or visit a retail location. Note that early termination fees may apply if you're under contract.")
]

# Convert to Pandas then Spark DataFrame
pdf = pd.DataFrame(faq_data, columns=["index", "faq"])
spark_df = SparkSession.builder.getOrCreate().createDataFrame(pdf)

# Save as a Delta table with optimizations
spark_df.write.format("delta") \
    .mode("overwrite") \
    .option("delta.enableOptimizeWrite", "true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA}.billing_faq_dataset")

logger.info(f"Created FAQ dataset with {len(faq_data)} entries in {CATALOG}.{SCHEMA}.billing_faq_dataset")

# COMMAND ----------

# DBTITLE 1,Display FAQ Dataset
sql_query = f"""
SELECT * FROM `{CATALOG}`.`{SCHEMA}`.`billing_faq_dataset`
"""
display(spark.sql(sql_query))

# COMMAND ----------

# DBTITLE 1,Create Vector Search Endpoint with Databricks Client
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk.errors import ResourceAlreadyExists, AlreadyExists, ResourceConflict

client = VectorSearchClient()

try:
    logger.info(f"Creating vector search endpoint: {VECTOR_SEARCH_ENDPOINT_NAME}")
    client.create_endpoint_and_wait(
        name=VECTOR_SEARCH_ENDPOINT_NAME,
        endpoint_type="STANDARD"
    )
    logger.info(f"Endpoint {VECTOR_SEARCH_ENDPOINT_NAME} created successfully")
except Exception as e:
    msg = str(e)
    if '"error_code":"ALREADY_EXISTS"' in msg or "status_code 409" in msg:
        logger.info(f"Endpoint {VECTOR_SEARCH_ENDPOINT_NAME} already exists. Continuing...")
    else:
        logger.error(f"Failed to create endpoint: {e}")
        raise

# COMMAND ----------

# DBTITLE 1,Initialize Vector Search Client
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c
import time

# Initialize the Vector Search Client with the option to disable the notice.
vsc = VectorSearchClient(disable_notice=True)

# COMMAND ----------

# DBTITLE 1,Create and Sync Vector Search Index with Timeout Protection
# Enable change data feed for Delta sync
sql_query = f"""
ALTER TABLE `{CATALOG}`.`{SCHEMA}`.`billing_faq_dataset` 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""
spark.sql(sql_query)
logger.info("Enabled Change Data Feed on FAQ dataset")

vs_index = vs_index_fullname
source_table = f"{CATALOG}.{SCHEMA}.billing_faq_dataset"

primary_key = "index"
embedding_source_column = "faq"

# Best Practice: Define timeout to prevent infinite loops
INDEX_CREATION_TIMEOUT_SECONDS = 600  # 10 minutes
POLL_INTERVAL_SECONDS = 10

logger.info(f"Creating index {vs_index} on endpoint {VECTOR_SEARCH_ENDPOINT_NAME}...")

try:
    index = vsc.create_delta_sync_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT_NAME,
        index_name=vs_index,
        source_table_name=source_table,
        pipeline_type="TRIGGERED",
        primary_key=primary_key,
        embedding_source_column=embedding_source_column,
        embedding_model_endpoint_name=embedding_model_endpoint_name
    )
    
    # Best Practice: Wait with timeout protection
    start_time = time.time()
    while not index.describe().get('status', {}).get('detailed_state', '').startswith('ONLINE'):
        elapsed = time.time() - start_time
        
        # Check timeout
        if elapsed > INDEX_CREATION_TIMEOUT_SECONDS:
            raise TimeoutError(
                f"Index creation timed out after {INDEX_CREATION_TIMEOUT_SECONDS} seconds. "
                f"Current status: {index.describe().get('status')}. "
                "The index may still be building. Check the Databricks UI for status."
            )
        
        status = index.describe().get('status', {})
        detailed_state = status.get('detailed_state', 'UNKNOWN')
        logger.info(f"Waiting for index to be ONLINE... Current state: {detailed_state} (elapsed: {int(elapsed)}s)")
        time.sleep(POLL_INTERVAL_SECONDS)
    
    logger.info(f"Index {vs_index} on table {source_table} is ready!")
    
except TimeoutError as e:
    logger.warning(str(e))
    logger.info("You can check the index status in the Databricks UI and continue when ready.")
    
except Exception as e:
    msg = str(e)
    if '"error_code":"RESOURCE_ALREADY_EXISTS"' in msg or "status_code 409" in msg:
        logger.info(f"Index {vs_index} already exists. Continuing...")
    else:
        logger.error(f"Failed to create index: {e}")
        raise

# COMMAND ----------

# DBTITLE 1,Verify Index Status
# Check current index status
try:
    index_info = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname)
    status = index_info.describe()
    logger.info(f"Index Status: {status.get('status', {}).get('detailed_state', 'UNKNOWN')}")
    logger.info(f"Index Name: {status.get('name')}")
    logger.info(f"Primary Key: {status.get('primary_key')}")
except Exception as e:
    logger.warning(f"Could not get index status: {e}")

# COMMAND ----------

# DBTITLE 1,Test Similarity Search with Hybrid Mode
# Best Practice: Use hybrid search combining keyword and semantic matching
query_text = "Can I change my bill due date"

logger.info(f"Testing similarity search with query: '{query_text}'")

try:
    results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
        query_text=query_text,
        columns=['index', 'faq'],
        query_type='hybrid',  # Best Practice: Use hybrid search for better retrieval
        num_results=5
    )
    
    logger.info(f"Found {len(results.get('result', {}).get('data_array', []))} matching results")
    display(results)
    
except Exception as e:
    logger.error(f"Similarity search failed: {e}")
    logger.info("The index may still be building. Wait for it to come online and retry.")

# COMMAND ----------

# DBTITLE 1,Test Additional Queries
# Test a few more queries to verify search quality
test_queries = [
    "How do I pay my bill?",
    "What are roaming charges?",
    "How to set up automatic payments?"
]

logger.info("Testing additional queries for search quality verification:")

for query in test_queries:
    try:
        results = vsc.get_index(VECTOR_SEARCH_ENDPOINT_NAME, vs_index_fullname).similarity_search(
            query_text=query,
            columns=['index', 'faq'],
            query_type='hybrid',
            num_results=1
        )
        
        data = results.get('result', {}).get('data_array', [])
        if data:
            score = data[0][0] if len(data[0]) > 0 else 'N/A'
            faq = data[0][2] if len(data[0]) > 2 else 'N/A'
            logger.info(f"Query: '{query}'")
            logger.info(f"  Top result (score: {score}): {faq[:100]}...")
        else:
            logger.warning(f"No results for query: '{query}'")
            
    except Exception as e:
        logger.error(f"Query failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC Vector Search index has been created with:
# MAGIC - ✅ FAQ dataset with 15 billing-related Q&As
# MAGIC - ✅ Delta sync index for automatic updates
# MAGIC - ✅ Hybrid search enabled for better retrieval quality
# MAGIC - ✅ Timeout protection during index creation
# MAGIC - ✅ Comprehensive logging for monitoring
