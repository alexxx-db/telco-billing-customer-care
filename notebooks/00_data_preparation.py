# Databricks notebook source
# MAGIC %run "./000-config"

# COMMAND ----------

# MAGIC %md ##Data Generation for the Demo
# MAGIC
# MAGIC For this solution accelerator, we will use data generator. The datasets include:
# MAGIC
# MAGIC - billing_plans: Telcos provide multiple billing plans to their consumers. These are representative plans for this solution accelarator.
# MAGIC - customers: Telco customer master data including attributes like customer_id, name, phone number, email etc
# MAGIC - billing_items: The detailed billing items including device_id, event type (data, call or text), call duration, data transferred, event timestamp etc
# MAGIC - invoice: Telco billing invoice for the customer
# MAGIC   
# MAGIC The invoice is based on:
# MAGIC   - cost per MB of internet activity
# MAGIC   - cost per minute of call for each of the call categories
# MAGIC   - cost per message
# MAGIC   
# MAGIC Internet activitity will be priced per MB transferred
# MAGIC
# MAGIC Phone calls will be priced per minute or partial minute.
# MAGIC
# MAGIC Messages will be priced per actual counts
# MAGIC
# MAGIC For simplicity, we'll ignore the free data, messages and calls threshold in most plans and the complexity
# MAGIC of matching devices to customers and telecoms operators - our goal here is to show generation of join
# MAGIC ready data, rather than full modelling of phone usage invoicing.
# MAGIC
# MAGIC ## Best Practices Applied:
# MAGIC - Dynamic paths instead of hardcoded user paths
# MAGIC - No Spark caching with Delta (Delta handles optimization internally)
# MAGIC - Liquid clustering for frequently queried columns
# MAGIC - Enable optimize write for automatic file compaction
# MAGIC - Proper logging instead of print statements

# COMMAND ----------

# MAGIC %md ## Databricks Labs Data Generator 
# MAGIC  Generating synthetic data is complex, however we have leveraged [Databricks Labs Data Generator ](https://github.com/databrickslabs/dbldatagen/tree/master)for the data generation. In fact, the labs project had a good [example](https://github.com/databrickslabs/dbldatagen/blob/master/dbldatagen/datasets/multi_table_telephony_provider.py) for telco which is reused for this solution. We highly recommend to use the labs project for synthetic data generation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install data generator package

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a catalog and a database
# MAGIC We create a catalog and a database (schema) to store the delta tables for our data.

# COMMAND ----------

# DBTITLE 1,Initialize logging and configuration
# Note: Config already loaded via %run at top of notebook
import logging

# Use logger from config or create new one
if 'logger' not in dir():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("telco-billing-data-prep")

catalog = config['catalog']
db = config['database']

logger.info(f"Starting data preparation for {catalog}.{db}")

# Ensure the schema exists within the specified catalog
_ = spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")
logger.info(f"Schema {catalog}.{db} ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configurations for data generation

# COMMAND ----------

# DBTITLE 1,Load configuration variables
UNIQUE_CUSTOMERS = config['UNIQUE_CUSTOMERS']
CUSTOMER_MIN_VALUE = config['CUSTOMER_MIN_VALUE']
DEVICE_MIN_VALUE = config['DEVICE_MIN_VALUE']
SUBSCRIBER_NUM_MIN_VALUE = config['SUBSCRIBER_NUM_MIN_VALUE']
UNIQUE_PLANS = config['UNIQUE_PLANS']
PLAN_MIN_VALUE = config['PLAN_MIN_VALUE']

AVG_EVENTS_PER_CUSTOMER = config['AVG_EVENTS_PER_CUSTOMER']

shuffle_partitions_requested = config['shuffle_partitions_requested']
partitions_requested = config['partitions_requested']
NUM_DAYS = config['NUM_DAYS'] 
MB_100 = config['MB_100']
K_1 = config['K_1']
start_dt = config['start_dt']
end_dt = config['end_dt']

llm_endpoint = config['llm_endpoint']
warehouse_id = config['warehouse_id']

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create billing plans table 
# MAGIC Billing plans represent various subscription options offered by the telecom provider to their customers. This dataset specifically focuses on plans available under contractual agreements. Due to its small size, the sample dataset has been uploaded to Git and imported into a Delta table.

# COMMAND ----------

# DBTITLE 1,Create billing plans data using dynamic paths
# Best Practice: Use dynamic paths from config instead of hardcoded personal paths
data_path = config['user_data_path']
dbutils.fs.mkdirs(data_path)

# Define the destination path dynamically
billing_plans_path = f"{data_path}billing_plans.json"
logger.info(f"Writing billing plans to: {billing_plans_path}")

# Define the content as a single string
json_data = """{"Plan_key":1,"Plan_id":"PLAN001","Plan_name":"50GB SIM12","contract_in_months":12,"monthly_charges_dollars":25,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"50","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":2,"Plan_id":"PLAN002","Plan_name":"50GB SIM24","contract_in_months":24,"monthly_charges_dollars":22,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"50","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":3,"Plan_id":"PLAN003","Plan_name":"100GB SIM12","contract_in_months":12,"monthly_charges_dollars":28,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"100","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":4,"Plan_id":"PLAN004","Plan_name":"100GB SIM24","contract_in_months":24,"monthly_charges_dollars":26,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"100","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":5,"Plan_id":"PLAN005","Plan_name":"150GB SIM12","contract_in_months":12,"monthly_charges_dollars":30,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"150","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":6,"Plan_id":"PLAN006","Plan_name":"150GB SIM24","contract_in_months":24,"monthly_charges_dollars":27,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"100","Data_Limit_GB":"150","Data_Outside_Allowance_Per_MB":0.01,"Roam_Data_charges_per_MB":0.1,"Roam_Call_charges_per_min":1.0,"Roam_text_charges":0.6,"International_call_charge_per_min":0.7,"International_text_charge":0.5}
{"Plan_key":7,"Plan_id":"PLAN007","Plan_name":"UNLIMITED SIM12","contract_in_months":12,"monthly_charges_dollars":35,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"UNLIMITED","Data_Limit_GB":"UNLIMITED","Data_Outside_Allowance_Per_MB":0.0,"Roam_Data_charges_per_MB":0.08,"Roam_Call_charges_per_min":0.8,"Roam_text_charges":0.5,"International_call_charge_per_min":0.6,"International_text_charge":0.4}
{"Plan_key":8,"Plan_id":"PLAN008","Plan_name":"UNLIMITED SIM24","contract_in_months":24,"monthly_charges_dollars":32,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"UNLIMITED","Data_Limit_GB":"UNLIMITED","Data_Outside_Allowance_Per_MB":0.0,"Roam_Data_charges_per_MB":0.08,"Roam_Call_charges_per_min":0.8,"Roam_text_charges":0.5,"International_call_charge_per_min":0.6,"International_text_charge":0.4}
{"Plan_key":9,"Plan_id":"PLAN009","Plan_name":"UNLIMITED WORLD SIM12","contract_in_months":12,"monthly_charges_dollars":42,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"UNLIMITED","Data_Limit_GB":"UNLIMITED","Data_Outside_Allowance_Per_MB":0.0,"Roam_Data_charges_per_MB":0.0,"Roam_Call_charges_per_min":0.0,"Roam_text_charges":0.0,"International_call_charge_per_min":0.0,"International_text_charge":0.0}
{"Plan_key":10,"Plan_id":"PLAN010","Plan_name":"UNLIMITED WORLD SIM24","contract_in_months":24,"monthly_charges_dollars":38,"Calls_Text":"UNLIMITED","Internet_Speed_MBPS":"UNLIMITED","Data_Limit_GB":"UNLIMITED","Data_Outside_Allowance_Per_MB":0.0,"Roam_Data_charges_per_MB":0.0,"Roam_Call_charges_per_min":0.0,"Roam_text_charges":0.0,"International_call_charge_per_min":0.0,"International_text_charge":0.0}"""

# Write to DBFS (True allows overwriting if the file already exists)
dbutils.fs.put(billing_plans_path, json_data, True)

# COMMAND ----------

# DBTITLE 1,Load billing plans with schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DecimalType

# Define the schema for the JSON data
schema = StructType([
    StructField("Plan_key", IntegerType(), True),
    StructField("Plan_id", StringType(), True),
    StructField("Plan_name", StringType(), True),
    StructField("contract_in_months", IntegerType(), True),
    StructField("monthly_charges_dollars", DoubleType(), True),
    StructField("Calls_Text", StringType(), True),
    StructField("Internet_Speed_MBPS", StringType(), True),
    StructField("Data_Limit_GB", StringType(), True),
    StructField("Data_Outside_Allowance_Per_MB", DoubleType(), True),
    StructField("Roam_Data_charges_per_MB", DoubleType(), True),
    StructField("Roam_Call_charges_per_min", DoubleType(), True),
    StructField("Roam_text_charges", DoubleType(), True),
    StructField("International_call_charge_per_min", DoubleType(), True),
    StructField("International_text_charge", DoubleType(), True)
])

# Load from dynamic path
df_plans = spark.read.format("json").schema(schema).load(billing_plans_path)

# COMMAND ----------

# DBTITLE 1,Write billing plans to Delta with optimizations
# Write the DataFrame to a Delta table with Delta optimizations
df_plans.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableOptimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{db}.billing_plans")

logger.info(f"Created table {catalog}.{db}.billing_plans")

# COMMAND ----------

display(df_plans)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Generate customer, billing items and invoice datasets 
# MAGIC Customer, Billing items and Invoice tables are generated using data generator and loaded in the delta tables

# COMMAND ----------

# MAGIC %md ###Lets model our customers
# MAGIC
# MAGIC Device is used as the the foreign key. For more details around the data generation, please refer Databricks Labs project.
# MAGIC
# MAGIC Note: dbldatagen was already installed at the start of this notebook.

# COMMAND ----------

# DBTITLE 1,Generate customer data
import dbldatagen as dg
import pyspark.sql.functions as F

shuffle_partitions_requested = 8
partitions_requested = 8

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

# Clear cache before generation
spark.catalog.clearCache()

data_rows = UNIQUE_CUSTOMERS

customer_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("customer_id","decimal(10)", minValue=CUSTOMER_MIN_VALUE, uniqueValues=UNIQUE_CUSTOMERS)
            .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")  
           
            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE, 
                        baseColumn="customer_id", baseColumnType="hash")

            .withColumn("phone_number","decimal(10)",  minValue=SUBSCRIBER_NUM_MIN_VALUE, 
                        baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

            # for email, we'll just use the formatted phone number
            .withColumn("email","string",  format="subscriber_%s@myoperator.com", baseColumn="phone_number")
            .withColumn("plan", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS, random=True)
            .withColumn("contract_start_dt", "date", data_range=dg.DateRange("2023-01-01 00:00:00",
                                                                             "2024-12-31 11:59:59",
                                                                             "days=1"),
                        random=True)
            )

# Best Practice: Do NOT use .cache() with Delta tables
# Delta Lake handles caching internally and .cache() prevents data skipping optimizations
df_customers = (customer_dataspec.build()
                .dropDuplicates(["device_id"])
                .dropDuplicates(["phone_number"])
                .orderBy("customer_id")
               )

# Write the DataFrame to a Delta table with optimizations
df_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableOptimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{db}.customers")

logger.info(f"Created table {catalog}.{db}.customers with {UNIQUE_CUSTOMERS} rows")

# Re-read from Delta for subsequent operations (leverages Delta optimizations)
df_customers = spark.table(f"{catalog}.{db}.customers")

# COMMAND ----------

# MAGIC %md ###Lets model our billing events
# MAGIC
# MAGIC Billing events like data usage, calls and texts are generated for this solution. 

# COMMAND ----------

# DBTITLE 1,Generate billing events data
import dbldatagen as dg

spark.catalog.clearCache()

data_rows = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS
logger.info(f"Generating {data_rows:,} billing events")

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

# use random seed method of 'hash_fieldname' for better spread - default in later builds
events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested, randomSeed=42,
                                    randomSeedMethod="hash_fieldname")
             # use same logic as per customers dataset to ensure matching keys - but make them random
            .withColumn("device_id_base", "decimal(10)", minValue=CUSTOMER_MIN_VALUE, uniqueValues=UNIQUE_CUSTOMERS,
                        random=True, omit=True)
            .withColumn("device_id", "decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="device_id_base", baseColumnType="hash")

            # use specific random seed to get better spread of values
            .withColumn("event_type", "string",  values=["call_mins_local", "data_local", "data_roaming", "call_mins_roaming", "texts_roaming", "call_mins_international", "texts_local", "texts_international"],
                                                weights=[100, 100, 1, 0.5, 0.6, 2,20,2], random=True)

            # use Gamma distribution for skew towards short calls
            .withColumn("base_minutes","decimal(7,2)",  minValue=1.0, maxValue=5.0, step=0.1,
                        distribution=dg.distributions.Gamma(shape=1.5, scale=2.0), random=True, omit=True)
                   
            # use Gamma distribution for skew towards short transfers
            .withColumn("base_bytes_transferred","decimal(12)",  minValue=K_1, maxValue=MB_100, 
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0), random=True, omit=True)
                   
            .withColumn("minutes", "decimal(7,2)", baseColumn=["event_type", "base_minutes"],
                        expr="""
                              case when event_type in ("call_mins_local", "call_mins_roaming", "call_mins_international") then base_minutes
                              else 0
                              end
                               """)
            .withColumn("bytes_transferred", "decimal(12)", baseColumn=["event_type", "base_bytes_transferred"],
                        expr="""
                              case when event_type in ("data_local", "data_roaming") then base_bytes_transferred
                              else 0
                              end
                               """)
                   
            .withColumn("event_ts", "timestamp", data_range=dg.DateRange(start_dt,
                                                                             end_dt,
                                                                             "seconds=1"),
                        random=True)
                   
            )

df_events = events_dataspec.build()

# COMMAND ----------

# MAGIC %md ###Billing Items
# MAGIC
# MAGIC Now generate events for generating billing items by joining the datasets customer, billing plans and events.

# COMMAND ----------

# DBTITLE 1,Create billing items by joining customers and plans
# Join the customer dataframe with the billing plans based on plan_key
df_customer_pricing = df_customers.join(df_plans, df_plans.Plan_key == df_customers.plan)

# COMMAND ----------

# DBTITLE 1,Filter and write billing items with Delta optimizations
# remove events before the contract start date
df_billing_items = df_events.alias("events") \
    .join(df_customer_pricing.alias("pricing"), df_events.device_id == df_customer_pricing.device_id) \
    .where(df_events.event_ts >= df_customer_pricing.contract_start_dt) \
    .select("events.*", "pricing.contract_start_dt") 
    
# Write the DataFrame to a Delta table with optimizations
df_billing_items.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableOptimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{db}.billing_items")

logger.info(f"Created table {catalog}.{db}.billing_items")

# COMMAND ----------

# MAGIC %md ###Billing Invoices
# MAGIC
# MAGIC Now generate monthly invoices based on the billing items and other master datasets

# COMMAND ----------

# MAGIC %md let's compute our summary information

# COMMAND ----------

# DBTITLE 1,Compute summary with CORRECT MB calculation (1024 not 1028)
import pyspark.sql.functions as F

# Re-read billing_items from Delta for optimal performance
df_billing_items = spark.table(f"{catalog}.{db}.billing_items")

# lets compute the summary minutes messages and bytes transferred
df_enriched_events = (df_billing_items
                      .withColumn("texts_roaming", F.expr("case when event_type='texts_roaming' then 1 else 0 end"))
                      .withColumn("texts_international", F.expr("case when event_type='texts_international' then 1 else 0 end"))
                      .withColumn("call_mins_roaming", F.expr("case when event_type='call_mins_roaming' then cast(ceil(minutes) as decimal(18,3)) else 0.0 end"))
                      .withColumn("call_mins_international", F.expr("case when event_type='call_mins_international' then cast(ceil(minutes) as decimal(18,3)) else 0.0 end"))
                      .withColumn("data_local", F.expr("case when event_type='data_local' then cast(ceil(bytes_transferred) as decimal(30,3)) else 0.0 end"))
                      .withColumn("data_roaming", F.expr("case when event_type='data_roaming' then cast(ceil(bytes_transferred) as decimal(30,3)) else 0.0 end"))
                     )

df_enriched_events.createOrReplaceTempView("telephony_events")

# BUG FIX: Changed 1028*1028 to 1024*1024 for correct MB calculation
# 1 MB = 1024 * 1024 bytes (not 1028 * 1028)
df_summary = spark.sql("""
    SELECT 
        device_id, 
        concat(extract(year FROM event_ts),"-",lpad(extract(month FROM event_ts),2,'0')) as event_month,
        round(sum(data_local) / (1024*1024), 3) as data_local_mb, 
        round(sum(data_roaming) / (1024*1024), 3) as data_roaming_mb, 
        sum(texts_roaming) as texts_roaming,
        sum(texts_international) as texts_international,
        sum(call_mins_roaming) as call_mins_roaming,
        sum(call_mins_international) as call_mins_international, 
        count(device_id) as event_count
    FROM telephony_events
    GROUP BY 1, 2
""")

df_summary.createOrReplaceTempView("event_summary")

# COMMAND ----------

# MAGIC %md let's create a summary temp view

# COMMAND ----------

df_customer_summary = (df_customer_pricing.join(df_summary, df_customer_pricing.device_id == df_summary.device_id)
                       .createOrReplaceTempView("customer_summary"))

# COMMAND ----------

# MAGIC %md
# MAGIC Now generate the invoices

# COMMAND ----------

# DBTITLE 1,Generate invoices with CORRECT MB calculation
# BUG FIX: Changed 1028 to 1024 in data_charges_outside_allowance calculation
df_invoices = spark.sql(
    """SELECT customer_id, 
       customer_name, 
       event_month, 
       phone_number, 
       email, 
       plan_name,      
       contract_start_dt, 
       contract_in_months, 
       monthly_charges_dollars as monthly_charges, 
       Calls_Text, 
       Internet_Speed_MBPS, 
       Data_Limit_GB, 
       Data_Outside_Allowance_Per_MB, 
       Roam_Data_charges_per_MB, 
       Roam_Call_charges_per_min, 
       Roam_text_charges, 
       International_text_charge,
       International_call_charge_per_min,
       data_local_mb,
       data_roaming_mb,
       call_mins_roaming,
       texts_roaming,
       call_mins_international,
       texts_international,
       case 
           when Data_Limit_GB != 'UNLIMITED' 
           then case 
                    when (data_local_mb - cast(Data_Limit_GB as double) * 1024) > 0
                    then cast((data_local_mb - cast(Data_Limit_GB as double) * 1024) * Data_Outside_Allowance_Per_MB as decimal(18,2))   
                    else 0 
                end
           else 0 
       end as data_charges_outside_allowance,
       case 
           when data_roaming_mb > 0 
           then cast(data_roaming_mb * Roam_Data_charges_per_MB as decimal(18,2))
           else 0 
       end as roaming_data_charges,
       case 
           when call_mins_roaming > 0 
           then cast(ceiling(call_mins_roaming) * Roam_Call_charges_per_min as decimal(18,2))
           else 0 
       end as roaming_call_charges,
       case 
           when texts_roaming > 0 
           then cast(texts_roaming * Roam_text_charges as decimal(18,2))
           else 0 
       end as roaming_text_charges,
       case 
           when call_mins_international > 0 
           then cast(ceiling(call_mins_international) * International_call_charge_per_min as decimal(18,2))
           else 0 
       end as international_call_charges,
       case 
           when texts_international > 0 
           then cast(texts_international * International_text_charge as decimal(18,2))
           else 0 
       end as international_text_charges,
       monthly_charges_dollars + data_charges_outside_allowance + roaming_data_charges + roaming_call_charges + roaming_text_charges + international_call_charges + international_text_charges as total_charges
FROM customer_summary
"""
)

# COMMAND ----------

# DBTITLE 1,Write invoice table with liquid clustering for performance
# Write the DataFrame to a Delta table with optimizations
df_invoices.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.enableOptimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{db}.invoice")

# Best Practice: Add liquid clustering for frequently queried columns
# This improves query performance on large tables
spark.sql(f"""
    ALTER TABLE {catalog}.{db}.invoice 
    CLUSTER BY (customer_id, event_month)
""")

logger.info(f"Created table {catalog}.{db}.invoice with liquid clustering on (customer_id, event_month)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant Permissions (Best Practice: Use Groups)

# COMMAND ----------

# DBTITLE 1,Grant permissions to defined groups
# Best Practice: Grant permissions to groups, not individuals
admin_group = config.get('admin_group', 'telco_billing_admins')
data_engineer_group = config.get('data_engineer_group', 'telco_billing_data_engineers')
data_scientist_group = config.get('data_scientist_group', 'telco_billing_data_scientists')

# Grant schema-level permissions
try:
    spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.{db} TO `{data_engineer_group}`")
    spark.sql(f"GRANT USAGE ON SCHEMA {catalog}.{db} TO `{data_scientist_group}`")
    spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{db} TO `{admin_group}`")
    
    # Grant table-level permissions for data scientists (read-only)
    for table in ['billing_plans', 'customers', 'billing_items', 'invoice']:
        spark.sql(f"GRANT SELECT ON TABLE {catalog}.{db}.{table} TO `{data_scientist_group}`")
    
    # Grant table-level permissions for data engineers (read-write)
    for table in ['billing_plans', 'customers', 'billing_items', 'invoice']:
        spark.sql(f"GRANT SELECT, MODIFY ON TABLE {catalog}.{db}.{table} TO `{data_engineer_group}`")
    
    logger.info("Permissions granted successfully")
except Exception as e:
    logger.warning(f"Could not grant permissions (groups may not exist): {e}")
    logger.info("Create the groups and re-run this cell to apply permissions")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

# DBTITLE 1,Print summary
ui_functions_path = f"{config['catalog']}.{config['database']}"
logger.info(f"Data preparation complete. Tables created in: {ui_functions_path}")
logger.info(f"Tables: billing_plans, customers, billing_items, invoice")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate config.yml for backward compatibility
# MAGIC 
# MAGIC Note: The primary config is now in 000-config.py with Databricks Secrets support.
# MAGIC This config.yml is generated for backward compatibility only.

# COMMAND ----------

# DBTITLE 1,Generate config.yml (backward compatibility)
content = f"""# Auto-generated config for backward compatibility
# Primary configuration is in 000-config.py with Databricks Secrets support

agent_prompt: |
  You are a Billing Support Agent assisting users with billing inquiries.

  Guidelines:
  - First, check FAQ Search before requesting any details.
  - If an FAQ answer exists, return it immediately.
  - If no FAQ match, request the customer_id before retrieving billing details.
  - Do not disclose confidential information like names, emails, device_id.

  Process:
  1. Run FAQ Search -> If an answer exists, return it.
  2. If no FAQ match, ask for the customer_id and use the relevant tool(s) to fetch billing details.
  3. If missing details (e.g., timeframe), ask clarifying questions.

  Keep responses polite, professional, and concise.

llm_endpoint: "{llm_endpoint}"
warehouse_id: "{warehouse_id}"
vector_search_index: "{ui_functions_path}.faq_index"
uc_functions:
  - "{ui_functions_path}.*"
"""

with open('config.yml', "w") as f:
    f.write(content)

logger.info("Generated config.yml for backward compatibility")
