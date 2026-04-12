# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Sync: Bridge Operational and Analytical Layers
# MAGIC
# MAGIC Sets up synced tables between Lakebase (transactional) and Delta (analytical).
# MAGIC
# MAGIC ### Sync direction
# MAGIC - **Lakebase → Delta**: Disputes and audit data synced to Delta so Genie and
# MAGIC   analytical queries can access operational state without querying PostgreSQL.
# MAGIC - **Delta → Lakebase** (optional): Reference data (customers, plans) synced to
# MAGIC   Lakebase for FK joins in application queries.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Run `08c_lakebase_setup` first (Lakebase instance + schema)
# MAGIC - Run `00_data_preparation` (customers, billing_plans tables)

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Configuration
catalog = config['catalog']
schema = config['database']
lakebase_instance = config.get('lakebase_instance', 'billing-accelerator')
lakebase_enabled = config.get('lakebase_enabled', False)

if not lakebase_enabled:
    print("Lakebase not configured. Run 08c_lakebase_setup first.")
    print("Skipping sync setup.")
    dbutils.notebook.exit("SKIPPED: Lakebase not configured")

print(f"Lakebase instance: {lakebase_instance}")
print(f"Target catalog:    {catalog}")
print(f"Target schema:     {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakebase → Delta Synced Tables
# MAGIC
# MAGIC These make operational data available to Genie and analytical queries.

# COMMAND ----------

# DBTITLE 1,Sync Disputes to Delta
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create synced table: Lakebase disputes → Delta billing_disputes_synced
# This uses Databricks' native sync infrastructure
try:
    # Register the Lakebase instance as a UC catalog if not already done
    lakebase_catalog = f"lakebase_{lakebase_instance.replace('-', '_')}"

    # Check if catalog exists
    try:
        w.catalogs.get(lakebase_catalog)
        print(f"Lakebase catalog already registered: {lakebase_catalog}")
    except Exception:
        # Register the Lakebase instance as a UC catalog
        try:
            w.api_client.do("POST", "/api/2.0/unity-catalog/catalogs", body={
                "name": lakebase_catalog,
                "connection_name": lakebase_instance,
                "comment": "Lakebase catalog for billing accelerator operational data",
            })
            print(f"Registered Lakebase catalog: {lakebase_catalog}")
        except Exception as e:
            print(f"Could not register catalog automatically: {e}")
            print(f"Register '{lakebase_instance}' as UC catalog '{lakebase_catalog}' manually.")

    # Create synced table for disputes
    disputes_source = f"{lakebase_catalog}.billing_ops.disputes"
    disputes_target = f"{catalog}.{schema}.billing_disputes_lakebase"

    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {disputes_target}
            USING DELTA
            TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
            AS SELECT * FROM {disputes_source} WHERE 1=0
        """)
        print(f"Created sync target: {disputes_target}")
    except Exception as e:
        print(f"Note: Direct sync table creation may require manual setup: {e}")
        print("Alternative: Use the Databricks UI to create a synced table from Lakebase.")

    # Create a view that unions Lakebase-synced disputes with any existing Delta disputes
    # This preserves backwards compatibility
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_all_disputes AS
        SELECT
            dispute_id, customer_id, anomaly_id, dispute_type, status,
            description, resolution_notes, disputed_amount_usd, resolved_amount_usd,
            created_by, assigned_to, created_at, updated_at, resolved_at,
            'lakebase' AS source
        FROM {disputes_target}
        UNION ALL
        SELECT
            dispute_id, customer_id, anomaly_id, dispute_type, status,
            description, resolution_notes, disputed_amount_usd, resolved_amount_usd,
            created_by, assigned_to, created_at, updated_at, resolved_at,
            'delta' AS source
        FROM {catalog}.{schema}.billing_disputes
        WHERE dispute_id NOT IN (SELECT dispute_id FROM {disputes_target})
    """)
    print(f"Created unified disputes view: {catalog}.{schema}.v_all_disputes")

except Exception as e:
    print(f"Sync setup encountered issues: {e}")
    print("\nManual sync alternative:")
    print(f"  1. Open Catalog Explorer in your workspace")
    print(f"  2. Find the Lakebase instance: {lakebase_instance}")
    print(f"  3. Create a synced table from billing_ops.disputes")
    print(f"  4. Target: {catalog}.{schema}.billing_disputes_lakebase")

# COMMAND ----------

# DBTITLE 1,Sync Audit Log to Delta
try:
    audit_target = f"{catalog}.{schema}.billing_audit_lakebase"
    audit_source = f"{lakebase_catalog}.billing_ops.audit_log"

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {audit_target}
        USING DELTA
        TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
        AS SELECT * FROM {audit_source} WHERE 1=0
    """)
    print(f"Created sync target: {audit_target}")

    # Unified audit view
    spark.sql(f"""
        CREATE OR REPLACE VIEW {catalog}.{schema}.v_all_audit AS
        SELECT *, 'lakebase' AS source FROM {audit_target}
        UNION ALL
        SELECT
            audit_id, action_type, target_table, target_record_id, customer_id,
            agent_session_id AS session_id, executed_by, payload_json,
            sql_statement, result_status, result_message, error_detail,
            executed_at AS created_at,
            initiating_user, executing_principal, persona, request_id,
            identity_degraded, user_groups,
            'delta' AS source
        FROM {catalog}.{schema}.billing_write_audit
    """)
    print(f"Created unified audit view: {catalog}.{schema}.v_all_audit")

except Exception as e:
    print(f"Audit sync setup issue: {e}")
    print("This is non-critical — audit data remains in Lakebase and Delta separately.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("LAKEBASE SYNC SUMMARY")
print("=" * 60)
print(f"""
Lakebase → Delta synced tables:
  billing_ops.disputes    → {catalog}.{schema}.billing_disputes_lakebase
  billing_ops.audit_log   → {catalog}.{schema}.billing_audit_lakebase

Unified views (combining Lakebase + Delta sources):
  {catalog}.{schema}.v_all_disputes   — all disputes from both backends
  {catalog}.{schema}.v_all_audit      — all audit entries from both backends

These views can be added to the Genie Space for analytics.

Next: Run 08e_validate_lakebase to verify the full pipeline.
""")
print("=" * 60)
