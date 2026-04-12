# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebase Setup: Operational Write-Back Store
# MAGIC
# MAGIC Provisions a Lakebase (managed PostgreSQL) instance and bootstraps the
# MAGIC operational schema for billing disputes, audit trail, and agent actions.
# MAGIC
# MAGIC ### What is Lakebase?
# MAGIC Lakebase is Databricks-managed PostgreSQL for low-latency transactional CRUD.
# MAGIC It is **not** the same as Lakehouse Federation (which queries external databases
# MAGIC via foreign catalogs). Lakebase runs **inside** Databricks.
# MAGIC
# MAGIC ### Why Lakebase for write-back?
# MAGIC The accelerator's write-back operations (disputes, audit, approvals) are
# MAGIC **transactional single-row writes** — the wrong workload for a SQL warehouse
# MAGIC optimized for large analytical scans. Lakebase provides:
# MAGIC - Sub-100ms point writes (vs 1-3s via Statement Execution API)
# MAGIC - ACID transactions across dispute + audit in a single commit
# MAGIC - No SQL warehouse dependency for writes (eliminates PM-010 contention)
# MAGIC
# MAGIC ### Architecture
# MAGIC ```
# MAGIC Agent / Dash App
# MAGIC      │ (psycopg2)
# MAGIC      v
# MAGIC Lakebase (PostgreSQL)
# MAGIC   ├── billing_ops.disputes
# MAGIC   ├── billing_ops.audit_log
# MAGIC   └── billing_ops.agent_actions
# MAGIC      │
# MAGIC      │ Synced Tables (reverse ETL)
# MAGIC      v
# MAGIC Unity Catalog (Delta)
# MAGIC   ├── billing_disputes_synced   → Genie analytics
# MAGIC   └── billing_audit_synced      → Compliance reporting
# MAGIC ```
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Run `000-config` first
# MAGIC - Lakebase must be available in your workspace (Public Preview)
# MAGIC
# MAGIC ### Track C
# MAGIC This is **Track C** in the accelerator's data architecture:
# MAGIC - Track A: External ERP via Lakehouse Federation
# MAGIC - Track B: Synthetic ERP simulation
# MAGIC - **Track C: Lakebase operational store for write-back**

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Configuration
import os
import json

catalog = config['catalog']
schema = config['database']

# Lakebase configuration — set these in config.yaml or environment
LAKEBASE_INSTANCE = config.get('lakebase_instance', 'billing-accelerator')
LAKEBASE_DATABASE = config.get('lakebase_database', 'databricks_postgres')
LAKEBASE_SCHEMA   = config.get('lakebase_schema', 'billing_ops')

# Connection parameters — auto-injected by Databricks Apps, or set manually
PG_HOST     = os.getenv("PGHOST", config.get("lakebase_host", ""))
PG_PORT     = os.getenv("PGPORT", config.get("lakebase_port", "5432"))
PG_USER     = os.getenv("PGUSER", config.get("lakebase_user", ""))
PG_PASSWORD = os.getenv("PGPASSWORD", config.get("lakebase_password", ""))
PG_DATABASE = os.getenv("PGDATABASE", LAKEBASE_DATABASE)

print(f"Lakebase instance: {LAKEBASE_INSTANCE}")
print(f"Lakebase database: {PG_DATABASE}")
print(f"Lakebase schema:   {LAKEBASE_SCHEMA}")
print(f"PG host:           {PG_HOST or '(not set — will provision)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Provision Lakebase Instance
# MAGIC
# MAGIC Creates the managed PostgreSQL instance if it does not already exist.

# COMMAND ----------

# DBTITLE 1,Provision or Verify Lakebase Instance
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Check if instance already exists
lakebase_ready = False
connection_info = {}

try:
    # List existing Lakebase instances via SDK/REST
    # The exact API depends on workspace configuration
    resp = w.api_client.do("GET", "/api/2.0/lakebase/instances")
    instances = resp.get("instances", []) if isinstance(resp, dict) else []
    for inst in instances:
        if inst.get("name") == LAKEBASE_INSTANCE:
            status = inst.get("status", inst.get("state", ""))
            connection_info = inst.get("connection_info", inst)
            print(f"Found existing instance '{LAKEBASE_INSTANCE}': {status}")
            if status.upper() in ("RUNNING", "ACTIVE", "ONLINE"):
                lakebase_ready = True
                # Extract connection info
                PG_HOST = connection_info.get("host", PG_HOST)
                PG_PORT = str(connection_info.get("port", PG_PORT))
            break
except Exception as e:
    print(f"Could not query Lakebase API: {e}")
    print("Attempting alternative provisioning path...")

if not lakebase_ready and not PG_HOST:
    # Try to provision via REST API
    try:
        result = w.api_client.do("POST", "/api/2.0/lakebase/instances", body={
            "name": LAKEBASE_INSTANCE,
            "type": "provisioned",
            "capacity": config.get("lakebase_capacity", "CU_1"),
        })
        print(f"Provisioning Lakebase instance: {result}")
        print("Instance may take 2-5 minutes to provision.")
        print("Re-run this cell after provisioning completes, or set connection info manually.")
    except Exception as e:
        print(f"Automated provisioning not available: {e}")
        print("\n" + "=" * 60)
        print("MANUAL STEP: Provision Lakebase Instance")
        print("=" * 60)
        print(f"""
1. Go to your Databricks workspace
2. Navigate to Catalog > Lakebase
3. Create a new instance named: {LAKEBASE_INSTANCE}
4. Select capacity: CU_1 (smallest)
5. Wait for status: RUNNING
6. Copy the connection details (host, port, user, password)
7. Set them in config.yaml under lakebase_host, lakebase_port, etc.
8. Or set PGHOST/PGPORT/PGUSER/PGPASSWORD environment variables
9. Re-run this notebook.
""")
        print("=" * 60)

if PG_HOST:
    print(f"\nLakebase connection: {PG_HOST}:{PG_PORT}/{PG_DATABASE}")
    lakebase_ready = True
else:
    print("\nWARNING: No Lakebase connection available.")
    print("Set PG_HOST or provision an instance, then re-run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Bootstrap PostgreSQL Schema
# MAGIC
# MAGIC Creates the `billing_ops` schema and operational tables.

# COMMAND ----------

# DBTITLE 1,Create Schema and Tables
if not lakebase_ready:
    print("SKIP: No Lakebase connection. Set PG_HOST and re-run.")
else:
    import psycopg2

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DATABASE,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Create schema
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKEBASE_SCHEMA}")
        print(f"Schema {LAKEBASE_SCHEMA} ensured.")

        # Disputes table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.disputes (
                dispute_id          VARCHAR(36) PRIMARY KEY,
                customer_id         BIGINT NOT NULL,
                anomaly_id          VARCHAR(64),
                event_month         VARCHAR(7),
                dispute_type        VARCHAR(32) NOT NULL DEFAULT 'AGENT_CREATED',
                status              VARCHAR(32) NOT NULL DEFAULT 'OPEN',
                description         TEXT NOT NULL,
                resolution_notes    TEXT,
                disputed_amount_usd NUMERIC(12,2),
                resolved_amount_usd NUMERIC(12,2),
                created_by          VARCHAR(128) NOT NULL,
                assigned_to         VARCHAR(128),
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                resolved_at         TIMESTAMPTZ,
                session_id          VARCHAR(64),
                request_id          VARCHAR(64),
                persona             VARCHAR(32)
            )
        """)
        print(f"Table {LAKEBASE_SCHEMA}.disputes ensured.")

        # Audit log table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.audit_log (
                audit_id            VARCHAR(36) PRIMARY KEY,
                action_type         VARCHAR(64) NOT NULL,
                target_table        VARCHAR(128) NOT NULL,
                target_record_id    VARCHAR(64),
                customer_id         BIGINT,
                initiating_user     VARCHAR(128) NOT NULL DEFAULT 'UNKNOWN',
                executing_principal VARCHAR(128) NOT NULL DEFAULT 'billing-agent-sp',
                persona             VARCHAR(32),
                request_id          VARCHAR(64),
                session_id          VARCHAR(64),
                sql_statement       TEXT,
                result_status       VARCHAR(16) NOT NULL DEFAULT 'PENDING',
                result_message      TEXT,
                identity_degraded   BOOLEAN DEFAULT FALSE,
                user_groups         TEXT,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        print(f"Table {LAKEBASE_SCHEMA}.audit_log ensured.")

        # Agent actions table
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.agent_actions (
                action_id       VARCHAR(36) PRIMARY KEY,
                action_type     VARCHAR(64) NOT NULL,
                tool_name       VARCHAR(128),
                customer_id     BIGINT,
                initiating_user VARCHAR(128),
                persona         VARCHAR(32),
                session_id      VARCHAR(64),
                request_id      VARCHAR(64),
                input_summary   TEXT,
                output_summary  TEXT,
                duration_ms     INTEGER,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        print(f"Table {LAKEBASE_SCHEMA}.agent_actions ensured.")

        # Indexes for common query patterns
        for idx_sql in [
            f"CREATE INDEX IF NOT EXISTS idx_disputes_customer ON {LAKEBASE_SCHEMA}.disputes(customer_id)",
            f"CREATE INDEX IF NOT EXISTS idx_disputes_status ON {LAKEBASE_SCHEMA}.disputes(status)",
            f"CREATE INDEX IF NOT EXISTS idx_audit_action ON {LAKEBASE_SCHEMA}.audit_log(action_type, created_at)",
            f"CREATE INDEX IF NOT EXISTS idx_audit_user ON {LAKEBASE_SCHEMA}.audit_log(initiating_user, created_at)",
            f"CREATE INDEX IF NOT EXISTS idx_actions_session ON {LAKEBASE_SCHEMA}.agent_actions(session_id)",
        ]:
            cur.execute(idx_sql)
        print("Indexes created.")

    conn.close()
    print("\nLakebase schema bootstrap complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Read/Write

# COMMAND ----------

# DBTITLE 1,Test Write and Read
if not lakebase_ready:
    print("SKIP: No Lakebase connection.")
else:
    import uuid
    from datetime import datetime, timezone

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DATABASE,
    )

    test_id = f"TEST-{str(uuid.uuid4())[:8]}"

    try:
        with conn:
            with conn.cursor() as cur:
                # Write a test dispute
                cur.execute(f"""
                    INSERT INTO {LAKEBASE_SCHEMA}.disputes
                    (dispute_id, customer_id, description, created_by, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (test_id, 4401, "Lakebase connectivity test", "setup_notebook", "TEST"))

                # Write a test audit entry
                cur.execute(f"""
                    INSERT INTO {LAKEBASE_SCHEMA}.audit_log
                    (audit_id, action_type, target_table, target_record_id, result_status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (str(uuid.uuid4()), "test_write", "disputes", test_id, "SUCCESS"))

        # Read it back
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT dispute_id, customer_id, status, created_at
                FROM {LAKEBASE_SCHEMA}.disputes
                WHERE dispute_id = %s
            """, (test_id,))
            row = cur.fetchone()

        if row:
            print(f"PASS: Write/read test succeeded")
            print(f"  dispute_id:  {row[0]}")
            print(f"  customer_id: {row[1]}")
            print(f"  status:      {row[2]}")
            print(f"  created_at:  {row[3]}")
        else:
            print("FAIL: Test record not found after insert")

        # Clean up test data
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {LAKEBASE_SCHEMA}.disputes WHERE dispute_id = %s", (test_id,))
                cur.execute(f"DELETE FROM {LAKEBASE_SCHEMA}.audit_log WHERE target_record_id = %s", (test_id,))
        print("  Test data cleaned up.")

    except Exception as e:
        print(f"FAIL: {e}")
    finally:
        conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Persist Configuration

# COMMAND ----------

# DBTITLE 1,Save Lakebase Config
if lakebase_ready:
    config['lakebase_enabled'] = True
    config['lakebase_instance'] = LAKEBASE_INSTANCE
    config['lakebase_host'] = PG_HOST
    config['lakebase_port'] = PG_PORT
    config['lakebase_database'] = PG_DATABASE
    config['lakebase_schema'] = LAKEBASE_SCHEMA
    print("Lakebase configuration saved to config dict.")
    print("Add these keys to config.yaml for persistence across notebooks.")
else:
    config['lakebase_enabled'] = False
    print("Lakebase not configured. Write-back will use Delta/Statement Execution API fallback.")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 60)
print("LAKEBASE SETUP SUMMARY")
print("=" * 60)
print(f"""
Instance:  {LAKEBASE_INSTANCE}
Database:  {PG_DATABASE}
Schema:    {LAKEBASE_SCHEMA}
Host:      {PG_HOST or '(not configured)'}
Status:    {'READY' if lakebase_ready else 'NOT CONFIGURED'}

Tables created:
  {'[x]' if lakebase_ready else '[ ]'} {LAKEBASE_SCHEMA}.disputes
  {'[x]' if lakebase_ready else '[ ]'} {LAKEBASE_SCHEMA}.audit_log
  {'[x]' if lakebase_ready else '[ ]'} {LAKEBASE_SCHEMA}.agent_actions

Next steps:
  {'1. Run 08d_lakebase_sync to set up Delta sync' if lakebase_ready else '1. Provision Lakebase instance and re-run this notebook'}
  {'2. Run 08e_validate_lakebase for full validation' if lakebase_ready else '2. Set PGHOST/PGPORT/PGUSER/PGPASSWORD'}
  {'3. Redeploy agent (notebook 03) to use Lakebase write path' if lakebase_ready else '3. Re-run this notebook'}
""")
print("=" * 60)
