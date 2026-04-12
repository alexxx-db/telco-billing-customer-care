# Databricks notebook source
# MAGIC %md
# MAGIC # Validate Lakebase Setup
# MAGIC
# MAGIC Checks connectivity, schema, read/write, and sync for the Lakebase
# MAGIC operational store. Reports PASS/FAIL for each check.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC - Run `08c_lakebase_setup` first

# COMMAND ----------

# MAGIC %pip install psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

import os
import uuid
import psycopg2
from datetime import datetime, timezone

catalog = config['catalog']
schema = config['database']

PG_HOST     = os.getenv("PGHOST", config.get("lakebase_host", ""))
PG_PORT     = os.getenv("PGPORT", config.get("lakebase_port", "5432"))
PG_USER     = os.getenv("PGUSER", config.get("lakebase_user", ""))
PG_PASSWORD = os.getenv("PGPASSWORD", config.get("lakebase_password", ""))
PG_DATABASE = os.getenv("PGDATABASE", config.get("lakebase_database", "databricks_postgres"))
LB_SCHEMA   = config.get("lakebase_schema", "billing_ops")

results = []

def record(check: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append({"check": check, "status": status, "detail": detail})
    icon = "+" if passed else "X"
    print(f"  [{icon}] {check}" + (f" — {detail}" if detail else ""))

# COMMAND ----------

# DBTITLE 1,Check 1: PostgreSQL Connectivity
try:
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        user=PG_USER, password=PG_PASSWORD,
        database=PG_DATABASE,
        connect_timeout=10,
    )
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        version = cur.fetchone()[0]
    conn.close()
    record("PostgreSQL connectivity", True, f"Connected to {PG_HOST}:{PG_PORT}")
except Exception as e:
    record("PostgreSQL connectivity", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 2: Schema Exists
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    with conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = %s",
                    (LB_SCHEMA,))
        row = cur.fetchone()
    conn.close()
    if row:
        record("Schema exists", True, f"{LB_SCHEMA}")
    else:
        record("Schema exists", False, f"Schema '{LB_SCHEMA}' not found")
except Exception as e:
    record("Schema exists", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 3: Required Tables Exist
required_tables = ["disputes", "audit_log", "agent_actions"]
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_name = ANY(%s)
        """, (LB_SCHEMA, required_tables))
        found = {row[0] for row in cur.fetchall()}
    conn.close()
    missing = set(required_tables) - found
    if not missing:
        record("Required tables exist", True, f"All {len(required_tables)} tables present")
    else:
        record("Required tables exist", False, f"Missing: {sorted(missing)}")
except Exception as e:
    record("Required tables exist", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 4: Write and Read Round-Trip
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    test_id = f"VAL-{uuid.uuid4().hex[:8]}"

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {LB_SCHEMA}.disputes
                (dispute_id, customer_id, description, created_by, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (test_id, 9999, "Validation test", "validate_notebook", "TEST"))

    with conn.cursor() as cur:
        cur.execute(f"SELECT dispute_id, status FROM {LB_SCHEMA}.disputes WHERE dispute_id = %s",
                    (test_id,))
        row = cur.fetchone()

    if row and row[0] == test_id:
        record("Write/read round-trip", True, f"Wrote and read back {test_id}")
    else:
        record("Write/read round-trip", False, "Insert succeeded but read-back failed")

    # Cleanup
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LB_SCHEMA}.disputes WHERE dispute_id = %s", (test_id,))
    conn.close()

except Exception as e:
    record("Write/read round-trip", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 5: Transaction Atomicity
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    test_id = f"TXN-{uuid.uuid4().hex[:8]}"
    audit_id = str(uuid.uuid4())

    # Write dispute + audit in a single transaction
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {LB_SCHEMA}.disputes
                (dispute_id, customer_id, description, created_by)
                VALUES (%s, %s, %s, %s)
            """, (test_id, 9999, "Transaction test", "validate"))
            cur.execute(f"""
                INSERT INTO {LB_SCHEMA}.audit_log
                (audit_id, action_type, target_table, target_record_id, result_status)
                VALUES (%s, %s, %s, %s, %s)
            """, (audit_id, "create_dispute", "disputes", test_id, "SUCCESS"))

    # Verify both exist
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {LB_SCHEMA}.disputes WHERE dispute_id = %s", (test_id,))
        d_count = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM {LB_SCHEMA}.audit_log WHERE audit_id = %s", (audit_id,))
        a_count = cur.fetchone()[0]

    if d_count == 1 and a_count == 1:
        record("Transaction atomicity", True, "Dispute + audit committed in single transaction")
    else:
        record("Transaction atomicity", False, f"dispute={d_count}, audit={a_count}")

    # Cleanup
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {LB_SCHEMA}.disputes WHERE dispute_id = %s", (test_id,))
            cur.execute(f"DELETE FROM {LB_SCHEMA}.audit_log WHERE audit_id = %s", (audit_id,))
    conn.close()

except Exception as e:
    record("Transaction atomicity", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 6: Disputes Columns Match Expected Schema
expected_cols = {
    "dispute_id", "customer_id", "anomaly_id", "dispute_type", "status",
    "description", "resolution_notes", "disputed_amount_usd", "resolved_amount_usd",
    "created_by", "assigned_to", "created_at", "updated_at", "resolved_at",
    "session_id", "request_id", "persona",
}
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'disputes'
        """, (LB_SCHEMA,))
        actual_cols = {row[0] for row in cur.fetchall()}
    conn.close()

    missing = expected_cols - actual_cols
    if not missing:
        record("Disputes schema", True, f"{len(actual_cols)} columns, all expected present")
    else:
        record("Disputes schema", False, f"Missing columns: {sorted(missing)}")
except Exception as e:
    record("Disputes schema", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 7: Audit Log Has Identity Columns
identity_cols = {"initiating_user", "executing_principal", "persona", "request_id",
                 "identity_degraded", "user_groups"}
try:
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASSWORD, database=PG_DATABASE)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = 'audit_log'
        """, (LB_SCHEMA,))
        actual_cols = {row[0] for row in cur.fetchall()}
    conn.close()

    missing = identity_cols - actual_cols
    if not missing:
        record("Audit identity columns", True, f"All identity columns present")
    else:
        record("Audit identity columns", False, f"Missing: {sorted(missing)}")
except Exception as e:
    record("Audit identity columns", False, str(e))

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "=" * 60)
print("LAKEBASE VALIDATION SUMMARY")
print("=" * 60)

pass_count = sum(1 for r in results if r["status"] == "PASS")
fail_count = sum(1 for r in results if r["status"] == "FAIL")

for r in results:
    icon = "+" if r["status"] == "PASS" else "X"
    print(f"  [{icon}] {r['check']}: {r['status']}")
    if r["detail"]:
        print(f"      {r['detail']}")

print(f"\nTotal: {pass_count} PASS, {fail_count} FAIL out of {len(results)} checks")

if fail_count == 0:
    print("\nLakebase is ready for operational write-back.")
else:
    print(f"\n{fail_count} check(s) failed. Review and fix before using Lakebase for writes.")

display(spark.createDataFrame(results))
