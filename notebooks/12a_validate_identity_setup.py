# Databricks notebook source
# MAGIC %md
# MAGIC # Validate EchoStar Identity Propagation Setup
# MAGIC
# MAGIC Runs a suite of checks to confirm the identity propagation infrastructure
# MAGIC is correctly configured. Reports PASS/FAIL for each check.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
schema = config['database']

results = []

def record(check_name: str, passed: bool, detail: str = ""):
    status = "PASS" if passed else "FAIL"
    results.append({"check": check_name, "status": status, "detail": detail})
    print(f"  [{status}] {check_name}" + (f" — {detail}" if detail else ""))

# COMMAND ----------

# DBTITLE 1,Check 1: Secret Scope Exists and HMAC Key is Accessible
scope = config.get("identity_secret_scope", "echostar-identity")
key = config.get("identity_secret_key", "hmac-secret")

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    secret_resp = w.secrets.get_secret(scope=scope, key=key)
    record("Secret scope accessible", True, f"scope={scope}, key={key}")
except Exception as e:
    record("Secret scope accessible", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 2: RequestContext Round-Trip (Sign, Serialize, Deserialize, Verify)
try:
    import sys, os
    # Ensure identity_utils is importable
    nb_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else "/Workspace"
    if nb_dir not in sys.path:
        sys.path.insert(0, nb_dir)

    from identity_utils import RequestContext

    test_secret = b"test-secret-for-validation-only"
    ctx = RequestContext.create(
        user_email="test@example.com",
        user_groups=["billing_finance", "platform_engineering"],
        persona="finance_ops",
        session_id="test-session-001",
        secret=test_secret,
        ttl_minutes=5,
    )

    # Serialize
    json_str = ctx.to_json()

    # Deserialize
    ctx2 = RequestContext.from_json(json_str)

    # Verify signature
    sig_valid = ctx2.verify(test_secret)
    not_expired = not ctx2.is_expired()

    if sig_valid and not_expired:
        record("RequestContext round-trip", True,
               f"request_id={ctx2.request_id}, sig_valid={sig_valid}, not_expired={not_expired}")
    else:
        record("RequestContext round-trip", False,
               f"sig_valid={sig_valid}, not_expired={not_expired}")
except Exception as e:
    record("RequestContext round-trip", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 3: gov.* Tags Exist on at Least One Table
try:
    tags_df = spark.sql(f"""
        SELECT COUNT(DISTINCT table_name) as tagged_tables
        FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}'
          AND tag_name LIKE 'gov.%'
    """)
    count = tags_df.collect()[0].tagged_tables
    if count > 0:
        record("gov.* tags present", True, f"{count} tables have gov.* tags")
    else:
        record("gov.* tags present", False,
               "No tables have gov.* tags. Run notebook 12_admin_tagging first.")
except Exception as e:
    record("gov.* tags present", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 4: billing_write_audit Has Identity Columns
try:
    cols_df = spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.billing_write_audit")
    col_names = {row.col_name for row in cols_df.collect()}
    required_cols = {"initiating_user", "executing_principal", "persona",
                     "request_id", "identity_degraded"}
    missing = required_cols - col_names
    if not missing:
        record("Audit table identity columns", True,
               f"All required columns present: {sorted(required_cols)}")
    else:
        record("Audit table identity columns", False,
               f"Missing columns: {sorted(missing)}")
except Exception as e:
    record("Audit table identity columns", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 5: lookup_customer Does NOT Return PII Columns
try:
    test_result = spark.sql(f"SELECT * FROM {catalog}.{schema}.lookup_customer('1') LIMIT 0")
    returned_cols = set(test_result.columns)
    pii_cols = {"customer_name", "email", "phone_number"}
    leaked = pii_cols & returned_cols
    if not leaked:
        record("lookup_customer PII-free", True,
               f"Columns returned: {sorted(returned_cols)}")
    else:
        record("lookup_customer PII-free", False,
               f"PII columns still present: {sorted(leaked)}")
except Exception as e:
    record("lookup_customer PII-free", False, str(e))

# COMMAND ----------

# DBTITLE 1,Check 6: lookup_billing_items Has LIMIT
try:
    # Inspect the function DDL for LIMIT clause
    ddl_rows = spark.sql(
        f"SHOW CREATE FUNCTION {catalog}.{schema}.lookup_billing_items"
    ).collect()
    ddl_text = ddl_rows[0][0] if ddl_rows else ""
    has_limit = "LIMIT" in ddl_text.upper()
    if has_limit:
        record("lookup_billing_items has LIMIT", True)
    else:
        record("lookup_billing_items has LIMIT", False,
               "No LIMIT clause found in function DDL — see PM-011")
except Exception as e:
    record("lookup_billing_items has LIMIT", False, str(e))

# COMMAND ----------

# DBTITLE 1,Summary
print("\n" + "=" * 60)
print("IDENTITY SETUP VALIDATION SUMMARY")
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
    print("\nAll checks passed. Identity propagation is ready.")
else:
    print(f"\n{fail_count} check(s) failed. Review and fix before deploying.")

display(spark.createDataFrame(results))
