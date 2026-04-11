# Databricks notebook source
# MAGIC %md
# MAGIC # Governance Tag Administration
# MAGIC
# MAGIC Bulk-apply, validate, and export `gov.*` Unity Catalog tags for the
# MAGIC EchoStar identity propagation and authorization-aware agent platform.
# MAGIC
# MAGIC **Tags used:**
# MAGIC - `gov.identity.mode` — `required` | `preferred` | `none`
# MAGIC - `gov.pii.level` — `high` | `medium` | `low` | `none`
# MAGIC - `gov.audit.required` — `true` | `false`
# MAGIC - `gov.allowed.personas` — comma-separated persona names (empty = all)
# MAGIC - `gov.projection.policy` — `full` | `redacted` | `minimal`
# MAGIC - `gov.write.policy` — `user_confirmed` | `sp_allowed` | `blocked`
# MAGIC - `gov.data.classification` — `internal` | `confidential` | `public`

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

catalog = config['catalog']
schema = config['database']

# COMMAND ----------

# DBTITLE 1,Bulk Apply Tags
def bulk_apply_tags(catalog: str, schema: str, tags: dict, table_names: list = None):
    """Apply a dict of gov.* tags to tables/views in the target scope.

    Args:
        catalog: UC catalog name
        schema: UC schema name
        tags: dict of tag_name -> tag_value, e.g. {"gov.identity.mode": "required"}
        table_names: optional list of specific tables. If None, applies to ALL tables in schema.
    """
    if not tags:
        print("No tags provided.")
        return

    if table_names is None:
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        table_names = [row.tableName for row in tables_df.collect()]

    tag_clause = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])

    results = []
    for tbl in table_names:
        fqn = f"{catalog}.{schema}.{tbl}"
        try:
            spark.sql(f"ALTER TABLE {fqn} SET TAGS ({tag_clause})")
            results.append({"table": fqn, "status": "OK", "tags_applied": len(tags)})
            print(f"  Tagged {fqn}")
        except Exception as e:
            # Try as VIEW
            try:
                spark.sql(f"ALTER VIEW {fqn} SET TAGS ({tag_clause})")
                results.append({"table": fqn, "status": "OK (view)", "tags_applied": len(tags)})
                print(f"  Tagged {fqn} (view)")
            except Exception as e2:
                results.append({"table": fqn, "status": f"FAILED: {e2}", "tags_applied": 0})
                print(f"  FAILED {fqn}: {e2}")

    return spark.createDataFrame(results)

# COMMAND ----------

# DBTITLE 1,Validate Tag Coverage
def validate_tags(catalog: str, schema: str):
    """Scan all assets and report which are missing required gov.* tags.

    Returns a DataFrame of non-compliant assets with missing tag names.
    """
    required_tags = ["gov.identity.mode", "gov.pii.level", "gov.audit.required"]

    # Get all tables in schema
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
    table_names = [row.tableName for row in tables_df.collect()]

    # Get existing tags
    try:
        existing_tags = spark.sql(f"""
            SELECT table_name, tag_name, tag_value
            FROM {catalog}.information_schema.table_tags
            WHERE schema_name = '{schema}'
              AND tag_name LIKE 'gov.%'
        """).collect()
    except Exception as e:
        print(f"Cannot query table_tags: {e}")
        existing_tags = []

    # Build lookup: table_name -> set of tag names
    tag_lookup = {}
    for row in existing_tags:
        tag_lookup.setdefault(row.table_name, set()).add(row.tag_name)

    # Check compliance
    non_compliant = []
    for tbl in table_names:
        present = tag_lookup.get(tbl, set())
        missing = [t for t in required_tags if t not in present]
        if missing:
            non_compliant.append({
                "table_name": tbl,
                "fqn": f"{catalog}.{schema}.{tbl}",
                "missing_tags": ", ".join(missing),
                "existing_tags": ", ".join(sorted(present)) if present else "none",
            })

    if non_compliant:
        df = spark.createDataFrame(non_compliant)
        print(f"Found {len(non_compliant)} non-compliant assets out of {len(table_names)} total.")
        return df
    else:
        print(f"All {len(table_names)} assets have required tags.")
        return spark.createDataFrame([], "table_name STRING, fqn STRING, missing_tags STRING, existing_tags STRING")

# COMMAND ----------

# DBTITLE 1,Generate Governed Views
def generate_governed_views(catalog: str, schema: str, pii_columns_map: dict = None):
    """For tables tagged gov.identity.mode=required and gov.pii.level=high,
    generate v_{table_name}_governed views that exclude PII columns.

    Args:
        catalog: UC catalog name
        schema: UC schema name
        pii_columns_map: dict of table_name -> list of PII column names to exclude.
            Defaults to common PII columns if not provided.
    """
    if pii_columns_map is None:
        pii_columns_map = {
            "customers": ["customer_name", "email", "phone_number"],
        }

    # Find tables that need governed views
    try:
        tagged_tables = spark.sql(f"""
            SELECT DISTINCT table_name
            FROM {catalog}.information_schema.table_tags
            WHERE schema_name = '{schema}'
              AND tag_name = 'gov.identity.mode'
              AND tag_value = 'required'
        """).collect()
    except Exception as e:
        print(f"Cannot query tags: {e}")
        return

    # Also check PII level
    try:
        pii_tables = spark.sql(f"""
            SELECT DISTINCT table_name
            FROM {catalog}.information_schema.table_tags
            WHERE schema_name = '{schema}'
              AND tag_name = 'gov.pii.level'
              AND tag_value = 'high'
        """).collect()
        pii_table_set = {row.table_name for row in pii_tables}
    except Exception:
        pii_table_set = set()

    created = []
    for row in tagged_tables:
        tbl = row.table_name
        if tbl not in pii_table_set:
            continue

        pii_cols = pii_columns_map.get(tbl, [])
        if not pii_cols:
            print(f"  Skipping {tbl}: no PII columns configured in pii_columns_map")
            continue

        # Get all columns
        try:
            cols_df = spark.sql(f"DESCRIBE TABLE {catalog}.{schema}.{tbl}").collect()
            all_cols = [r.col_name for r in cols_df if not r.col_name.startswith("#")]
        except Exception as e:
            print(f"  Cannot describe {tbl}: {e}")
            continue

        # Filter out PII columns
        safe_cols = [c for c in all_cols if c not in pii_cols]
        if not safe_cols:
            print(f"  Skipping {tbl}: all columns are PII")
            continue

        view_name = f"v_{tbl}_governed"
        cols_str = ", ".join(safe_cols)
        try:
            spark.sql(f"""
                CREATE OR REPLACE VIEW {catalog}.{schema}.{view_name} AS
                SELECT {cols_str}
                FROM {catalog}.{schema}.{tbl}
            """)
            # Tag the governed view
            spark.sql(f"""
                ALTER VIEW {catalog}.{schema}.{view_name}
                SET TAGS ('gov.identity.mode' = 'required',
                          'gov.pii.level' = 'none',
                          'gov.projection.policy' = 'redacted')
            """)
            created.append({"source_table": tbl, "governed_view": view_name,
                            "excluded_columns": ", ".join(pii_cols)})
            print(f"  Created {view_name} (excluded: {pii_cols})")
        except Exception as e:
            print(f"  FAILED to create {view_name}: {e}")

    if created:
        return spark.createDataFrame(created)
    else:
        print("No governed views created.")
        return None

# COMMAND ----------

# DBTITLE 1,Export Tags to Delta
def export_tags_to_delta(catalog: str, schema: str, target_table: str = None):
    """Dump all gov.* tags to a Delta table for analytics.

    Args:
        catalog: UC catalog name
        schema: UC schema name
        target_table: full table name for output. Defaults to {catalog}.{schema}.gov_tags_export
    """
    if target_table is None:
        target_table = f"{catalog}.{schema}.gov_tags_export"

    try:
        tags_df = spark.sql(f"""
            SELECT
                catalog_name,
                schema_name,
                table_name,
                tag_name,
                tag_value,
                current_timestamp() as exported_at
            FROM {catalog}.information_schema.table_tags
            WHERE schema_name = '{schema}'
              AND tag_name LIKE 'gov.%'
            ORDER BY table_name, tag_name
        """)

        count = tags_df.count()
        tags_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        print(f"Exported {count} gov.* tags to {target_table}")
        return tags_df
    except Exception as e:
        print(f"Export failed: {e}")
        return None

# COMMAND ----------

# DBTITLE 1,Example: Bulk Tag the Accelerator Schema
# Apply baseline governance tags to all tables
baseline_tags = {
    "gov.identity.mode": "preferred",
    "gov.pii.level": "none",
    "gov.audit.required": "true",
    "gov.data.classification": "internal",
}
print("Applying baseline tags to all tables...")
bulk_apply_tags(catalog, schema, baseline_tags)

# COMMAND ----------

# Override for PII-containing tables
pii_tags = {
    "gov.identity.mode": "required",
    "gov.pii.level": "high",
    "gov.audit.required": "true",
    "gov.projection.policy": "redacted",
    "gov.data.classification": "confidential",
}
print("Applying PII tags to customers table...")
bulk_apply_tags(catalog, schema, pii_tags, table_names=["customers"])

# Write-back tables need user confirmation
write_tags = {
    "gov.identity.mode": "required",
    "gov.write.policy": "user_confirmed",
    "gov.audit.required": "true",
}
print("Applying write-policy tags to write-back tables...")
bulk_apply_tags(catalog, schema, write_tags, table_names=["billing_disputes", "billing_anomalies"])

# COMMAND ----------

# Validate coverage
print("Validating tag coverage...")
display(validate_tags(catalog, schema))

# COMMAND ----------

# Generate governed views
print("Generating governed views...")
generate_governed_views(catalog, schema)

# COMMAND ----------

# Export tags to Delta
print("Exporting tags to Delta...")
export_tags_to_delta(catalog, schema)
