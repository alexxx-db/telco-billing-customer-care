# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Bricks Deployment: Managed Read-Only Billing Intelligence
# MAGIC
# MAGIC This notebook deploys the **Agent Bricks tier** of the Customer Billing Accelerator —
# MAGIC a managed, read-only Supervisor Agent that routes queries to two specialized sub-agents:
# MAGIC
# MAGIC | Sub-Agent | Type | Capability |
# MAGIC |-----------|------|------------|
# MAGIC | **Billing FAQ** | Knowledge Assistant (KA) | Policy, how-to, payments, disputes (from FAQ docs) |
# MAGIC | **Billing Analytics** | Genie Space | Fleet-wide SQL analytics, trends, comparisons, top-N |
# MAGIC
# MAGIC ### What this tier IS
# MAGIC - A **managed, read-only** conversational analytics and FAQ interface
# MAGIC - Powered by Databricks Agent Bricks (KA + Genie + Supervisor)
# MAGIC - Zero custom code at inference time — fully platform-managed
# MAGIC - Ideal for demos, read-only deployments, and analyst self-service
# MAGIC
# MAGIC ### What this tier is NOT
# MAGIC This is **not** the full-capability agent. The following require the
# MAGIC **LangGraph deployment** (notebook 03):
# MAGIC - Individual customer account lookups
# MAGIC - Write-back operations (disputes, anomaly acknowledgement)
# MAGIC - ERP/finance profile access
# MAGIC - Persona-based access control and identity propagation
# MAGIC - Real-time streaming estimates
# MAGIC - Operational KPI tools
# MAGIC
# MAGIC > **Notebook 03 and 04 are alternative deployment tiers, not sequential steps.**
# MAGIC > Choose ONE based on your requirements.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC Run these notebooks first:
# MAGIC 1. `000-config` — Configuration
# MAGIC 2. `00_data_preparation` — Generate billing data
# MAGIC 3. `01_create_vector_search` — Create FAQ dataset
# MAGIC 4. `02_define_uc_tools` — Define UC functions (used by Genie, not directly by Agent Bricks)
# MAGIC 5. `03a_create_genie_space` — Create Genie Space (required — sets `genie_space_id`)

# COMMAND ----------

# DBTITLE 1,Install Dependencies
# MAGIC %pip install -U -qqqq databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Agent Bricks Helpers
import json
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import (
    KnowledgeAssistant,
    KnowledgeSource,
    FilesSpec,
)

w = WorkspaceClient()

MAX_WAIT = 600
POLL_INTERVAL = 15

# ---------------------------------------------------------------------------
# Agent Bricks REST API helpers
# The Agent Bricks platform exposes a tiles API for managing KA, Genie, and
# Supervisor resources.  The SDK may not have full coverage, so we use
# direct REST calls via w.api_client.do() as a fallback.
# ---------------------------------------------------------------------------

def _ab_api(method: str, path: str, body: dict = None):
    """Call the Agent Bricks REST API. Returns parsed JSON response."""
    url = f"/api/2.0/agent-bricks{path}"
    try:
        if body is not None:
            return w.api_client.do(method, url, body=body)
        return w.api_client.do(method, url)
    except Exception as e:
        raise RuntimeError(f"Agent Bricks API {method} {url} failed: {e}") from e


def find_tile(display_name: str, tile_type: str = None) -> dict:
    """Find an Agent Bricks tile by display_name. Returns tile dict or None."""
    try:
        resp = _ab_api("GET", "/tiles")
        tiles = resp.get("tiles", []) if isinstance(resp, dict) else []
        for tile in tiles:
            name_match = tile.get("display_name", "") == display_name
            type_match = (tile_type is None or
                          tile.get("tile_type", "").upper() == tile_type.upper())
            if name_match and type_match:
                return tile
    except Exception as e:
        print(f"  Could not search tiles: {e}")
    return None


def wait_for_tile(tile_id: str, label: str = "tile",
                  max_wait: int = MAX_WAIT, poll_interval: int = POLL_INTERVAL) -> dict:
    """Poll an Agent Bricks tile until its endpoint is ready."""
    print(f"Waiting for {label} to provision (tile {tile_id})...")
    elapsed = 0
    while elapsed < max_wait:
        try:
            tile = _ab_api("GET", f"/tiles/{tile_id}")
            status = (tile.get("endpoint_status")
                      or tile.get("state")
                      or "UNKNOWN")
            print(f"  {label}: {status} ({elapsed}s)")
            if status.upper() in ("ONLINE", "ACTIVE", "READY"):
                ep = tile.get("endpoint_name", "")
                print(f"  {label} is ready! Endpoint: {ep}")
                return tile
            if status.upper() in ("FAILED", "ERROR", "DELETED"):
                print(f"  ERROR: {label} provisioning failed.")
                return tile
        except Exception as e:
            print(f"  Polling error: {e}")
        time.sleep(poll_interval)
        elapsed += poll_interval
    print(f"  WARNING: {label} did not provision within {max_wait}s. Check the Agent Bricks UI.")
    return None


def find_ka_by_name(display_name: str):
    """Find a Knowledge Assistant by display_name using SDK."""
    try:
        for ka in w.knowledge_assistants.list_knowledge_assistants():
            if ka.display_name == display_name:
                return ka
    except Exception:
        pass
    return None


def find_genie_space_by_name(space_name: str) -> str:
    """Find a Genie Space by title. Returns space_id or None."""
    try:
        resp = w.genie.list_spaces()
        if hasattr(resp, 'spaces') and resp.spaces:
            for s in resp.spaces:
                if s.title == space_name:
                    return s.space_id
    except Exception:
        pass
    return None


def query_endpoint(endpoint_name: str, question: str) -> str:
    """Send a test question to a serving endpoint. Returns response text."""
    try:
        response = w.serving_endpoints.query(
            name=endpoint_name,
            input={"messages": [{"role": "user", "content": question}]},
        )
        if hasattr(response, 'choices') and response.choices:
            return response.choices[0].message.content
        elif hasattr(response, 'messages') and response.messages:
            return response.messages[-1].content
        return str(response)
    except Exception as e:
        return f"ERROR: {e}"


print("Agent Bricks helpers loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare FAQ Documents for Knowledge Assistant
# MAGIC
# MAGIC The Knowledge Assistant indexes documents from a Unity Catalog Volume.
# MAGIC We write billing FAQ entries as individual text files with companion
# MAGIC JSON evaluation files (question/guideline pairs).

# COMMAND ----------

# DBTITLE 1,Create Volume and Write FAQ Documents
catalog = config['catalog']
db = config['database']
volume_path = config['ka_volume_path']

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{db}.billing_faq_docs")

faq_df = spark.table(f"{catalog}.{db}.billing_faq_dataset").collect()

for row in faq_df:
    idx = row['index']
    faq_text = row['faq']

    if "Q:" in faq_text and "A:" in faq_text:
        parts = faq_text.split("A:", 1)
        question = parts[0].replace("Q:", "").strip()
        answer = parts[1].strip()
    else:
        question = f"FAQ {idx}"
        answer = faq_text

    doc_content = f"# Billing FAQ {idx}\n\n**Question:** {question}\n\n**Answer:** {answer}\n"
    file_path = f"{volume_path}/faq_{idx:03d}.txt"
    dbutils.fs.put(file_path.replace("/Volumes/", "dbfs:/Volumes/"), doc_content, True)

    example = {"question": question, "guideline": f"Should answer using FAQ {idx}: {answer[:100]}..."}
    json_path = f"{volume_path}/faq_{idx:03d}.json"
    dbutils.fs.put(json_path.replace("/Volumes/", "dbfs:/Volumes/"), json.dumps(example), True)

print(f"Wrote {len(faq_df)} FAQ documents + JSON examples to {volume_path}")

# COMMAND ----------

# DBTITLE 1,Verify Volume Contents
files = dbutils.fs.ls(volume_path.replace("/Volumes/", "dbfs:/Volumes/"))
txt_count = sum(1 for f in files if f.name.endswith('.txt'))
json_count = sum(1 for f in files if f.name.endswith('.json'))
print(f"Volume contents: {txt_count} FAQ docs, {json_count} evaluation files, {len(files)} total")
assert txt_count > 0, "No FAQ documents found in volume. Check data preparation."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Knowledge Assistant
# MAGIC
# MAGIC Create a Knowledge Assistant that indexes the billing FAQ documents.
# MAGIC Uses the Databricks SDK for KA creation and knowledge source management.

# COMMAND ----------

# DBTITLE 1,Create or Update Knowledge Assistant
ka_name = config['ka_name']
ka_volume_path = config['ka_volume_path']

existing_ka = find_ka_by_name(ka_name)

if existing_ka:
    ka_id = existing_ka.id
    ka_resource_name = existing_ka.name
    ka_endpoint_name = existing_ka.endpoint_name or "(provisioning)"
    print(f"Found existing KA '{ka_name}':")
    print(f"  ID:       {ka_id}")
    print(f"  Endpoint: {ka_endpoint_name}")
else:
    created = w.knowledge_assistants.create_knowledge_assistant(
        knowledge_assistant=KnowledgeAssistant(
            display_name=ka_name,
            description=config['ka_description'],
            instructions=config['ka_instructions'],
        )
    )
    ka_id = created.id
    ka_resource_name = created.name
    ka_endpoint_name = None
    print(f"Created new KA '{ka_name}': {ka_id}")

# Ensure knowledge source (UC Volume) is attached
existing_sources = list(w.knowledge_assistants.list_knowledge_sources(ka_resource_name))
has_volume = any(s.files and s.files.path == ka_volume_path for s in existing_sources)

if not has_volume:
    ks = w.knowledge_assistants.create_knowledge_source(
        parent=ka_resource_name,
        knowledge_source=KnowledgeSource(
            display_name="Billing FAQ Documents",
            description="FAQ documents covering billing questions, payment methods, and plan details.",
            source_type="files",
            files=FilesSpec(path=ka_volume_path),
        ),
    )
    print(f"Added knowledge source: {ks.id}")
    w.knowledge_assistants.sync_knowledge_sources(ka_resource_name)
    print("Triggered knowledge source sync.")
else:
    print("Knowledge source already attached.")

# COMMAND ----------

# DBTITLE 1,Wait for KA Endpoint
ka_result = None
if not ka_endpoint_name or ka_endpoint_name == "(provisioning)":
    print("Waiting for KA endpoint to provision...")
    elapsed = 0
    while elapsed < MAX_WAIT:
        try:
            ka = w.knowledge_assistants.get_knowledge_assistant(ka_resource_name)
            state = ka.state.value if ka.state else "UNKNOWN"
            print(f"  KA state: {state} ({elapsed}s)")
            if state in ("ONLINE", "ACTIVE"):
                ka_endpoint_name = ka.endpoint_name
                print(f"  KA ready! Endpoint: {ka_endpoint_name}")
                break
            if state in ("FAILED", "ERROR"):
                print(f"  ERROR: KA provisioning failed: {ka.error_info}")
                break
        except Exception as e:
            print(f"  Error: {e}")
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
    else:
        # Try one more time — KA may have been ready before polling started
        ka = w.knowledge_assistants.get_knowledge_assistant(ka_resource_name)
        ka_endpoint_name = ka.endpoint_name
        if ka_endpoint_name:
            print(f"  KA endpoint (from existing): {ka_endpoint_name}")
        else:
            print(f"  WARNING: KA endpoint not available after {MAX_WAIT}s.")

# Resolve KA tile_id for Supervisor creation
ka_tile_id = None
ka_tile = find_tile(ka_name, "KNOWLEDGE_ASSISTANT")
if ka_tile:
    ka_tile_id = ka_tile.get("tile_id", ka_tile.get("id"))
    print(f"KA tile_id: {ka_tile_id}")
else:
    # tile_id may equal ka_id in some API versions
    ka_tile_id = ka_id
    print(f"KA tile_id (from SDK): {ka_tile_id}")

config['ka_tile_id'] = ka_tile_id
config['ka_endpoint_name'] = ka_endpoint_name
assert ka_endpoint_name, "KA endpoint not available. Cannot proceed."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Supervisor Agent
# MAGIC
# MAGIC Create a Supervisor Agent (MAS) that routes queries between the
# MAGIC FAQ Knowledge Assistant and the Billing Analytics Genie Space.

# COMMAND ----------

# DBTITLE 1,Verify Genie Space
genie_space_id = config.get('genie_space_id')

if not genie_space_id:
    genie_space_name = config.get('genie_space_name', '')
    if genie_space_name:
        genie_space_id = find_genie_space_by_name(genie_space_name)
        if genie_space_id:
            config['genie_space_id'] = genie_space_id
            print(f"Found Genie Space '{genie_space_name}': {genie_space_id}")

if not genie_space_id:
    raise ValueError(
        "genie_space_id is not set and could not be found by name. "
        "Run notebook 03a_create_genie_space first."
    )

print(f"Genie Space ID:    {genie_space_id}")
print(f"KA Tile ID:        {ka_tile_id}")
print(f"KA Endpoint:       {ka_endpoint_name}")
print("\nAll prerequisites verified. Creating Supervisor Agent...")

# COMMAND ----------

# DBTITLE 1,Supervisor Routing Instructions
# These instructions are injected into the Supervisor Agent and control
# how it routes queries between sub-agents.  They explicitly document
# what this tier CAN and CANNOT do.

MAS_INSTRUCTIONS = """You are a Billing Support Supervisor that routes queries to two specialized agents:

**billing_faq_agent**: Answers general billing questions using FAQ documentation — policies, how-to guides, payment methods, due dates, disputes, refunds, late fees, and plan details.

**billing_analytics_agent**: Runs SQL analytics across the entire billing dataset — charge trends, plan comparisons, customer segmentation, top-N rankings, revenue analysis, anomaly summaries, and month-over-month analysis.

## Routing Rules
1. Policy, how-to, or FAQ questions -> billing_faq_agent
2. Data analysis, trends, aggregations, comparisons, top-N -> billing_analytics_agent
3. Questions needing BOTH explanation AND data -> billing_faq_agent first for the explanation, then billing_analytics_agent for the data; synthesize into one answer

## Unsupported Capabilities
This system provides fleet-wide analytics and FAQ answers only. It does NOT support:
- Individual customer account lookups (e.g., "show charges for customer 4401")
- Write operations (creating disputes, acknowledging anomalies, updating statuses)
- Customer-specific ERP, credit, or payment profile data
- Real-time streaming billing estimates
- Operational platform KPIs (DBU costs, job reliability)

If a user asks for any of these, respond helpfully:
"I can help with general billing questions and fleet-wide analytics. For individual customer account lookups, billing disputes, or account-specific operations, the full Billing Agent deployment (LangGraph tier) has dedicated tools for those tasks."

Then suggest an alternative that IS supported. For example:
- "Would you like to see fleet-wide billing trends or plan comparisons instead?"
- "I can show you aggregate anomaly statistics across all customers."
- "I can explain our dispute resolution policy from the FAQ."

## Response Style
- Start with a direct 2-3 sentence answer
- Follow with supporting detail or a compact table if appropriate
- State any assumptions, timeframes, or filters applied
- If information is unavailable, say so clearly"""

print("Routing instructions prepared.")
print(f"Length: {len(MAS_INSTRUCTIONS)} chars")

# COMMAND ----------

# DBTITLE 1,Demo Examples for Supervisor
MAS_EXAMPLES = [
    # FAQ routing
    {"question": "How is my bill calculated?",
     "guideline": "Route to billing_faq_agent. Explain plan fee + usage charges + taxes."},
    {"question": "How do I set up autopay?",
     "guideline": "Route to billing_faq_agent. Step-by-step autopay setup."},
    {"question": "Can I change my bill due date?",
     "guideline": "Route to billing_faq_agent. Explain due date change policy."},
    {"question": "What happens if I pay my bill late?",
     "guideline": "Route to billing_faq_agent. Explain late fee policy and grace period."},
    # Analytics routing
    {"question": "What is the average monthly charge across all billing plans?",
     "guideline": "Route to billing_analytics_agent. Return aggregated charge data by plan."},
    {"question": "Which billing plan has the highest roaming charges?",
     "guideline": "Route to billing_analytics_agent. Compare roaming charges across plans."},
    {"question": "What are the top 10 customers by total charges this quarter?",
     "guideline": "Route to billing_analytics_agent. Top-N ranking query."},
    {"question": "How many billing anomalies were detected by type last month?",
     "guideline": "Route to billing_analytics_agent. Anomaly count aggregation."},
    {"question": "What is total billed revenue vs ERP recognized revenue for the last 3 months?",
     "guideline": "Route to billing_analytics_agent. Revenue reconciliation query."},
    # Hybrid routing
    {"question": "Why is the average bill higher than usual and what does that include?",
     "guideline": "Chain: billing_faq_agent for explanation of bill components, then billing_analytics_agent for current average. Synthesize."},
    # Unsupported — graceful handling
    {"question": "Show me the charges for customer 4401",
     "guideline": "Politely decline. Individual customer lookups are not available in this tier. Suggest fleet-wide analytics or the LangGraph deployment."},
    {"question": "Create a billing dispute for customer 4401",
     "guideline": "Politely decline. Write operations are not available in this tier. Explain the dispute policy from FAQ and direct to the LangGraph deployment."},
]

print(f"Prepared {len(MAS_EXAMPLES)} demo/evaluation examples.")

# COMMAND ----------

# DBTITLE 1,Create or Update Supervisor Agent
mas_name = config['mas_name']
mas_tile_id = None
mas_endpoint_name = None

# Check if Supervisor already exists
existing_mas = find_tile(mas_name, "SUPERVISOR_AGENT")
if not existing_mas:
    # Also try without type filter (tile_type may vary across API versions)
    existing_mas = find_tile(mas_name)

if existing_mas:
    mas_tile_id = existing_mas.get("tile_id", existing_mas.get("id"))
    mas_endpoint_name = existing_mas.get("endpoint_name")
    print(f"Found existing Supervisor '{mas_name}':")
    print(f"  Tile ID:  {mas_tile_id}")
    print(f"  Endpoint: {mas_endpoint_name}")
    print("Updating configuration...")
    operation = "update"
else:
    operation = "create"
    print(f"Creating new Supervisor Agent: {mas_name}")

# Build the Supervisor Agent configuration
mas_agents = [
    {
        "name": "billing_faq_agent",
        "description": (
            "Answers frequently asked billing questions using indexed FAQ documents. "
            "Covers bill calculation, payment methods, autopay, disputes, refunds, "
            "late fees, data usage, roaming charges, and due date changes."
        ),
        "ka_tile_id": str(ka_tile_id),
    },
    {
        "name": "billing_analytics_agent",
        "description": (
            "Runs SQL analytics on billing data: charge trends, plan comparisons, "
            "customer segmentation, top-N rankings, month-over-month analysis, "
            "anomaly summaries, and revenue reconciliation across the full billing dataset."
        ),
        "genie_space_id": str(genie_space_id),
    },
]

mas_payload = {
    "display_name": mas_name,
    "description": config['mas_description'],
    "agents": mas_agents,
    "instructions": MAS_INSTRUCTIONS,
    "examples": MAS_EXAMPLES,
}

# Attempt programmatic creation via Agent Bricks REST API
try:
    if operation == "update" and mas_tile_id:
        result = _ab_api("PATCH", f"/tiles/{mas_tile_id}", body=mas_payload)
        print(f"Updated Supervisor Agent: {mas_tile_id}")
    else:
        mas_payload["tile_type"] = "SUPERVISOR_AGENT"
        result = _ab_api("POST", "/tiles", body=mas_payload)
        mas_tile_id = result.get("tile_id", result.get("id"))
        mas_endpoint_name = result.get("endpoint_name")
        print(f"Created Supervisor Agent: {mas_tile_id}")

    if isinstance(result, dict):
        mas_tile_id = mas_tile_id or result.get("tile_id", result.get("id"))
        mas_endpoint_name = mas_endpoint_name or result.get("endpoint_name")

    _PROGRAMMATIC_SUCCESS = True

except Exception as e:
    _PROGRAMMATIC_SUCCESS = False
    print(f"\nProgrammatic creation not available: {e}")
    print("\n" + "=" * 70)
    print("  MANUAL STEP REQUIRED: Create Supervisor Agent via the UI")
    print("=" * 70)
    host = w.config.host
    print(f"""
1. Open the Agent Bricks UI:
   {host}/#/agents

2. Click 'Supervisor Agent' -> 'Build'

3. Configure:
   Name:        {mas_name}
   Description: {config['mas_description'][:100]}...

4. Add sub-agents:
   a) Knowledge Assistant -> select '{ka_name}'
      (endpoint: {ka_endpoint_name})
   b) Genie Space -> select ID: {genie_space_id}

5. Paste these routing instructions (copy the block below):
""")
    print("--- BEGIN INSTRUCTIONS ---")
    print(MAS_INSTRUCTIONS)
    print("--- END INSTRUCTIONS ---")
    print(f"""
6. Add example questions from the list above.

7. Click 'Create' and wait for the endpoint to provision.

8. After creation, re-run the next cell to detect the Supervisor.
""")
    print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Detect Supervisor Agent (after manual creation if needed)
if not mas_tile_id or not mas_endpoint_name:
    print("Searching for Supervisor Agent by name...")
    found = find_tile(mas_name)
    if found:
        mas_tile_id = found.get("tile_id", found.get("id"))
        mas_endpoint_name = found.get("endpoint_name")
        print(f"Found: tile_id={mas_tile_id}, endpoint={mas_endpoint_name}")
    else:
        # Try listing all serving endpoints matching the MAS pattern
        try:
            for ep in w.serving_endpoints.list():
                if mas_name.lower().replace(" ", "_") in (ep.name or "").lower():
                    mas_endpoint_name = ep.name
                    print(f"Found serving endpoint: {mas_endpoint_name}")
                    break
        except Exception:
            pass

if not mas_endpoint_name:
    print("\nWARNING: Supervisor Agent endpoint not found.")
    print("If you created it via the UI, it may still be provisioning.")
    print("You can set the endpoint name manually:")
    print('  mas_endpoint_name = "your-endpoint-name"')
    print("Then re-run the cells below.")
else:
    print(f"\nSupervisor Agent ready:")
    print(f"  Tile ID:  {mas_tile_id}")
    print(f"  Endpoint: {mas_endpoint_name}")

config['mas_tile_id'] = mas_tile_id

# COMMAND ----------

# DBTITLE 1,Wait for MAS Endpoint (if just created)
if mas_tile_id and _PROGRAMMATIC_SUCCESS:
    tile_result = wait_for_tile(str(mas_tile_id), label="Supervisor Agent")
    if tile_result:
        mas_endpoint_name = (tile_result.get("endpoint_name")
                             or mas_endpoint_name)
elif mas_endpoint_name:
    print(f"Supervisor endpoint already known: {mas_endpoint_name}")
else:
    print("Skipping wait — no tile_id available. Set mas_endpoint_name manually if needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Validate Deployment
# MAGIC
# MAGIC Run validation queries to confirm both sub-agents respond correctly
# MAGIC and that unsupported requests are handled gracefully.

# COMMAND ----------

# DBTITLE 1,Validation: FAQ Routing
if not mas_endpoint_name:
    print("SKIP: No MAS endpoint available. Set mas_endpoint_name and re-run.")
else:
    print("=" * 60)
    print("TEST 1: FAQ Routing")
    print("=" * 60)
    faq_q = "How do I set up autopay for my bill?"
    print(f"Q: {faq_q}")
    answer = query_endpoint(mas_endpoint_name, faq_q)
    print(f"A: {answer[:600]}")
    faq_pass = "ERROR" not in answer
    print(f"\nResult: {'PASS' if faq_pass else 'FAIL'}")

# COMMAND ----------

# DBTITLE 1,Validation: Analytics Routing
if not mas_endpoint_name:
    print("SKIP: No MAS endpoint available.")
else:
    print("=" * 60)
    print("TEST 2: Analytics Routing")
    print("=" * 60)
    analytics_q = "What is the average monthly total charge across all billing plans?"
    print(f"Q: {analytics_q}")
    answer = query_endpoint(mas_endpoint_name, analytics_q)
    print(f"A: {answer[:600]}")
    analytics_pass = "ERROR" not in answer
    print(f"\nResult: {'PASS' if analytics_pass else 'FAIL'}")

# COMMAND ----------

# DBTITLE 1,Validation: Unsupported Request Handling
if not mas_endpoint_name:
    print("SKIP: No MAS endpoint available.")
else:
    print("=" * 60)
    print("TEST 3: Unsupported Request (Individual Customer Lookup)")
    print("=" * 60)
    unsupported_q = "Show me the billing details for customer 4401"
    print(f"Q: {unsupported_q}")
    answer = query_endpoint(mas_endpoint_name, unsupported_q)
    print(f"A: {answer[:600]}")
    # Check that the response acknowledges the limitation
    lower_answer = answer.lower()
    graceful = any(phrase in lower_answer for phrase in [
        "not available", "not support", "fleet-wide", "full",
        "individual", "langraph", "dedicated", "cannot",
        "account lookup", "analytics", "instead",
    ])
    print(f"\nGraceful decline: {'YES' if graceful else 'NO (review routing instructions)'}")
    print(f"Result: {'PASS' if graceful else 'REVIEW'}")

# COMMAND ----------

# DBTITLE 1,Validation: Write Request Handling
if not mas_endpoint_name:
    print("SKIP: No MAS endpoint available.")
else:
    print("=" * 60)
    print("TEST 4: Unsupported Request (Write Operation)")
    print("=" * 60)
    write_q = "Create a billing dispute for customer 4401 for overcharges"
    print(f"Q: {write_q}")
    answer = query_endpoint(mas_endpoint_name, write_q)
    print(f"A: {answer[:600]}")
    lower_answer = answer.lower()
    graceful = any(phrase in lower_answer for phrase in [
        "not available", "not support", "write", "dispute",
        "cannot", "full", "langraph", "read-only",
    ])
    print(f"\nGraceful decline: {'YES' if graceful else 'NO (review routing instructions)'}")
    print(f"Result: {'PASS' if graceful else 'REVIEW'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Demo Scenarios
# MAGIC
# MAGIC Ready-to-use demo prompts organized by scenario. Use these for
# MAGIC customer workshops, field demos, or technical validation.
# MAGIC
# MAGIC ### Demo Bundle 1: Executive Demo (5 prompts)
# MAGIC Focus: Business value of conversational billing intelligence
# MAGIC
# MAGIC | # | Prompt | Expected Route | Purpose |
# MAGIC |---|--------|----------------|---------|
# MAGIC | 1 | "How is a customer's monthly bill calculated?" | FAQ | Show policy retrieval |
# MAGIC | 2 | "What is the average monthly charge across all plans?" | Analytics | Show fleet-wide analytics |
# MAGIC | 3 | "Which plans have the highest roaming charges?" | Analytics | Show plan comparison |
# MAGIC | 4 | "How many billing anomalies were detected last month?" | Analytics | Show anomaly monitoring |
# MAGIC | 5 | "Show charges for customer 4401" | Graceful decline | Show honest scope boundaries |
# MAGIC
# MAGIC ### Demo Bundle 2: Field SE Demo (8 prompts)
# MAGIC Focus: Technical capabilities and routing intelligence
# MAGIC
# MAGIC | # | Prompt | Expected Route | Purpose |
# MAGIC |---|--------|----------------|---------|
# MAGIC | 1 | "What payment methods do you accept?" | FAQ | Basic FAQ retrieval |
# MAGIC | 2 | "Can I change my bill due date?" | FAQ | Policy question |
# MAGIC | 3 | "What are the top 10 customers by total charges this quarter?" | Analytics | Top-N ranking |
# MAGIC | 4 | "Compare total charges between 12-month and 24-month contract plans" | Analytics | Plan comparison with filter |
# MAGIC | 5 | "What is total billed revenue vs ERP recognized revenue?" | Analytics | Revenue reconciliation |
# MAGIC | 6 | "Why is the average bill higher than usual and what does it include?" | Both (hybrid) | Show multi-agent chaining |
# MAGIC | 7 | "Create a billing dispute for customer 4401" | Graceful decline | Show write-op boundary |
# MAGIC | 8 | "What is the customer ERP credit profile for customer 4401?" | Graceful decline | Show capability boundary |
# MAGIC
# MAGIC ### Demo Bundle 3: Customer Read-Only Demo (5 prompts)
# MAGIC Focus: Self-service analytics for business users
# MAGIC
# MAGIC | # | Prompt | Expected Route | Purpose |
# MAGIC |---|--------|----------------|---------|
# MAGIC | 1 | "What are the most common billing questions?" | FAQ | Meta-FAQ query |
# MAGIC | 2 | "How do I dispute a charge?" | FAQ | Process question |
# MAGIC | 3 | "What is the average data overage charge by plan?" | Analytics | Specific charge analysis |
# MAGIC | 4 | "Show me the monthly charge trend for the last 6 months" | Analytics | Trend analysis |
# MAGIC | 5 | "How many customers are on each billing plan?" | Analytics | Segmentation |

# COMMAND ----------

# DBTITLE 1,Run Executive Demo
if not mas_endpoint_name:
    print("SKIP: No MAS endpoint. Set mas_endpoint_name and re-run.")
else:
    exec_demo = [
        "How is a customer's monthly bill calculated?",
        "What is the average monthly charge across all plans?",
        "Which plans have the highest roaming charges?",
        "How many billing anomalies were detected last month?",
        "Show charges for customer 4401",
    ]
    print("=" * 60)
    print("EXECUTIVE DEMO")
    print("=" * 60)
    for i, q in enumerate(exec_demo, 1):
        print(f"\n--- Prompt {i}/{len(exec_demo)} ---")
        print(f"Q: {q}")
        a = query_endpoint(mas_endpoint_name, q)
        # Truncate long analytics responses for demo readability
        display_len = 400 if "customer 4401" in q.lower() else 600
        print(f"A: {a[:display_len]}{'...' if len(a) > display_len else ''}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployment Summary

# COMMAND ----------

# DBTITLE 1,Summary
host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()

print("=" * 70)
print("  AGENT BRICKS DEPLOYMENT SUMMARY")
print("  Tier: Managed Read-Only (FAQ + Analytics)")
print("=" * 70)

print(f"""
Knowledge Assistant (FAQ):
  Name:     {ka_name}
  Tile ID:  {ka_tile_id or '(unknown)'}
  Endpoint: {ka_endpoint_name or '(not available)'}
  Volume:   {config['ka_volume_path']}

Genie Space (Analytics):
  Space ID: {genie_space_id}
  URL:      https://{host}/genie/spaces/{genie_space_id}

Supervisor Agent:
  Name:     {mas_name}
  Tile ID:  {mas_tile_id or '(unknown)'}
  Endpoint: {mas_endpoint_name or '(not available)'}
""")

print("SUPPORTED CAPABILITIES:")
print("  [x] FAQ retrieval (billing policy, how-to, payments)")
print("  [x] Fleet-wide billing analytics (trends, comparisons, top-N)")
print("  [x] Anomaly summary analytics")
print("  [x] Revenue and finance analytics")
print("  [x] Multi-agent routing (FAQ + Analytics)")
print()
print("NOT SUPPORTED IN THIS TIER:")
print("  [ ] Individual customer account lookups")
print("  [ ] Write operations (disputes, anomaly acknowledgement)")
print("  [ ] ERP/credit profile access")
print("  [ ] Identity propagation and persona filtering")
print("  [ ] Real-time streaming estimates")
print("  [ ] Operational platform KPIs")
print()
print("For full capabilities, deploy the LangGraph agent (notebook 03).")

if mas_endpoint_name:
    print(f"\nTo use in the Dash app, set SERVING_ENDPOINT to: {mas_endpoint_name}")
print("=" * 70)
