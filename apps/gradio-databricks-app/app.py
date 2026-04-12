"""
Customer Billing Accelerator — Gradio Databricks App.

Multi-workspace billing intelligence application with persona-aware chat,
analytics exploration, operational workflows, and governed identity propagation.

Deployed as a Databricks App. Talks to the LangGraph agent via Model Serving.
"""

import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Optional

import gradio as gr

# ---------------------------------------------------------------------------
# Shared identity and serving client
# ---------------------------------------------------------------------------
# Add shared module to path (deployed alongside app via DAB or manual copy)
_app_dir = os.path.dirname(os.path.abspath(__file__))
_shared_dir = os.path.join(os.path.dirname(_app_dir), "shared")
if os.path.isdir(_shared_dir) and _shared_dir not in sys.path:
    sys.path.insert(0, _shared_dir)

try:
    from serving_client import (
        query_serving_endpoint, build_request_context, get_user_info,
        PERSONAS,
    )
    _IDENTITY_AVAILABLE = True
except ImportError:
    _IDENTITY_AVAILABLE = False
    PERSONAS = {
        "customer_care": {"label": "Customer Support", "description": "General billing help.", "icon": "👤"},
        "finance_ops": {"label": "Finance & Analytics", "description": "Fleet-wide analytics.", "icon": "📊"},
        "executive": {"label": "Executive View", "description": "Leadership summaries.", "icon": "🎯"},
        "technical": {"label": "Platform Engineering", "description": "Platform diagnostics.", "icon": "⚙️"},
    }

# ---------------------------------------------------------------------------
# Logging and configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("billing_app")

SERVING_ENDPOINT = os.getenv("SERVING_ENDPOINT_NAME", os.getenv("SERVING_ENDPOINT", ""))
APP_PORT = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("APP_PORT", "8000")))
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
SESSION_ID = str(uuid.uuid4())

# ---------------------------------------------------------------------------
# Identity helpers for Gradio (mirrors Dash app pattern)
# ---------------------------------------------------------------------------

def _get_user_token() -> Optional[str]:
    """Extract user OAuth token from Databricks App proxy headers.

    In Gradio, the request headers are accessible via gr.Request in event handlers.
    For server-side calls, we check the environment as fallback.
    """
    return os.getenv("DATABRICKS_TOKEN", None)


def _get_workspace_host() -> str:
    """Get the workspace hostname."""
    host = os.getenv("DATABRICKS_HOST", "")
    if host:
        return host.replace("https://", "").rstrip("/")
    try:
        from databricks.sdk.core import Config
        return Config().host.replace("https://", "").rstrip("/")
    except Exception:
        return ""


def _resolve_user(request: gr.Request = None) -> dict:
    """Resolve the current user from Databricks App proxy headers."""
    user_info = {"email": "unknown", "groups": [], "display_name": "User"}

    if not _IDENTITY_AVAILABLE:
        return user_info

    # Try to get token from Gradio request headers
    token = None
    host = _get_workspace_host()

    if request:
        token = request.headers.get("x-forwarded-access-token")
        host = host or request.headers.get("host", "")

    if not token:
        token = _get_user_token()

    if token and host:
        try:
            user_info = get_user_info(token, host)
        except Exception as e:
            log.warning(f"SCIM user resolution failed: {e}")

    return user_info


# ---------------------------------------------------------------------------
# Chat handler
# ---------------------------------------------------------------------------

def chat_respond(message: str, history: list, persona: str,
                 request: gr.Request = None) -> str:
    """Send a message to the billing agent and return the response."""
    if not SERVING_ENDPOINT:
        return ("Serving endpoint not configured. Set `SERVING_ENDPOINT_NAME` "
                "in app.yaml to connect to the deployed billing agent.")

    if not message.strip():
        return ""

    # Build message history for the agent
    messages = []
    for user_msg, bot_msg in history:
        if user_msg:
            messages.append({"role": "user", "content": user_msg})
        if bot_msg:
            messages.append({"role": "assistant", "content": bot_msg})
    messages.append({"role": "user", "content": message})

    # Extract identity from request headers
    user_token = None
    workspace_host = _get_workspace_host()

    if request:
        user_token = request.headers.get("x-forwarded-access-token")
        workspace_host = workspace_host or request.headers.get("host", "")

    if not user_token:
        user_token = _get_user_token()

    try:
        if _IDENTITY_AVAILABLE:
            result = query_serving_endpoint(
                endpoint_name=SERVING_ENDPOINT,
                messages=messages,
                persona=persona,
                user_token=user_token,
                workspace_host=workspace_host,
                session_id=SESSION_ID,
            )
            return result.get("content", str(result))
        else:
            # Fallback without identity module
            from mlflow.deployments import get_deploy_client
            res = get_deploy_client("databricks").predict(
                endpoint=SERVING_ENDPOINT,
                inputs={
                    "messages": messages,
                    "max_tokens": 1024,
                    "custom_inputs": {"persona": persona},
                },
            )
            if "messages" in res:
                return res["messages"][-1].get("content", "")
            elif "choices" in res:
                return res["choices"][0]["message"].get("content", "")
            return str(res)
    except Exception as e:
        log.error(f"Chat error: {e}")
        return f"Error communicating with the billing agent: {e}"


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

def health_check(request: gr.Request = None) -> str:
    """Return a detailed health and configuration report."""
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    mode = "Databricks App" if os.getenv("DATABRICKS_APP_PORT") else "Local"

    user = _resolve_user(request)

    lines = [
        "STATUS: OK",
        f"Timestamp:     {now}",
        f"Mode:          {mode}",
        f"Session ID:    {SESSION_ID}",
        "",
        "--- User Identity ---",
        f"Email:         {user.get('email', 'unknown')}",
        f"Display Name:  {user.get('display_name', 'unknown')}",
        f"Groups:        {', '.join(user.get('groups', [])) or 'none resolved'}",
        f"Identity SDK:  {'available' if _IDENTITY_AVAILABLE else 'NOT AVAILABLE'}",
        "",
        "--- Serving Endpoint ---",
        f"Endpoint:      {SERVING_ENDPOINT or 'NOT CONFIGURED'}",
        "",
        "--- Databricks Resources ---",
        f"Workspace:     {_get_workspace_host() or 'not resolved'}",
        f"Lakebase PG:   {'configured' if os.getenv('PGHOST') else 'not configured'}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Sample prompts for each workspace
# ---------------------------------------------------------------------------

CHAT_SAMPLES = {
    "customer_care": [
        "How is my monthly bill calculated?",
        "What are the charges for customer 4401?",
        "How do I set up autopay?",
        "Are there any billing anomalies for customer 4401?",
        "Create a billing dispute for customer 4401 for overcharges",
    ],
    "finance_ops": [
        "What is total billing revenue this month vs last month?",
        "How many CRITICAL anomalies are unacknowledged right now?",
        "Show me the OPEX ratio trend for the last 6 months.",
        "Which customer segments have the highest overdue AR ratio?",
    ],
    "executive": [
        "Give me a billing health summary for this month.",
        "What are the top billing risks I should know about?",
        "What is our anomaly exposure this quarter?",
    ],
    "technical": [
        "Is the billing anomaly detection pipeline healthy?",
        "What is our DBU consumption and estimated cost this week?",
        "Show me job reliability for billing pipelines in the last 30 days.",
    ],
}

ANALYTICS_SAMPLES = [
    "What is the average monthly charge across all billing plans?",
    "Which billing plan has the highest roaming charges?",
    "What are the top 10 customers by total charges this quarter?",
    "How many billing anomalies were detected by type last month?",
    "What is total billed revenue vs ERP recognized revenue for the last 3 months?",
    "Compare total charges between 12-month and 24-month contract plans.",
    "Show me the monthly charge trend for the last 6 months.",
    "What is the average data overage charge by plan?",
]


# ---------------------------------------------------------------------------
# Build the Gradio UI
# ---------------------------------------------------------------------------

def build_app() -> gr.Blocks:
    theme = gr.themes.Soft(primary_hue="blue", secondary_hue="slate")

    with gr.Blocks(theme=theme, title="Billing Intelligence") as app:

        # ── Header ──────────────────────────────────────────────────
        gr.Markdown("""
# Customer Billing Intelligence
**Databricks-native billing support powered by LangGraph, Genie Spaces, and Agent Bricks.**

Select a workspace below to explore billing FAQ, fleet analytics, customer support, or platform health.
        """)

        # ── Persona selector (shared across tabs) ──────────────────
        with gr.Row():
            persona_dd = gr.Dropdown(
                choices=[(f"{v['icon']} {v['label']}", k) for k, v in PERSONAS.items()],
                value="customer_care",
                label="Active Persona",
                interactive=True,
                scale=2,
            )
            persona_desc = gr.Textbox(
                value=PERSONAS["customer_care"]["description"],
                label="Persona Description",
                interactive=False,
                scale=3,
            )

        def update_persona_desc(persona):
            return PERSONAS.get(persona, {}).get("description", "")

        persona_dd.change(fn=update_persona_desc, inputs=persona_dd, outputs=persona_desc)

        # ── Tab 1: Chat Workspace ───────────────────────────────────
        with gr.Tab("Chat", id="chat"):
            gr.Markdown("""
### Billing Agent Chat
Converse with the full-capability LangGraph billing agent. Supports FAQ, customer lookups,
analytics delegation to Genie, dispute creation, anomaly acknowledgement, and more.

*Persona selection controls which tools are available and how the agent responds.*
            """)

            chatbot = gr.Chatbot(
                label="Billing Agent",
                height=450,
                show_copy_button=True,
                type="tuples",
            )

            with gr.Row():
                chat_input = gr.Textbox(
                    placeholder="Ask a billing question...",
                    label="Your message",
                    scale=4,
                    lines=1,
                )
                send_btn = gr.Button("Send", variant="primary", scale=1)

            with gr.Row():
                clear_btn = gr.Button("Clear Chat", variant="secondary", size="sm")

            with gr.Accordion("Sample Prompts", open=False):
                sample_md = gr.Markdown(
                    _format_samples("customer_care")
                )

            def on_send(message, history, persona, request: gr.Request):
                if not message.strip():
                    return history, ""
                response = chat_respond(message, history, persona, request)
                history = history + [(message, response)]
                return history, ""

            send_btn.click(
                fn=on_send,
                inputs=[chat_input, chatbot, persona_dd],
                outputs=[chatbot, chat_input],
            )
            chat_input.submit(
                fn=on_send,
                inputs=[chat_input, chatbot, persona_dd],
                outputs=[chatbot, chat_input],
            )
            clear_btn.click(fn=lambda: ([], ""), outputs=[chatbot, chat_input])

            def update_samples(persona):
                return _format_samples(persona)

            persona_dd.change(fn=update_samples, inputs=persona_dd, outputs=sample_md)

        # ── Tab 2: Analytics ────────────────────────────────────────
        with gr.Tab("Analytics", id="analytics"):
            gr.Markdown("""
### Fleet Analytics (Genie-Powered)
Ask analytical questions across the entire billing dataset. These queries are routed to a
**Databricks Genie Space** that generates SQL over 18+ billing, finance, and operational tables.

*This is the same analytics engine available through the Agent Bricks read-only tier.*
            """)

            analytics_input = gr.Textbox(
                placeholder="Ask a fleet-wide analytics question...",
                label="Analytics Question",
                lines=2,
            )
            analytics_btn = gr.Button("Run Analytics Query", variant="primary")
            analytics_output = gr.Textbox(
                label="Analytics Result",
                lines=12,
                interactive=False,
                show_copy_button=True,
            )

            with gr.Accordion("Sample Analytics Prompts", open=True):
                gr.Markdown("\n".join(f"- {q}" for q in ANALYTICS_SAMPLES))

            def run_analytics(question, persona, request: gr.Request):
                if not question.strip():
                    return "Please enter a question."
                # Route through the agent with an analytics-focused prompt
                prefixed = (
                    f"[Analytics query — use ask_billing_analytics tool] {question}"
                )
                return chat_respond(prefixed, [], persona, request)

            analytics_btn.click(
                fn=run_analytics,
                inputs=[analytics_input, persona_dd],
                outputs=analytics_output,
            )

        # ── Tab 3: Data Integration ─────────────────────────────────
        with gr.Tab("Data Integration", id="data"):
            gr.Markdown("""
### External Data & Lakebase

This accelerator integrates external enterprise data through three tracks:

| Track | Technology | Purpose | Status |
|-------|-----------|---------|--------|
| **Track A** | Lakehouse Federation | Query external PostgreSQL ERP via foreign catalogs | Available when `erp_connection_host` is configured |
| **Track B** | Synthetic Simulation | Local Delta tables simulating ERP accounts, orders, FX rates | Always available (default) |
| **Track C** | Lakebase (Managed PostgreSQL) | Transactional write-back for disputes, audit, agent actions | Available when Lakebase is provisioned |

#### What this means for the agent
- **ERP data** (accounts, revenue, procurement) flows through `ext_*` views into Silver/Gold Delta tables
- The agent accesses ERP data via UC functions: `lookup_customer_erp_profile`, `lookup_revenue_attribution`, `get_finance_operations_summary`
- **Lakebase** stores transactional state (disputes, audit log) with ACID guarantees and sub-100ms writes
- **Genie** queries the Delta analytical surface, including synced Lakebase data

#### Try these prompts in the Chat tab
- "What is the ERP credit profile for customer 4401?"
- "What is total billed revenue vs ERP recognized revenue for the last 3 months?"
- "Show me the OPEX ratio trend for the last 6 months"
            """)

            lakebase_status = gr.Textbox(
                label="Lakebase Status",
                value=_check_lakebase_status(),
                interactive=False,
                lines=4,
            )
            refresh_lb = gr.Button("Refresh Status", size="sm")
            refresh_lb.click(fn=lambda: _check_lakebase_status(), outputs=lakebase_status)

        # ── Tab 4: Operations ───────────────────────────────────────
        with gr.Tab("Operations", id="ops"):
            gr.Markdown("""
### Operational Intelligence

Use the Chat tab with the **Platform Engineering** persona for:
- Pipeline health and job reliability
- DBU consumption and cost analysis
- Warehouse performance metrics
- Write audit trail review

Or with the **Finance & Analytics** persona for:
- Revenue and ARPU trends
- AR health and overdue ratios
- Anomaly exposure and acknowledgement status

#### Quick operational queries
            """)

            ops_question = gr.Textbox(
                placeholder="Ask about platform health, costs, or pipeline status...",
                label="Operations Question",
                lines=2,
            )
            ops_btn = gr.Button("Query Operations", variant="primary")
            ops_output = gr.Textbox(
                label="Operations Result",
                lines=10,
                interactive=False,
                show_copy_button=True,
            )

            ops_samples = [
                "Is the billing anomaly detection pipeline healthy?",
                "What is our DBU consumption and estimated cost this week?",
                "What write operations has the agent performed in the last 24 hours?",
                "How many CRITICAL anomalies are unacknowledged right now?",
            ]
            with gr.Accordion("Sample Operations Prompts", open=True):
                gr.Markdown("\n".join(f"- {q}" for q in ops_samples))

            def run_ops_query(question, persona, request: gr.Request):
                if not question.strip():
                    return "Please enter a question."
                # Use technical persona for ops queries
                effective_persona = "technical" if persona in ("customer_care",) else persona
                return chat_respond(question, [], effective_persona, request)

            ops_btn.click(
                fn=run_ops_query,
                inputs=[ops_question, persona_dd],
                outputs=ops_output,
            )

        # ── Tab 5: Platform Info ────────────────────────────────────
        with gr.Tab("Platform", id="platform"):
            gr.Markdown("""
### Deployment & Capability Overview

This accelerator demonstrates four Databricks platform capabilities:

| Pillar | What it provides | Notebook |
|--------|-----------------|----------|
| **Agent Bricks** | Managed read-only FAQ + Analytics supervisor (KA + Genie) | `04_agent_bricks_deployment` |
| **Genie Spaces** | Natural language SQL over 18+ billing/finance tables | `03a_create_genie_space` |
| **Lakebase** | Transactional write-back store for disputes and audit | `08c_lakebase_setup` |
| **Databricks Apps** | This application — governed, persona-aware UI | This app |

#### Deployment Tiers

| Capability | LangGraph (Full) | Agent Bricks (Read-Only) |
|---|---|---|
| FAQ retrieval | Yes | Yes |
| Fleet-wide analytics (Genie) | Yes | Yes |
| Individual customer lookup | Yes | No |
| Write-back (disputes, anomalies) | Yes | No |
| ERP/finance data | Yes | No |
| Persona filtering | Yes | No |
| Identity propagation | Yes | No |

> This app connects to the **LangGraph full-capability endpoint**.
> For the Agent Bricks read-only experience, deploy notebook 04.
            """)

            health_output = gr.Textbox(label="Health Report", lines=14, interactive=False)
            health_btn = gr.Button("Run Health Check", variant="secondary")
            health_btn.click(fn=health_check, outputs=health_output)

        # ── Footer ──────────────────────────────────────────────────
        gr.Markdown("""
---
*Customer Billing Accelerator* — Powered by Databricks LangGraph, Genie Spaces, Agent Bricks, and Lakebase.
        """)

    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_samples(persona: str) -> str:
    """Format sample prompts for a persona as a markdown list."""
    samples = CHAT_SAMPLES.get(persona, CHAT_SAMPLES["customer_care"])
    lines = [f"**Try these with {PERSONAS.get(persona, {}).get('label', persona)}:**"]
    for s in samples:
        lines.append(f"- {s}")
    return "\n".join(lines)


def _check_lakebase_status() -> str:
    """Check if Lakebase connectivity is available."""
    pg_host = os.getenv("PGHOST", "")
    if pg_host:
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=pg_host,
                port=os.getenv("PGPORT", "5432"),
                user=os.getenv("PGUSER", ""),
                password=os.getenv("PGPASSWORD", ""),
                database=os.getenv("PGDATABASE", "databricks_postgres"),
                connect_timeout=5,
            )
            conn.close()
            return f"Lakebase: CONNECTED\nHost: {pg_host}\nTrack C: Active (transactional write-back enabled)"
        except Exception as e:
            return f"Lakebase: CONNECTION FAILED\nHost: {pg_host}\nError: {e}"
    return (
        "Lakebase: NOT CONFIGURED\n"
        "To enable Track C, add a Lakebase database resource to this app\n"
        "and run notebook 08c_lakebase_setup."
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("Starting Billing Intelligence App on %s:%d", APP_HOST, APP_PORT)
    log.info("Serving endpoint: %s", SERVING_ENDPOINT or "(not configured)")
    log.info("Identity SDK: %s", "available" if _IDENTITY_AVAILABLE else "not available")
    log.info("Session ID: %s", SESSION_ID)

    ui = build_app()
    ui.launch(server_name=APP_HOST, server_port=APP_PORT)
