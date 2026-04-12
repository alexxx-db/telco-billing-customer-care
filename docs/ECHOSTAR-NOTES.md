# EchoStar: Evaluation and Deployment Guide

Notes specific to EchoStar's governed identity propagation and billing
intelligence requirements.

---

## Why This Accelerator

EchoStar's requirement is not just "build a billing chatbot." It is:

> Make governed, user-aware execution work across the entire stack — from
> the app UI through agent orchestration to data access and write-back —
> even when a service principal mediates all SQL execution.

This accelerator implements exactly that through Pattern C (Hybrid) identity
propagation, UC-tag-driven governance, schema-level PII isolation, and
dual-identity audit trails.

---

## Recommended Deployment Path

### Primary: LangGraph Agent (Notebook 03)

This is the full-capability path. It provides:
- 19 tools (14 UC functions + Vector Search + Genie + 3 write-back)
- Identity propagation (SCIM + HMAC + RequestContext)
- Persona-based tool filtering with group binding
- Write-back with token-gated confirmation and dual-identity audit
- PII isolation via schema separation + UC grants
- UC tag-driven governance policies

### Secondary: Agent Bricks (Notebook 04)

Read-only tier for demos and analyst self-service:
- FAQ retrieval (Knowledge Assistant)
- Fleet-wide analytics (Genie Space)
- Supervisor routing between the two
- No write-back, no individual customer tools, no identity propagation

### App Surface: Gradio App (Preferred for Demos)

The Gradio app provides a multi-workspace view:
- Chat, Analytics, Data Integration, Operations, Platform tabs
- Full identity propagation
- Visible platform pillar showcase

### Data: LangGraph + Federation Track B + Optional Lakebase

- Track B (synthetic ERP simulation) for initial evaluation
- Track A (real federation) when external ERP is available
- Track C (Lakebase) for transactional write-back when provisioned

---

## Identity and Governance: What EchoStar Should Validate

### 1. User Identity Propagation

**What to test**: Log in as different users with different SCIM group memberships.
Verify that:
- The health check (Gradio Platform tab) shows the correct email and groups
- Persona restrictions are enforced (e.g., a user without `c_suite` group
  cannot use the `executive` persona — the agent falls back to `customer_care`)
- Write audit entries show the correct `initiating_user`, not just the SP

**Where to look**: `billing_write_audit` table, `initiating_user` and
`executing_principal` columns.

### 2. PII Isolation

**What to test**: Ask the agent "What is the contact information for customer 4401?"
The agent should NOT return `customer_name`, `email`, or `phone_number` because
`lookup_customer` no longer includes those columns in its RETURNS TABLE.

**Where to look**: Run `SELECT * FROM {catalog}.{schema}.lookup_customer('4401')` —
only `customer_id`, `device_id`, `plan`, `contract_start_dt` should appear.

### 3. Write Guards

**What to test**: Try to create a dispute without identity context (e.g., from a
direct API call without `request_context` in custom_inputs). The write should be
BLOCKED with a clear error message.

**Where to look**: Agent response should include "BLOCKED: Authenticated user
context required for write operations."

### 4. Tag-Driven Policies

**What to test**: Run `12a_validate_identity_setup.py` — all 9 checks should pass.
Run `12_admin_tagging.py` — verify `gov.*` tags appear on tables.

**Where to look**: `information_schema.table_tags` in your catalog.

### 5. Bounded Tool Responses

**What to test**: Ask the agent to look up billing items for a high-activity device.
Verify the response contains at most 100 items (the LIMIT).

**Where to look**: `SHOW CREATE FUNCTION {catalog}.{schema}.lookup_billing_items` —
verify LIMIT clause is present.

---

## Evaluation Scenarios

### Scenario 1: Customer Service (5 minutes)

1. Select **Customer Support** persona
2. "How is my bill calculated?" — FAQ retrieval
3. "What are the charges for customer 4401?" — Individual lookup
4. "Are there any billing anomalies for customer 4401?" — Anomaly check
5. "Create a billing dispute for customer 4401 for overcharges" — Write-back staging
6. "CONFIRM" — Write execution with identity audit

**Verify**: The `billing_write_audit` table should have entries with your email
as `initiating_user` and the SP as `executing_principal`.

### Scenario 2: Finance Analytics (5 minutes)

1. Select **Finance & Analytics** persona
2. "What is total billing revenue this month vs last month?" — Genie analytics
3. "Which customer segments have the highest overdue AR ratio?" — ERP data
4. "How many CRITICAL anomalies are unacknowledged right now?" — Monitoring
5. "Show me the OPEX ratio trend for the last 6 months" — Finance operations

**Verify**: Responses should include specific numbers from the billing dataset.

### Scenario 3: Governance Boundary (3 minutes)

1. Log in as a user WITHOUT `c_suite` or `billing_leadership` groups
2. Select **Executive** persona
3. Ask any question
4. **Verify**: The agent should respond using `customer_care` tool set (not executive),
   and the agent logs should show "Persona denied: user=... groups=... persona=executive"

---

## Caveats

### Lakebase

Lakebase (Track C) is designed but depends on Lakebase availability in the target
workspace. If Lakebase is not provisioned, write-back uses the existing Statement
Execution API -> Delta path. The architecture is ready; the infrastructure may not be.

### Agent Bricks

Agent Bricks is read-only. It cannot create disputes, look up individual customers,
or propagate user identity. Do not evaluate Agent Bricks as a production-equivalent
path — it is a managed demo tier.

### Cold Start

The first Genie Space query after inactivity takes 45-90 seconds. Warm the Genie
Space with a test query before a demo session.

### Streaming

Neither the Dash nor Gradio app supports streaming responses. The agent's
`predict_stream()` method exists but is not wired to the app layer. Full
responses are returned after processing completes.

---

## Key Files for EchoStar Review

| File | Why it matters |
|---|---|
| `notebooks/identity_utils.py` | RequestContext, HMAC, SCIM, tag resolution, authorization guards |
| `notebooks/agent.py` | Identity validation in predict(), write guards, contextvars |
| `notebooks/02_define_uc_tools.py` | PII isolation, UC grants, bounded functions |
| `notebooks/12_admin_tagging.py` | Governance tag administration |
| `notebooks/12a_validate_identity_setup.py` | Identity validation suite |
| `apps/shared/serving_client.py` | App-side identity creation (shared by Dash and Gradio) |
| `docs/GOVERNANCE-AND-IDENTITY.md` | Complete governance model documentation |
