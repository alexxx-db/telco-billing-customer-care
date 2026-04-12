# Truth Report: Platform Pillar Audit

Produced 2026-04-12. Covers every file in the repo that touches Agent Bricks, Genie Spaces,
Databricks Apps, or Lakebase/Federation. Each entry states exactly what the code does, what
the docs claim, and where the two diverge.

---

## 1. Agent Bricks

### What the code actually does

**`notebooks/04_agent_bricks_deployment.py`** (389 lines) — the only implementation file.

| Step | What it does | Status |
|---|---|---|
| Step 1: Prepare FAQ docs | Reads `billing_faq_dataset` table, writes text+JSON files to UC Volume `billing_faq_docs` | Working code |
| Step 2: Create KA | Uses `w.knowledge_assistants.create_knowledge_assistant()`, adds UC Volume as knowledge source, syncs | Working code |
| Step 3: Create Supervisor | **Prints manual UI instructions** to the notebook output — tells the user to open `/#/agents` and click through the UI | **No programmatic creation** |
| Step 4: Test MAS | Calls `w.serving_endpoints.query()` against `mas-{tile_id}-endpoint` | Working code, but depends on Step 3 completing manually |

**Line 314**: `"Note: The Supervisor Agent API is UI-only; no programmatic SDK is available yet."`
**Line 319**: `wait_for_tile(mas_tile_id, label="MAS")` — this function is **never defined** in the notebook. It will crash at runtime with `NameError`.

**What Agent Bricks deploys**: FAQ Knowledge Assistant + Genie Space as sub-agents in a Supervisor. Two capabilities only: FAQ document retrieval and Genie SQL analytics.

**What Agent Bricks does NOT deploy**: No UC function tools, no write-back, no anomaly acknowledgement, no individual customer lookup, no ERP data access, no streaming estimates, no operational KPIs, no persona filtering, no identity propagation.

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 42 | "Deploys the solution as a Databricks Agent Bricks Supervisor Agent combining a FAQ Knowledge Assistant with the Billing Analytics Genie Space." | **Accurate** but understates the gap — "deploys the solution" implies functional equivalence with notebook 03 |
| README line 71 | "Deploy as an Agent Bricks Supervisor Agent (KA + Genie Space) for a fully managed multi-agent experience." | **Misleading** — "fully managed multi-agent experience" overstates what 2 sub-agents provide. The full agent has 19 tools; Agent Bricks has 2 sub-agents with 0 tools. |
| README line 92 | "Agent Bricks (notebook 04) is a read-only demo tier." | **Accurate** |
| ARCHITECTURE.md line 49-51 | "The Agent Bricks path (notebook 04) deploys a Supervisor with only FAQ Knowledge Assistant + Genie Space — no write-back, no individual customer tools, no identity propagation." | **Accurate** |
| Capability matrix (README lines 79-91) | 10-row comparison table showing Yes/No per capability | **Accurate** — honestly shows Agent Bricks as No for 8 of 10 capabilities |
| DECISIONS.md DEC-002 | Documents the two-path divergence, capability gap table, undocumented routing to nonexistent tools | **Accurate and self-critical** |
| POSTMORTEM.md PM-006 | "A customer deployment team ran notebooks 01-04 in sequence... They then asked why the billing agent could not acknowledge anomalies, create disputes, check ERP credit status..." | **Accurate** — documents the real confusion |

### Overstatements

1. **README line 71**: "fully managed multi-agent experience" — implies production-grade multi-agent orchestration. Reality: 2 sub-agents (FAQ + Genie), read-only, no tool binding, manual UI creation step.
2. **Notebook 04 line 319**: `wait_for_tile(mas_tile_id)` — references an undefined function. The notebook will crash at this line. This is a runtime bug, not an overstatement, but it means the notebook **cannot run end-to-end without error**.
3. **Notebook 04 Step 3 framing**: The step title "Create Supervisor Agent" implies the notebook creates it. It prints instructions for the user to create it manually via the UI.

### What is genuinely good

- The capability matrix in README is honest.
- DEC-002 and PM-006 openly document the gap.
- The KA creation code (Step 2) is real, working SDK code.
- The FAQ document preparation from UC Volume is a clean pattern.

---

## 2. Genie Spaces

### What the code actually does

**`notebooks/03a_create_genie_space.py`** (215 lines) — Genie Space creation and configuration.

| Component | What it does | Status |
|---|---|---|
| PII-safe view | Creates `invoice_analytics` view excluding PII columns | Working code |
| Space creation | Uses `w.genie.create_space()` / `update_space()` with serialized config | Working code |
| Instructions | 6 PII guardrails injected into Genie Space config | Working code |
| Test conversation | Sends test question, polls for result, displays SQL + response | Working code |
| Table registration | 18 tables registered from config | Working code |

**`notebooks/agent.py`** lines 149-176 — `ask_billing_analytics` tool.

| Component | What it does | Status |
|---|---|---|
| Tool definition | `@tool` wrapping `_ws_client.genie.start_conversation_and_wait()` | Working code |
| Response parsing | Extracts text content and generated SQL from attachments | Working code |
| Error handling | Returns error message string on exception | Working code |
| Persona filtering | Included in customer_care, finance_ops, executive; excluded from technical | Working code |

**`notebooks/000-config.py`** lines 141-215 — Configuration.

| Component | What it does | Status |
|---|---|---|
| 18 table identifiers | Hardcoded FQNs for Genie Space | Working config |
| 28 sample questions | Covering billing, anomalies, streaming, telemetry, ERP, finance | Working config |
| Space name/description | "Customer Billing Analytics" with detailed description | Working config |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 40 | "Creates a Databricks Genie Space for ad-hoc billing analytics over invoice and plan tables." | **Accurate but understated** — actually registers 18 tables, not just invoice and plan |
| ARCHITECTURE.md "Runtime Components" | Documents Genie as fleet-wide analytics delegation, distinct from individual lookups | **Accurate** |
| DECISIONS.md DEC-007 | Documents Genie choice over UC functions for arbitrary SQL, cold start tradeoff | **Accurate** |
| POSTMORTEM.md PM-005 | Documents 45-90s cold start on first query after inactivity | **Accurate** — this is a real operational issue |

### Overstatements

**None found.** Genie documentation is honest and accurate throughout. The cold start issue (PM-005) is openly documented. The PII guardrails are both documented and implemented.

### What is genuinely good

- 28 curated sample questions covering 6 analytical domains.
- PII-safe `invoice_analytics` view created before Space registration.
- 6 explicit PII guardrail instructions in the Space config.
- Four-layer PII defense (schema isolation, UC grants, Genie instructions, governance tags).
- Honest cold-start documentation.

### What is missing

- No **Genie Space warm-up** mechanism (PM-005 recommends a keep-warm ping).
- No **conversation reuse** — each `ask_billing_analytics` call creates a new conversation.
- Genie is **not directly exposed in either Databricks App** — users interact with it only through the agent's `ask_billing_analytics` tool or the Agent Bricks Supervisor routing. There is no direct Genie experience in the Dash or Gradio app.
- No **demo script** or **guided walkthrough** for Genie-specific capabilities.

---

## 3. Lakebase / Lakehouse Federation

### What the code actually does

**`notebooks/08_federation_setup.py`** (99 lines) — Track A: Real Federation.

| Component | What it does | Status |
|---|---|---|
| Track detection | `USE_REAL_FEDERATION = bool(config.get("erp_connection_host", ""))` | Working code |
| UC Connection | Creates PostgreSQL connection via `w.connections.create()` | Working code (Track A only) |
| Foreign Catalog | Creates `erp_federated_{database}` catalog | Working code (Track A only) |
| Config update | Writes connection/catalog names to config.yaml via regex | Working code |
| Default behavior | **Skips entirely** if `erp_connection_host` is not configured | By design — Track B is the default |

**`notebooks/08a_erp_data_simulation.py`** (204 lines) — Track B: Simulation.

| Component | What it does | Status |
|---|---|---|
| Synthetic ERP data | 4 tables via `dbldatagen`: accounts, orders, procurement, FX rates | Working code |
| Abstraction views | 4 `ext_*` views pointing to simulated tables | Working code |
| Data volume | 1000 accounts, ~8000 orders, procurement costs, 3650 FX rate rows | Working code |

**`notebooks/08b_external_data_ingestion.py`** (254 lines) — Medallion pipeline (both tracks).

| Component | What it does | Status |
|---|---|---|
| Guard clause | Validates all 4 `ext_*` views exist | Working code |
| Silver tables | `silver_customer_account_dims`, `silver_fx_daily`, `silver_procurement_monthly`, `silver_conformed_kpi_defs` | Working code |
| Gold tables | `gold_revenue_attribution`, `gold_finance_operations_summary` | Working code |
| Track agnostic | Works identically with Track A or B via `ext_*` abstraction | Working code |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 49 | "Sets up Lakehouse Federation: UC connection to external ERP (Track A). Skip for Track B (simulation)." | **Accurate** |
| README line 50 | "Generates synthetic ERP accounts/orders, procurement costs, and FX rates (Track B). Creates ext_* view abstraction layer." | **Accurate** |
| DECISIONS.md DEC-011 | Documents both tracks, ext_* abstraction layer, config-driven track selection | **Accurate** |
| ARCHITECTURE.md Data Architecture | "Abstract ext_* views decouple Track A/B, enabling identical downstream Silver/Gold logic" | **Accurate** |

### Overstatements

1. **No mention of "Lakebase" anywhere in the codebase.** The README, DECISIONS.md, ARCHITECTURE.md, and all notebooks use "Lakehouse Federation" or "federation". The term "Lakebase" appears only in the EchoStar identity implementation context (`notebooks/config.yaml` line for secret scope, and in CLAUDE.md). If the market-facing story requires "Lakebase" branding, the current repo does not use or demonstrate it.
2. **ARCHITECTURE.md** positions the `ext_*` views as a federation pattern. This is accurate for Track A (real foreign catalog). For Track B (simulation), the `ext_*` views just point to local Delta tables — there is no federation happening, just a local abstraction layer pretending to be external data.

### What is genuinely good

- The `ext_*` view abstraction is a clean design pattern — downstream code is truly track-agnostic.
- Track B simulation generates realistic ERP data (1000 accounts, procurement, FX rates, revenue).
- The Silver/Gold medallion pipeline in `08b` is solid PySpark with proper joins and aggregations.
- Config-driven track selection (`erp_connection_host` presence) is clean.

### What is missing

- **No Lakebase demo** — the repo uses Lakehouse Federation (foreign catalogs), not Lakebase (managed PostgreSQL). These are different Databricks products. If the requirement is to demo Lakebase specifically, the current codebase does not do this.
- **No visibility in Apps** — neither the Dash app nor the Gradio app surfaces ERP/federated data. The data flows through UC functions (`lookup_customer_erp_profile`, `lookup_revenue_attribution`, `get_finance_operations_summary`) but the user has no indication they are seeing federated/external data.
- **No demo script** — no guided walkthrough showing the federation value proposition.
- **Track A is untestable without a real external database** — there is no Docker-compose or local PostgreSQL setup for testing Track A. The only way to test Track A is to have an actual external PostgreSQL ERP system.

---

## 4. Databricks Apps

### What the code actually does

**`apps/dash-chatbot-app/`** — Production Dash app (4 files, 511 total lines).

| Component | What it does | Status |
|---|---|---|
| Chat UI | Persona selector (4 radio buttons), chat history with markdown rendering, send/clear | Working code, polished |
| Identity propagation | Extracts `x-forwarded-access-token`, SCIM resolution, HMAC signing | Working code |
| Serving endpoint call | MLflow `get_deploy_client("databricks").predict()` with `custom_inputs` | Working code |
| Persona switching | Clears chat, shows mode description, routes to persona-specific agent | Working code |
| CSS/UX | Custom DM Sans font, branded colors, typing animation, auto-scroll | Working code, polished |

**`apps/gradio-databricks-app/`** — Starter template (3 files, 411 total lines).

| Component | What it does | Status |
|---|---|---|
| Multi-tab UI | Data Explorer, Text Transformer, Configuration, Health & Status | Working code (template) |
| Data Explorer | Filter/sort/limit against 10 hardcoded sample records | Working code (static data) |
| Serving endpoint | **Placeholder** — `_call_serving_endpoint()` returns mock response | **Not implemented** |
| SQL integration | **Placeholder** — `_query_sql()` returns mock data | **Not implemented** |
| DAB deployment | `databricks.yml` with dev/prod targets | Working config |
| Health check | Reports environment variable status and resource configuration | Working code |

### What the docs claim

| Document | Claim | Accurate? |
|---|---|---|
| README line 57 | "A simple Dash web app that lets users chat with the deployed agent" | **Accurate** — it is simple (chat only) |
| ARCHITECTURE.md App Layer | Documents Dash app as production-ready with identity propagation, Gradio as starter template | **Accurate** |
| DECISIONS.md DEC-013 | "Dash was chosen for simplicity... a thin shell around the serving endpoint" | **Accurate** — it is exactly that |

### Overstatements

1. **ARCHITECTURE.md line 22**: `"Databricks App (Dash/Gradio)"` — implies both apps are functional. The Gradio app is a **starter template with TODO placeholders**, not a functional billing agent interface. It has no serving endpoint integration, no identity propagation, and no billing-specific UI.
2. **README line 57**: `"dash-chatbot-app/ | A simple Dash web app"` — calling it "simple" is accurate but undersells the identity propagation work. However, it is a bare chat surface with no Agent Bricks, Genie, or Lakebase demo visibility.

### What is genuinely good

- Dash app identity propagation (SCIM + HMAC + RequestContext) is production-quality.
- Persona selector with 4 options and clear mode descriptions.
- CSS/UX polish: branded colors, typing animation, auto-scroll, responsive layout.
- Clean separation: `DatabricksChatbot.py` (UI) vs `model_serving_utils.py` (identity + endpoint).

### What is missing

- **No landing page or feature showcase** — the app opens directly to a chat interface. There are no cards, tabs, or sections exposing Agent Bricks, Genie, or Lakebase demos.
- **No Genie direct access** — users can only access Genie through the agent's `ask_billing_analytics` tool. No standalone Genie exploration experience.
- **No Lakebase/ERP data visibility** — no UI element shows that ERP data is being accessed or what federation looks like.
- **No Agent Bricks toggle** — no way to switch between LangGraph and Agent Bricks endpoints from the UI.
- **No demo mode** — no guided walkthrough, sample prompts, or capability showcase.
- **Gradio app is non-functional for billing** — it's a generic starter template. The Data Explorer shows 10 hardcoded mock customer records unrelated to the actual billing data.

---

## 5. Cross-Cutting Overstatements

### README "How to Use" section (lines 63-73)

The numbered list presents notebooks 1-8 as a **sequential flow**, implying each builds on the previous:
```
1. 000-config
2. 00_data_preparation
...
6. 03_agent_deployment_and_evaluation
7. 04_agent_bricks_deployment
8. 05_billing_anomaly_detection
```

**Overstatement**: This numbering implies notebook 04 extends notebook 03. PM-006 documents that a customer team made exactly this mistake. The notebooks are **alternative deployment paths** (03 OR 04), not sequential steps (03 THEN 04). The capability matrix at line 79 corrects this, but its placement after the sequential list means users encounter the misleading sequence first.

### ARCHITECTURE.md System Overview diagram

The diagram shows the LangGraph path only. It does not depict the Agent Bricks path at all. The text at line 48-51 acknowledges this, but a reader looking only at the diagram would not know Agent Bricks exists.

### No "Lakebase" in the codebase

The term "Lakebase" does not appear in any notebook, config file, or app code. The federation implementation uses Lakehouse Federation (foreign catalogs over PostgreSQL). If the market requirement is to demo **Lakebase** (Databricks' managed PostgreSQL offering for OLTP workloads), the current codebase demonstrates the wrong product. Lakehouse Federation and Lakebase are different:

| | Lakehouse Federation | Lakebase |
|---|---|---|
| What it is | Query external databases via foreign catalogs | Managed PostgreSQL instances inside Databricks |
| Data location | External system (customer's PostgreSQL, MySQL, etc.) | Databricks-managed PostgreSQL |
| Use case | Read external data without ETL | OLTP workloads, app backends, synced tables |
| Current repo support | **Track A: Yes** (foreign catalog over PostgreSQL) | **No** |

---

## 6. Summary Matrix

| Pillar | Code exists? | Code works? | Docs accurate? | Demo-ready? | Key gap |
|---|---|---|---|---|---|
| **Agent Bricks** | Yes (nb 04) | Partially — Step 3 is manual UI, `wait_for_tile` undefined | Mostly — README line 71 overstates | No — requires manual UI step, undefined function crashes | Supervisor creation is not programmatic; `wait_for_tile` is a runtime bug |
| **Genie Spaces** | Yes (nb 03a + agent.py) | Yes — creation, querying, PII guardrails all work | Yes — honest and accurate | Partially — no standalone demo, cold start undocumented in app | Not surfaced in either Databricks App; no demo script |
| **Lakebase** | **No** — repo implements Lakehouse Federation, not Lakebase | N/A | **Mislabeled** if "Lakebase" is the requirement | No — would need new implementation | Current code demos Federation, not Lakebase. Different products. |
| **Federation** | Yes (nb 08/08a/08b) | Yes — both tracks work, ext_* abstraction is clean | Yes — accurate | Partially — no visibility in Apps, no demo script | Not surfaced in either Databricks App |
| **Databricks Apps** | Yes (Dash functional, Gradio template) | Dash: yes. Gradio: placeholder stubs only | Mostly — Gradio claimed alongside Dash but is a template | Dash: chat-only, no pillar showcase. Gradio: non-functional for billing | Neither app surfaces Agent Bricks, Genie, or Federation as explicit demo experiences |

---

## 7. Files That Should Be Modified (Implementation Plan Inputs)

| File | Current state | What needs to change |
|---|---|---|
| `notebooks/04_agent_bricks_deployment.py` | `wait_for_tile` undefined; MAS creation is manual UI | Fix runtime bug; evaluate SDK `manage_mas` for programmatic creation |
| `apps/dash-chatbot-app/DatabricksChatbot.py` | Bare chat surface | Add landing page with 4 pillar cards, demo mode, Genie embed |
| `apps/dash-chatbot-app/model_serving_utils.py` | Single endpoint targeting | Support endpoint switching (LangGraph vs Agent Bricks) |
| `apps/gradio-databricks-app/app.py` | Generic starter template | Either implement billing-specific UI or remove from "Apps demo" claims |
| `notebooks/03a_create_genie_space.py` | Works but no warm-up | Add optional keep-warm cell |
| `notebooks/08_federation_setup.py` | Federation only | If Lakebase is required: add Lakebase provisioned instance creation |
| `README.md` | Sequential numbering misleads | Restructure around deployment tiers, not notebook sequence |
| `ARCHITECTURE.md` | LangGraph-only diagram | Add Agent Bricks architecture view |
