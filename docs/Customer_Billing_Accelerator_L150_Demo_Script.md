# Customer Billing Accelerator — L150 Demo Script

> **Level:** 150 (business-fluent, technically grounded — not a deep architecture walkthrough)
> **Audience:** AEs, partner sellers, customer stakeholders (CFO, VP Finance, CTO adjacent)
> **Goal:** Explain what this does, why it matters, and how it works — with enough technical specificity that nobody needs to interrupt you to ask "but how?"
> **Tone:** Confident, conversational, zero hand-waving. You know the system.

---

## Opening (≈1 min)

"This is the Customer Billing Accelerator — built by Entrada in partnership with Databricks. It's a production-ready system, not a proof of concept. It runs natively on the Databricks Lakehouse and solves a problem every enterprise has: billing data is fragmented, investigating anomalies is manual, and finance teams can't self-serve.

What we built is an agentic AI system — meaning it doesn't just answer questions when asked. It continuously monitors billing data, detects anomalies on its own, and lets business users investigate in natural language. Everything governed through Unity Catalog. No external dependencies, no shadow IT."

---

## The Problem (≈2 min)

"Three things are broken in enterprise billing analytics today.

**First — the data is everywhere.** ERP for order-to-cash, a billing engine for charge computation, operational systems for usage events, maybe a separate warehouse for reporting. Finance can't get a single view without filing a ticket to the data team. That delay is where revenue leakage hides.

**Second — investigation is manual.** An analyst spots a customer whose bill doubled. What happens? They file a ticket, wait for someone to write a query, get a spreadsheet back, manually cross-reference against plan pricing, maybe check the ERP. That's a multi-day cycle for a single anomaly.

**Third — nobody monitors the platform itself.** Finance teams need to know: are the pipelines running? Is anomaly detection succeeding? What's our compute cost trending? Without that, you're flying blind on the system that's supposed to give you visibility."

---

## What the Accelerator Does (≈3 min)

"The accelerator solves all three of those problems with four capabilities.

**Continuous anomaly detection.** Agents run on a schedule — daily for anomaly scoring, every few hours for platform health monitoring. They compute per-customer baselines from billing history, then flag statistical outliers: z-score thresholds for total charge spikes, multiplier-based detection for roaming and international charges, overage spike detection. These aren't static thresholds — they're adaptive per customer. Anomalies get severity-classified: CRITICAL, HIGH, MEDIUM. Critical ones trigger proactive alerts.

*Why this matters to the audience:* The CFO sees aggregate exposure — 'what's my total anomaly dollar amount this quarter?' Finance Ops sees operational triage — 'which anomalies haven't been acknowledged yet?'

**Natural-language investigation.** When someone wants to dig deeper, they ask the agent in plain English. The agent doesn't hallucinate an answer — it executes structured SQL against governed tables through Unity Catalog functions. It can traverse the full billing chain: customer → plan → charge breakdown → ERP revenue recognition. Seconds, not days.

**Self-service conversational BI via Genie.** Finance users ask questions like 'what's the average monthly charge by plan tier?' or 'top 10 customers by roaming charges.' Genie translates that to SQL, runs it, returns the result. No SQL knowledge needed. And the Genie Space only sees a PII-safe projection of the data — customer names, emails, phone numbers are excluded from the view by design. Genie literally cannot surface PII in its generated queries.

**Platform self-monitoring.** The system ingests Databricks system tables — DBU billing, job run timelines, query history — into governed telemetry tables. The agent can tell you: pipeline reliability is 98.3%, average run duration is 12 minutes, DBU consumption is trending 15% above the 7-day rolling average. The platform watches itself."

---

## How It Works — Just Enough Architecture (≈3 min)

"Under the hood, five things to know.

**One — Medallion architecture.** All data lands in a Bronze → Silver → Gold pipeline governed by Unity Catalog. Bronze is raw ingestion. Silver is cleansed and joined — billing transactions matched with customer dimensions, KPI definitions computed, anomaly feature tables built. Gold is business-ready — revenue attribution, billing KPIs, anomaly scores. Every table, every column, governed with RBAC, ABAC, and full lineage.

**Two — AgentBricks for agent orchestration.** This isn't one monolithic agent. It's specialized agents composed declaratively: anomaly detection, investigation, billing lookup, FAQ retrieval, ad-hoc analytics. Each agent has a different system prompt and different tool access. Different personas — Executive, Finance Ops, Technical — see different agents with different capabilities. The agents call Unity Catalog functions as tools, so governance is enforced at the tool level, not just the prompt level.

**Three — Genie for conversational analytics.** Separate from the agents. Genie does NL-to-SQL against PII-safe views on Databricks SQL Serverless. Auto-scaling compute, so it handles billing-cycle burst without pre-provisioning.

**Four — Lakehouse Federation for external data.** ERP data from SAP, Oracle, Workday can be accessed via zero-ETL federation through Lakebase — no data copies required. The agent queries it in place. For systems that don't support federation, we do a traditional sync to UC tables.

**Five — everything is Databricks-native.** Unity Catalog for governance, Serverless for compute, Delta for storage, MLflow for model lifecycle and agent tracing, Mosaic AI Model Serving for inference. The default LLM is Claude 3.7 Sonnet via Databricks Model Serving, but it's model-agnostic — swap in DBRX, Llama, whatever's available through the Foundation Model API. Zero external dependencies."

---

## Demo Walkthrough — What You're Seeing (≈5–7 min)

> *Narrate over the demo video. Adapt these beats to what's on screen.*

**Agent conversation UI:**
"What you're seeing here is the agent interface. This is an AgentBricks agent deployed as a Databricks app. The user is a finance analyst — the agent knows their persona and scopes its responses accordingly.

Watch what happens when they ask about billing anomalies — the agent isn't doing retrieval-augmented generation. It's calling Unity Catalog functions that execute real SQL against governed Gold tables. The response includes actual numbers from actual data, with the query lineage traced in MLflow."

**Anomaly detection results:**
"These anomaly results were computed by the detection agent on its last scheduled run. Each anomaly has a severity, a dollar impact, and a root-cause hypothesis. The agent computed per-customer baselines and flagged deviations. When the user asks 'why did this customer spike?' — the investigation agent traverses the billing chain and returns the breakdown."

**Genie Space:**
"Now we switch to the Genie interface. This is the self-service layer for business users. Notice the question is in plain English. Genie writes the SQL, executes it on Serverless, returns the result. The user can refine — 'now break that down by region' — and Genie maintains conversational context.

Look at the columns in the result — no customer names, no emails, no PII. The Genie Space is scoped to a PII-safe view. That's a design-time decision, not a runtime filter."

**Platform monitoring:**
"And here's the observability layer. The agent is reporting on its own platform health — job success rates, pipeline durations, DBU consumption trends. This comes from ingesting Databricks system tables on a 30-day rolling window. The CTO or Head of Data Engineering cares about this: is the system healthy, and what's it costing?"

---

## PepsiCo — Where This Has Already Worked (≈2 min)

"We deployed this at PepsiCo during their migration of hundreds of SQL warehouses to Databricks Serverless. The accelerator rode alongside the migration — we didn't wait for it to finish.

Four results worth knowing:

**$2.4M in annual optimization opportunities** identified by analyzing DBU consumption patterns, warehouse queuing, and job reliability through the telemetry pipeline. Over-provisioned clusters, right-sizing opportunities, warehouse consolidation.

**85% success rate** migrating DBSQL Pro warehouses to Serverless — with the accelerator's telemetry monitoring giving the migration team before-and-after query performance confidence.

**Genie went from executive demo to daily production use** by finance teams for self-service billing queries.

**Validated governed agentic write-back.** Agents don't just read — they write back. Acknowledging anomalies, creating disputes, escalating issues, all through a human-in-the-loop confirmation workflow with full audit trails. That's a big architectural proof point."

---

## What a Customer Needs (≈1 min)

"Four requirements:

- **Unity Catalog** — billing data and telemetry registered in UC. Non-negotiable; it's how governance works.
- **Lakehouse Federation or sync** — external sources accessible via Lakebase or synced to UC tables.
- **Serverless compute** — billing workloads are bursty; you need auto-scaling, not fixed clusters.
- **AgentBricks + Genie + Foundation Model API access** — Premium tier or above.

If a customer's data is already in Unity Catalog, core deployment is 2–4 weeks. Add ERP federation and the full observability pipeline, that's another 2–3 weeks. PepsiCo-scale with migration co-delivery was 3–4 months."

---

## Closing (≈30 sec)

"This is a partnership offering between Entrada and Databricks. We've parameterized the accelerator by industry — telco, SaaS, utility, CPG — and by persona. The underlying architecture is industry-agnostic; swapping domains changes vocabulary and charge categories, not the pipeline.

Happy to go deeper on any layer — the agent framework, the Genie analytics, the PepsiCo deployment, or the anomaly detection model. We can also run the live demo if you want to see it respond in real time."

---

## Preemptive FAQ — Things They'll Think But Might Not Ask

| Question | Your Answer |
|---|---|
| "How is this different from a RAG chatbot?" | Three differences. (1) The agent executes structured SQL against governed UC functions — it doesn't retrieve documents and hallucinate over them. (2) It can *act* — acknowledge anomalies, create disputes, escalate — through human-in-the-loop confirmation. (3) It monitors its own platform health. A chatbot answers questions; this system detects, investigates, and acts. |
| "What about PII / data security?" | Four layers. PII-safe views for Genie — it never sees names or emails. Agent system prompts that exclude confidential fields. Unity Catalog RBAC/ABAC on service principals. Persona-based tool restrictions on who can invoke what. |
| "What LLM does it use?" | Default is Claude 3.7 Sonnet via Databricks Model Serving. Model-agnostic — swap in DBRX, Llama, or anything available through Foundation Model API. Tool-calling is standardized through LangChain/LangGraph. |
| "Can this work outside telco?" | Yes. Domain configs for telco (subscriber/plan), SaaS (customer/subscription), utility (account/rate plan), CPG. Swapping domains changes agent vocabulary and charge category labels. Architecture is industry-agnostic. |
| "What Databricks SKUs?" | Unity Catalog (all tiers), Serverless SQL or DBSQL Pro, Foundation Model API, Vector Search, Model Serving, DLT for streaming. Genie and system table access require Premium tier. |
