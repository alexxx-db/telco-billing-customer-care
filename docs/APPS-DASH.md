# Dash App: Billing Chat Interface

The Dash app (`apps/dash-chatbot-app/`) is the reference chat interface for the
Customer Billing Accelerator. It provides persona-aware chat with full identity
propagation.

---

## Role

- **Primary use case**: Production chat interface for the LangGraph billing agent
- **Identity**: Full SCIM + HMAC identity propagation
- **UX model**: Single-pane chat with persona selector
- **Serving**: Calls the deployed Model Serving endpoint via MLflow deploy client

## Architecture

```
Browser → Databricks Apps Proxy (injects x-forwarded-access-token)
   │
   v
Dash App (Flask-based)
   ├── DatabricksChatbot.py — UI: persona radio, chat history, send/clear
   ├── model_serving_utils.py — Identity: SCIM, HMAC, RequestContext, endpoint call
   └── app.py — Entry point, creates Dash + Bootstrap layout
   │
   v
Model Serving Endpoint (custom_inputs: persona + request_context)
```

## Files

| File | Lines | Purpose |
|---|---|---|
| `app.py` | 26 | Entry point — creates Dash app with Bootstrap theme |
| `DatabricksChatbot.py` | 261 | Chat UI component with persona selector |
| `model_serving_utils.py` | 216 | Identity-aware serving client (inlined RequestContext) |
| `app.yaml` | 12 | Databricks App deployment config |
| `requirements.txt` | 5 | `dash`, `dash-bootstrap-components`, `mlflow`, `psycopg2-binary` |

## Identity Flow

1. `_call_model_endpoint()` reads `x-forwarded-access-token` from Flask headers
2. `model_serving_utils.query_endpoint()` calls `_get_user_info()` (SCIM with 5-min cache)
3. Builds `_RequestContext`, signs with HMAC-SHA256, serializes to JSON
4. Sends as `custom_inputs["request_context"]` alongside `custom_inputs["persona"]`
5. Session ID: UUID generated per `DatabricksChatbot` instance

## Persona UX

Four radio buttons at the top of the chat:
- **Customer Support** (`customer_care`) — billing inquiries, disputes, anomalies
- **Finance & Analytics** (`finance_ops`) — fleet-wide analytics, revenue, AR
- **Executive View** (`executive`) — concise leadership summaries
- **Platform Engineering** (`technical`) — pipeline health, costs, job reliability

Switching personas clears the chat history and shows a mode description.

## Configuration

`app.yaml`:
```yaml
env:
  - name: SERVING_ENDPOINT
    valueFrom: "serving-endpoint"
```

Set `SERVING_ENDPOINT` to the deployed LangGraph agent endpoint name. For the
Agent Bricks tier, set it to the MAS endpoint instead.

## Limitations

- **No streaming**: Waits for full response before displaying (agent uses `predict()`, not `predict_stream()`)
- **Chat-only UX**: No analytics workspace, no data integration visibility, no operations panel
- **No session persistence**: Chat history lost on page reload
- **Single endpoint**: Cannot switch between LangGraph and Agent Bricks from the UI
