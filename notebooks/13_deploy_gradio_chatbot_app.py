# Databricks notebook source
# MAGIC %md
# MAGIC # Deploy Gradio chatbot as a Databricks App
# MAGIC
# MAGIC Deploys [`apps/gradio-chatbot-app`](../apps/gradio-chatbot-app) using the same Apps API pattern as notebook **12** (serving-endpoint resource + snapshot deploy).
# MAGIC
# MAGIC **Prerequisites**
# MAGIC - **`000-config`** and **`03_agent_deployment_and_evaluation`** (serving endpoint exists).
# MAGIC - Repo layout with `apps/gradio-chatbot-app` next to `notebooks/` (e.g. Databricks Repos).
# MAGIC
# MAGIC Default workspace app name: **`billing-gradio-chat`** (distinct from the Dash app **`billing-chatbot`**).

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("app_name", "billing-gradio-chat", "App name (workspace)")
dbutils.widgets.text("serving_endpoint_name", "", "Serving endpoint (empty = config agent_name)")
dbutils.widgets.text("source_code_path", "", "Workspace path to gradio-chatbot-app (empty = auto)")
dbutils.widgets.dropdown("skip_start", "false", ["false", "true"], "Skip start after deploy")

# COMMAND ----------

# DBTITLE 1,Load accelerator config
# MAGIC %run ./000-config

# COMMAND ----------

# DBTITLE 1,Resolve paths and import deploy helper
import os
import sys

from databricks.sdk import WorkspaceClient

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_np = _ctx.notebookPath()
_nb_path = _np.get() if _np is not None else None
if not _nb_path:
    raise RuntimeError(
        "Could not resolve notebook path; set the source_code_path widget to the "
        "workspace folder for apps/gradio-chatbot-app (under /Workspace/...)."
    )
if not _nb_path.startswith("/"):
    _nb_path = "/" + _nb_path
if "/notebooks/" in _nb_path or _nb_path.rstrip("/").endswith("/notebooks"):
    _repo_root = _nb_path.split("/notebooks")[0]
else:
    _repo_root = os.path.dirname(_nb_path)

_scripts = os.path.join(_repo_root, "scripts")
if _scripts not in sys.path:
    sys.path.insert(0, _scripts)

from deploy_gradio_chatbot_app import deploy_gradio_chatbot_app

# COMMAND ----------

# DBTITLE 1,Resolve widget defaults
_serving = dbutils.widgets.get("serving_endpoint_name").strip()
if not _serving:
    _serving = config.get("agent_name") or "ai_customer_billing_agent"

_app_name = dbutils.widgets.get("app_name").strip() or "billing-gradio-chat"

_source = dbutils.widgets.get("source_code_path").strip()
if not _source:
    _source = os.path.join(_repo_root, "apps", "gradio-chatbot-app").replace("\\", "/")

_skip = dbutils.widgets.get("skip_start").lower() == "true"

print(f"app_name:           {_app_name}")
print(f"serving_endpoint:   {_serving}")
print(f"source_code_path:   {_source}")
print(f"skip_start:         {_skip}")

# COMMAND ----------

# DBTITLE 1,Verify source folder exists in workspace
_wc = WorkspaceClient()
try:
    _wc.workspace.get_status(_source)
except Exception as e:
    raise RuntimeError(
        f"Source path not found in workspace: {_source}\n"
        "Ensure this repo is cloned in Repos (or upload apps/gradio-chatbot-app) so the path exists."
    ) from e

# COMMAND ----------

# DBTITLE 1,Create / update app, deploy snapshot, start compute
_result = deploy_gradio_chatbot_app(
    source_code_path=_source,
    serving_endpoint=_serving,
    app_name=_app_name,
    skip_start=_skip,
)

# COMMAND ----------

# DBTITLE 1,Summary
_host = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .browserHostName()
    .get()
)
print("---")
print("Gradio Databricks App deployment complete.")
if getattr(_result, "url", None):
    print(f"URL: {_result.url}")
else:
    print(f"Open Apps in the workspace: https://{_host}/compute/apps")
print("---")
