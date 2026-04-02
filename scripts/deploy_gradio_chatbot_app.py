#!/usr/bin/env python3
"""
Deploy apps/gradio-chatbot-app as a Databricks App (SDK).

Usage:
  python scripts/deploy_gradio_chatbot_app.py \\
    --source-code-path /Workspace/Users/you@corp.com/Repos/org/repo/apps/gradio-chatbot-app \\
    --serving-endpoint ai_customer_billing_agent

  # Optional: --app-name billing-gradio-chat (default)
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timedelta

_scripts_dir = os.path.dirname(os.path.abspath(__file__))
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from databricks.sdk import WorkspaceClient
from workspace_app_deploy import deploy_and_start, deploy_serving_endpoint_app, ensure_app


def deploy_gradio_chatbot_app(
    source_code_path: str,
    serving_endpoint: str,
    *,
    app_name: str = "billing-gradio-chat",
    description: str = "Gradio chat UI for the customer billing AI agent",
    skip_start: bool = False,
    timeout_minutes: int = 25,
):
    """Programmatic entry used by notebook 13."""
    return deploy_serving_endpoint_app(
        source_code_path,
        serving_endpoint,
        app_name=app_name,
        description=description,
        skip_start=skip_start,
        timeout_minutes=timeout_minutes,
    )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deploy Gradio chatbot as a Databricks App.")
    p.add_argument(
        "--app-name",
        default="billing-gradio-chat",
        help="Workspace app name (lowercase, hyphens; max 26 chars).",
    )
    p.add_argument(
        "--serving-endpoint",
        required=True,
        help="Model serving endpoint name (from notebook 03 or 04).",
    )
    p.add_argument(
        "--source-code-path",
        required=True,
        help="Workspace path to apps/gradio-chatbot-app (under /Workspace/...).",
    )
    p.add_argument(
        "--description",
        default="Gradio chat UI for the customer billing AI agent",
    )
    p.add_argument(
        "--skip-start",
        action="store_true",
        help="Deploy only; do not start app compute.",
    )
    p.add_argument(
        "--timeout-minutes",
        type=int,
        default=25,
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    timeout = timedelta(minutes=args.timeout_minutes)
    w = WorkspaceClient()
    ensure_app(
        w,
        args.app_name,
        args.serving_endpoint,
        args.description,
        timeout,
    )
    deploy_and_start(
        w,
        args.app_name,
        args.source_code_path,
        args.skip_start,
        timeout,
    )
    final = w.apps.get(name=args.app_name)
    if final.url:
        print(f"App URL: {final.url}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(130)
