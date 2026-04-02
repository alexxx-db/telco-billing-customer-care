"""
Shared Databricks Apps deployment helpers (serving-endpoint resource + snapshot deploy + start).

Used by deploy_dash_chatbot_app.py, deploy_gradio_chatbot_app.py, and notebooks 12 / 13.
"""

from __future__ import annotations

from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppDeploymentMode,
    AppResource,
    AppResourceServingEndpoint,
    AppResourceServingEndpointServingEndpointPermission,
)


def serving_resource(endpoint_name: str) -> AppResource:
    return AppResource(
        name="serving-endpoint",
        serving_endpoint=AppResourceServingEndpoint(
            name=endpoint_name,
            permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY,
        ),
    )


def ensure_app(
    w: WorkspaceClient,
    app_name: str,
    serving_endpoint: str,
    description: str,
    timeout: timedelta,
) -> None:
    """Create the app if missing, or PATCH resources when the serving endpoint changed."""
    body = App(
        name=app_name,
        description=description,
        resources=[serving_resource(serving_endpoint)],
    )
    try:
        existing = w.apps.get(name=app_name)
        cur = None
        if existing.resources:
            for r in existing.resources:
                if r.name == "serving-endpoint" and r.serving_endpoint:
                    cur = r.serving_endpoint.name
                    break
        if cur != serving_endpoint:
            w.apps.update(
                name=app_name,
                app=App(
                    name=app_name,
                    description=description,
                    resources=[serving_resource(serving_endpoint)],
                ),
            )
            print(f"Updated app resources (serving endpoint: {serving_endpoint}).")
        else:
            print("App exists; serving endpoint resource already matches.")
    except NotFound:
        w.apps.create_and_wait(app=body, no_compute=True, timeout=timeout)
        print(f"Created app {app_name!r}.")


def deploy_and_start(
    w: WorkspaceClient,
    app_name: str,
    source_code_path: str,
    skip_start: bool,
    timeout: timedelta,
) -> None:
    dep = AppDeployment(
        source_code_path=source_code_path.rstrip("/"),
        mode=AppDeploymentMode.SNAPSHOT,
    )
    w.apps.deploy_and_wait(app_name=app_name, app_deployment=dep, timeout=timeout)
    print("Deployment finished.")
    if not skip_start:
        w.apps.start_and_wait(name=app_name, timeout=timeout)
        print("App compute started.")


def deploy_serving_endpoint_app(
    source_code_path: str,
    serving_endpoint: str,
    *,
    app_name: str,
    description: str,
    skip_start: bool = False,
    timeout_minutes: int = 25,
) -> App:
    """Create/update app, deploy source snapshot, optionally start compute."""
    timeout = timedelta(minutes=timeout_minutes)
    w = WorkspaceClient()
    ensure_app(w, app_name, serving_endpoint, description, timeout)
    deploy_and_start(w, app_name, source_code_path, skip_start, timeout)
    return w.apps.get(name=app_name)
