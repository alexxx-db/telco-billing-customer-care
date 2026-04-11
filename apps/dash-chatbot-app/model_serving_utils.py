"""Model serving utilities with identity context propagation."""

import json
import logging
from typing import Optional
from mlflow.deployments import get_deploy_client

logger = logging.getLogger(__name__)

# Lazy import to avoid circular deps in serving context
_identity_secret: Optional[bytes] = None


def _get_secret() -> bytes:
    global _identity_secret
    if _identity_secret is None:
        from identity_utils import get_identity_secret
        _identity_secret = get_identity_secret()
    return _identity_secret


def build_request_context(user_token: str, workspace_host: str,
                          persona: str, session_id: str) -> Optional[str]:
    """Build a signed RequestContext JSON string from a user OAuth token."""
    try:
        from identity_utils import get_user_info, RequestContext
        user_info = get_user_info(user_token, workspace_host)
        ctx = RequestContext.create(
            user_email=user_info["email"],
            user_groups=user_info["groups"],
            persona=persona,
            session_id=session_id,
            secret=_get_secret(),
            ttl_minutes=15,
        )
        return ctx.to_json()
    except Exception as e:
        logger.error(f"Failed to build request context: {e}")
        return None


def query_endpoint(
    endpoint_name: str,
    messages: list[dict[str, str]],
    max_tokens: int,
    persona: str = "customer_care",
    user_token: Optional[str] = None,
    workspace_host: Optional[str] = None,
    session_id: str = "",
) -> dict[str, str]:
    """Call the serving endpoint with identity context if available."""
    custom_inputs = {"persona": persona}

    if user_token and workspace_host:
        ctx_json = build_request_context(
            user_token, workspace_host, persona, session_id
        )
        if ctx_json:
            custom_inputs["request_context"] = ctx_json

    try:
        res = get_deploy_client("databricks").predict(
            endpoint=endpoint_name,
            inputs={
                "messages": messages,
                "max_tokens": max_tokens,
                "custom_inputs": custom_inputs,
            },
        )
    except ConnectionError as e:
        raise RuntimeError(f"Cannot connect to '{endpoint_name}'.") from e
    except TimeoutError as e:
        raise RuntimeError(f"Timeout calling '{endpoint_name}'.") from e

    if "messages" in res:
        return res["messages"][-1]
    elif "choices" in res:
        return res["choices"][0]["message"]
    raise RuntimeError("Unexpected response format from serving endpoint.")
