"""
Shared serving client for Databricks Apps.

Provides identity-aware Model Serving endpoint invocation, SCIM user
resolution, and HMAC-signed RequestContext creation. Used by both the
Dash and Gradio apps to ensure consistent identity propagation.

This module is the single source of truth for App-side identity logic.
The agent-side validation lives in notebooks/identity_utils.py; this
module only CREATES signed contexts, never verifies them.
"""

import hashlib
import hmac
import json
import logging
import os
import re
import time
import uuid
import base64
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Must match notebooks/identity_utils.py for HMAC determinism
_JSON_SEPARATORS = (",", ":")


# ---------------------------------------------------------------------------
# RequestContext — signed identity envelope (App-side creation only)
# ---------------------------------------------------------------------------

@dataclass
class RequestContext:
    """Signed, time-bounded identity context propagated from App to Agent.

    IMPORTANT: _signing_payload() must produce identical bytes to
    notebooks/identity_utils.py::RequestContext._signing_payload().
    If you change field names, JSON separators, or sort order here,
    you MUST update identity_utils.py to match (or signatures will fail).
    """
    user_email: str
    user_groups: list[str]
    persona: str
    session_id: str
    request_id: str
    issued_at: str
    expires_at: str
    signature: str = ""

    def _signing_payload(self) -> bytes:
        d = {k: v for k, v in asdict(self).items() if k != "signature"}
        return json.dumps(d, sort_keys=True, separators=_JSON_SEPARATORS,
                          default=str).encode("utf-8")

    def sign(self, secret: bytes) -> "RequestContext":
        self.signature = hmac.new(
            secret, self._signing_payload(), hashlib.sha256
        ).hexdigest()
        return self

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True,
                          separators=_JSON_SEPARATORS, default=str)

    @classmethod
    def create(cls, user_email, user_groups, persona, session_id,
               secret, ttl_minutes=15):
        now = datetime.now(timezone.utc)
        ctx = cls(
            user_email=user_email,
            user_groups=user_groups,
            persona=persona,
            session_id=session_id,
            request_id=str(uuid.uuid4()),
            issued_at=now.isoformat(),
            expires_at=(now + timedelta(minutes=ttl_minutes)).isoformat(),
        )
        return ctx.sign(secret)


# ---------------------------------------------------------------------------
# SCIM user info — cached per token with 5-minute TTL
# ---------------------------------------------------------------------------

_scim_cache: dict[str, tuple[dict, float]] = {}
_SCIM_CACHE_TTL = 300


def get_user_info(user_token: str, workspace_host: str) -> dict:
    """Call SCIM /Me to get user email and groups. Cached per token."""
    now = time.monotonic()
    if user_token in _scim_cache:
        cached, cached_at = _scim_cache[user_token]
        if now - cached_at < _SCIM_CACHE_TTL:
            return cached

    if not re.match(r'^[\w\-\.]+$', workspace_host):
        raise ValueError(f"Invalid workspace host: {workspace_host!r}")

    url = f"https://{workspace_host}/api/2.0/preview/scim/v2/Me"
    headers = {"Authorization": f"Bearer {user_token}"}
    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    result = {
        "email": data.get("userName", ""),
        "groups": [
            g.get("display", "") for g in data.get("groups", [])
            if g.get("display")
        ],
        "display_name": data.get("displayName", data.get("userName", "")),
    }
    _scim_cache[user_token] = (result, now)
    return result


# ---------------------------------------------------------------------------
# Identity secret (cached for process lifetime, thread-safe)
# ---------------------------------------------------------------------------

_identity_secret: Optional[bytes] = None
_secret_lock = threading.Lock()


def get_identity_secret(scope: str = "echostar-identity",
                        key: str = "hmac-secret") -> bytes:
    global _identity_secret
    if _identity_secret is not None:
        return _identity_secret
    with _secret_lock:
        if _identity_secret is not None:
            return _identity_secret
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            secret_resp = w.secrets.get_secret(scope=scope, key=key)
            _identity_secret = base64.b64decode(secret_resp.value)
            return _identity_secret
        except Exception as e:
            logger.error(f"Cannot retrieve identity secret: {e}")
            raise


# ---------------------------------------------------------------------------
# Request context builder
# ---------------------------------------------------------------------------

def build_request_context(user_token: str, workspace_host: str,
                          persona: str, session_id: str) -> Optional[str]:
    """Build a signed RequestContext JSON string from a user OAuth token."""
    try:
        user_info = get_user_info(user_token, workspace_host)
        ctx = RequestContext.create(
            user_email=user_info["email"],
            user_groups=user_info["groups"],
            persona=persona,
            session_id=session_id,
            secret=get_identity_secret(),
            ttl_minutes=15,
        )
        return ctx.to_json()
    except Exception as e:
        logger.error(f"Failed to build request context: {e}")
        return None


# ---------------------------------------------------------------------------
# Serving endpoint invocation
# ---------------------------------------------------------------------------

def query_serving_endpoint(
    endpoint_name: str,
    messages: list[dict[str, str]],
    max_tokens: int = 1024,
    persona: str = "customer_care",
    user_token: Optional[str] = None,
    workspace_host: Optional[str] = None,
    session_id: str = "",
) -> dict[str, str]:
    """Call a serving endpoint with identity context if available.

    Returns the last assistant message dict {"role": "assistant", "content": "..."}.
    """
    from mlflow.deployments import get_deploy_client

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


# ---------------------------------------------------------------------------
# Persona definitions (source of truth for both apps)
# ---------------------------------------------------------------------------

PERSONAS = {
    "customer_care": {
        "label": "Customer Support",
        "description": "Individual customer billing inquiries, disputes, and plan questions.",
        "icon": "👤",
    },
    "finance_ops": {
        "label": "Finance & Analytics",
        "description": "Fleet-wide billing analytics, revenue attribution, AR health.",
        "icon": "📊",
    },
    "executive": {
        "label": "Executive View",
        "description": "High-level billing performance summaries for leadership.",
        "icon": "🎯",
    },
    "technical": {
        "label": "Platform Engineering",
        "description": "Pipeline health, DBU costs, job reliability diagnostics.",
        "icon": "⚙️",
    },
}
