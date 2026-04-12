"""Model serving utilities with identity context propagation.

Inlines the minimal RequestContext creation + SCIM lookup needed by the App
layer. The agent-side validation lives in notebooks/identity_utils.py; the
App only needs to CREATE signed contexts, never verify them.
"""

import hashlib
import hmac
import json
import logging
import re
import time
import uuid
import base64
import threading
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests
from mlflow.deployments import get_deploy_client

logger = logging.getLogger(__name__)

# Must match identity_utils.py for HMAC determinism (H11 fix)
_JSON_SEPARATORS = (",", ":")

# ---------------------------------------------------------------------------
# Inline identity helpers (App-side only — avoids notebooks/ import path)
# ---------------------------------------------------------------------------

@dataclass
class _RequestContext:
    """Minimal RequestContext for App-side creation + signing.

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

    def sign(self, secret: bytes) -> "_RequestContext":
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
# SCIM user info — cached per token with 5-minute TTL (H4 fix)
# ---------------------------------------------------------------------------

_scim_cache: dict[str, tuple[dict, float]] = {}
_SCIM_CACHE_TTL = 300  # 5 minutes


def _get_user_info(user_token: str, workspace_host: str) -> dict:
    """Call SCIM /Me to get user email and groups. Results cached per token."""
    now = time.monotonic()
    if user_token in _scim_cache:
        cached, cached_at = _scim_cache[user_token]
        if now - cached_at < _SCIM_CACHE_TTL:
            return cached

    # Validate host to prevent SSRF
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
            g.get("display", "")
            for g in data.get("groups", [])
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


def _get_secret() -> bytes:
    global _identity_secret
    if _identity_secret is not None:
        return _identity_secret
    with _secret_lock:
        if _identity_secret is not None:
            return _identity_secret
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            secret_resp = w.secrets.get_secret(
                scope="echostar-identity", key="hmac-secret"
            )
            _identity_secret = base64.b64decode(secret_resp.value)
            return _identity_secret
        except Exception as e:
            logger.error(f"Cannot retrieve identity secret: {e}")
            raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_request_context(user_token: str, workspace_host: str,
                          persona: str, session_id: str) -> Optional[str]:
    """Build a signed RequestContext JSON string from a user OAuth token."""
    try:
        user_info = _get_user_info(user_token, workspace_host)
        ctx = _RequestContext.create(
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
        raise RuntimeError(
            f"Could not connect to serving endpoint '{endpoint_name}'."
        ) from e
    except TimeoutError as e:
        raise RuntimeError(
            f"Request to serving endpoint '{endpoint_name}' timed out."
        ) from e

    if "messages" in res:
        return res["messages"][-1]
    elif "choices" in res:
        return res["choices"][0]["message"]
    raise RuntimeError(
        "Unexpected response format from serving endpoint. "
        "This app requires a Databricks agent serving endpoint or a "
        "foundation model endpoint with the chat task type."
    )
