"""
EchoStar Identity Propagation Utilities.

Provides RequestContext creation, HMAC signing/verification, SCIM user
info retrieval, UC tag resolution, and identity validation helpers.

Used by:
  - Databricks App layer (Dash/Gradio) to build signed contexts
  - agent.py to validate contexts and enforce identity requirements
  - Write tools to require user context before execution
"""

import hashlib
import hmac
import json
import uuid
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional
from functools import lru_cache

import requests
from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RequestContext — the signed identity envelope
# ---------------------------------------------------------------------------

@dataclass
class RequestContext:
    """Signed, time-bounded identity context propagated from App to Agent."""
    user_email: str
    user_groups: list[str]
    persona: str
    session_id: str
    request_id: str
    issued_at: str   # ISO 8601 UTC
    expires_at: str  # ISO 8601 UTC
    signature: str = ""

    def _signing_payload(self) -> bytes:
        d = {k: v for k, v in asdict(self).items() if k != "signature"}
        return json.dumps(d, sort_keys=True, default=str).encode("utf-8")

    def sign(self, secret: bytes) -> "RequestContext":
        self.signature = hmac.new(secret, self._signing_payload(), hashlib.sha256).hexdigest()
        return self

    def verify(self, secret: bytes) -> bool:
        expected = hmac.new(secret, self._signing_payload(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(self.signature, expected)

    def is_expired(self) -> bool:
        return datetime.now(timezone.utc) > datetime.fromisoformat(self.expires_at)

    def is_valid(self, secret: bytes) -> bool:
        return self.verify(secret) and not self.is_expired()

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True, default=str)

    @classmethod
    def from_json(cls, s: str) -> "RequestContext":
        return cls(**json.loads(s))

    @classmethod
    def create(
        cls,
        user_email: str,
        user_groups: list[str],
        persona: str,
        session_id: str,
        secret: bytes,
        ttl_minutes: int = 15,
    ) -> "RequestContext":
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
# SCIM user info retrieval
# ---------------------------------------------------------------------------

def get_user_info(user_token: str, workspace_host: str) -> dict:
    """Call SCIM /Me endpoint to get user email and groups from OAuth token.

    Returns:
        {"email": "...", "groups": ["group1", "group2"], "display_name": "..."}

    Raises:
        IdentityError if the call fails or returns unexpected data.
    """
    url = f"https://{workspace_host}/api/2.0/preview/scim/v2/Me"
    headers = {"Authorization": f"Bearer {user_token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        email = data.get("userName", "")
        groups = [
            g.get("display", "")
            for g in data.get("groups", [])
            if g.get("display")
        ]
        display_name = data.get("displayName", email)
        return {"email": email, "groups": groups, "display_name": display_name}
    except Exception as e:
        raise IdentityError(f"SCIM /Me call failed: {e}") from e


# ---------------------------------------------------------------------------
# Identity secret management
# ---------------------------------------------------------------------------

_IDENTITY_SECRET_CACHE: Optional[bytes] = None


def get_identity_secret(scope: str = "echostar-identity",
                        key: str = "hmac-secret") -> bytes:
    """Retrieve the HMAC signing secret from Databricks Secret Scope.

    In the App context, uses WorkspaceClient with SP credentials.
    Caches the result for the process lifetime.
    """
    global _IDENTITY_SECRET_CACHE
    if _IDENTITY_SECRET_CACHE is not None:
        return _IDENTITY_SECRET_CACHE
    try:
        w = WorkspaceClient()
        secret_str = w.secrets.get_secret(scope=scope, key=key)
        # SDK returns bytes or base64 depending on version
        if isinstance(secret_str, bytes):
            _IDENTITY_SECRET_CACHE = secret_str
        else:
            import base64
            _IDENTITY_SECRET_CACHE = base64.b64decode(secret_str.value)
        return _IDENTITY_SECRET_CACHE
    except Exception as e:
        raise IdentityError(f"Cannot retrieve identity secret from scope={scope} key={key}: {e}") from e


# ---------------------------------------------------------------------------
# Tag resolution
# ---------------------------------------------------------------------------

@dataclass
class AssetPolicy:
    """Resolved governance policy for a UC asset, derived from tags."""
    identity_mode: str = "none"         # required | preferred | none
    pii_level: str = "none"             # high | medium | low | none
    audit_required: bool = False
    allowed_personas: list[str] = field(default_factory=list)  # empty = all
    projection_policy: str = "full"     # full | redacted | minimal
    write_policy: str = "user_confirmed"  # user_confirmed | sp_allowed | blocked
    data_classification: str = "internal"


def resolve_asset_policy(catalog: str, schema: str, asset_name: str,
                         asset_tags: Optional[dict] = None) -> AssetPolicy:
    """Resolve governance policy for a UC asset from its tags.

    Checks object-level tags first, then falls back to schema, then catalog.
    If asset_tags is provided directly, uses those (for testing / caching).
    """
    if asset_tags is None:
        asset_tags = _fetch_tags(catalog, schema, asset_name)

    return AssetPolicy(
        identity_mode=asset_tags.get("gov.identity.mode", "none"),
        pii_level=asset_tags.get("gov.pii.level", "none"),
        audit_required=asset_tags.get("gov.audit.required", "false").lower() == "true",
        allowed_personas=[
            p.strip() for p in asset_tags.get("gov.allowed.personas", "").split(",")
            if p.strip()
        ],
        projection_policy=asset_tags.get("gov.projection.policy", "full"),
        write_policy=asset_tags.get("gov.write.policy", "user_confirmed"),
        data_classification=asset_tags.get("gov.data.classification", "internal"),
    )


def _fetch_tags(catalog: str, schema: str, asset_name: str) -> dict:
    """Fetch UC tags for an asset with inheritance (object > schema > catalog).

    Uses information_schema.table_tags (available in Databricks).
    Falls back to empty dict on failure.
    """
    try:
        w = WorkspaceClient()
        # Try object-level tags first
        tags = _get_object_tags(w, catalog, schema, asset_name)
        if not tags:
            tags = _get_schema_tags(w, catalog, schema)
        if not tags:
            tags = _get_catalog_tags(w, catalog)
        return tags
    except Exception as e:
        logger.warning(f"Tag fetch failed for {catalog}.{schema}.{asset_name}: {e}")
        return {}


def _get_object_tags(w: WorkspaceClient, catalog: str, schema: str, name: str) -> dict:
    """Query UC tags for a specific table/view/function."""
    try:
        # Use Statement Execution API to query information_schema
        from databricks.sdk.service.sql import StatementState
        result = w.statement_execution.execute_statement(
            statement=f"""
                SELECT tag_name, tag_value
                FROM {catalog}.information_schema.table_tags
                WHERE schema_name = '{schema}'
                  AND table_name = '{name}'
                  AND tag_name LIKE 'gov.%'
            """,
            warehouse_id=_get_warehouse_id(),
            wait_timeout="10s",
        )
        if result.status and result.status.state == StatementState.SUCCEEDED and result.result:
            rows = result.result.data_array or []
            return {r[0]: r[1] for r in rows if r[0] and r[1]}
    except Exception as e:
        logger.debug(f"Object tag query failed: {e}")
    return {}


def _get_schema_tags(w: WorkspaceClient, catalog: str, schema: str) -> dict:
    """Query UC tags for a schema (inheritable defaults)."""
    try:
        from databricks.sdk.service.sql import StatementState
        result = w.statement_execution.execute_statement(
            statement=f"""
                SELECT tag_name, tag_value
                FROM {catalog}.information_schema.schema_tags
                WHERE schema_name = '{schema}'
                  AND tag_name LIKE 'gov.%'
            """,
            warehouse_id=_get_warehouse_id(),
            wait_timeout="10s",
        )
        if result.status and result.status.state == StatementState.SUCCEEDED and result.result:
            rows = result.result.data_array or []
            return {r[0]: r[1] for r in rows if r[0] and r[1]}
    except Exception as e:
        logger.debug(f"Schema tag query failed: {e}")
    return {}


def _get_catalog_tags(w: WorkspaceClient, catalog: str) -> dict:
    """Query UC tags for a catalog (inheritable defaults)."""
    try:
        from databricks.sdk.service.sql import StatementState
        result = w.statement_execution.execute_statement(
            statement=f"""
                SELECT tag_name, tag_value
                FROM {catalog}.information_schema.catalog_tags
                WHERE tag_name LIKE 'gov.%'
            """,
            warehouse_id=_get_warehouse_id(),
            wait_timeout="10s",
        )
        if result.status and result.status.state == StatementState.SUCCEEDED and result.result:
            rows = result.result.data_array or []
            return {r[0]: r[1] for r in rows if r[0] and r[1]}
    except Exception as e:
        logger.debug(f"Catalog tag query failed: {e}")
    return {}


def _get_warehouse_id() -> str:
    """Get warehouse ID from config or environment."""
    import os
    wh = os.environ.get("ECHOSTAR_WAREHOUSE_ID", "")
    if not wh:
        try:
            from mlflow.models import ModelConfig
            cfg = ModelConfig(development_config="config.yaml").to_dict()
            wh = cfg.get("warehouse_id", "")
        except Exception:
            pass
    return wh


# ---------------------------------------------------------------------------
# Pre-tool authorization guard
# ---------------------------------------------------------------------------

class IdentityError(Exception):
    """Raised when identity validation fails."""
    pass


class AuthorizationError(Exception):
    """Raised when authorization check fails."""
    pass


def validate_request_context(raw_json: Optional[str], secret: bytes) -> RequestContext:
    """Parse, verify signature, and check expiry of a serialized RequestContext.

    Raises IdentityError on any failure.
    """
    if not raw_json:
        raise IdentityError("No request context provided. User identity required.")
    try:
        ctx = RequestContext.from_json(raw_json)
    except Exception as e:
        raise IdentityError(f"Malformed request context: {e}") from e
    if not ctx.verify(secret):
        raise IdentityError("Request context signature invalid. Possible tampering.")
    if ctx.is_expired():
        raise IdentityError(
            f"Request context expired at {ctx.expires_at}. "
            "Re-authenticate and retry."
        )
    return ctx


def check_tool_authorization(
    tool_name: str,
    policy: AssetPolicy,
    ctx: Optional[RequestContext],
) -> None:
    """Enforce pre-tool authorization. Raises AuthorizationError on deny.

    Checks:
    1. If identity is required and no valid context exists -> block.
    2. If allowed_personas is set and current persona is not listed -> block.
    3. Logs the access check if audit_required.
    """
    # Check identity requirement
    if policy.identity_mode == "required":
        if ctx is None:
            raise AuthorizationError(
                f"BLOCKED: Tool '{tool_name}' requires user identity context. "
                "No valid RequestContext found."
            )
    # Check persona restriction
    if policy.allowed_personas and ctx:
        if ctx.persona not in policy.allowed_personas:
            raise AuthorizationError(
                f"BLOCKED: Persona '{ctx.persona}' is not authorized for tool '{tool_name}'. "
                f"Allowed: {policy.allowed_personas}"
            )
    # Log if audit required
    if policy.audit_required:
        user = ctx.user_email if ctx else "UNKNOWN"
        logger.info(f"AUDIT: tool={tool_name} user={user} persona={ctx.persona if ctx else 'NONE'} policy={policy.identity_mode}")


def require_user_context(ctx: Optional[RequestContext], action: str) -> RequestContext:
    """Hard guard: require a valid RequestContext or raise.

    Use at the top of any write tool or sensitive operation.
    """
    if ctx is None:
        raise AuthorizationError(
            f"BLOCKED: Action '{action}' requires authenticated user context. "
            "Cannot execute under service principal alone."
        )
    return ctx


# ---------------------------------------------------------------------------
# Persona-to-group binding validation
# ---------------------------------------------------------------------------

def validate_persona_for_user(
    persona: str,
    user_groups: list[str],
    persona_group_map: dict[str, list[str]],
) -> bool:
    """Check if the user's groups allow them to use the requested persona.

    persona_group_map: {"executive": ["billing_leadership", "c_suite"], ...}
    Empty list means no group restriction (anyone can use the persona).
    """
    required_groups = persona_group_map.get(persona, [])
    if not required_groups:
        return True  # No restriction
    return bool(set(user_groups) & set(required_groups))
