"""Authentication, API key hashing, rotation, and log redaction utilities."""

from __future__ import annotations

import hashlib
import re
import secrets
import uuid
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple

from app.db import Database

DUMMY_UUID = "00000000-0000-0000-0000-000000000000"
_KEY_REDACT_RE = re.compile(r"(soup_sk_[A-Za-z0-9_\-]+|Bearer\s+[A-Za-z0-9_\-\.]+)", re.IGNORECASE)


class AuthError(Exception):
    """Raised on authentication or security policy errors."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class AuthContext:
    key_id: str
    project_id: str
    key_hash: str


def generate_api_key() -> Tuple[str, str]:
    """Return `(raw_key, sha256_hex_hash)` with 256 bits of randomness."""
    raw_key = f"soup_sk_{secrets.token_urlsafe(32)}"
    return raw_key, hash_api_key(raw_key)


def hash_api_key(raw_key: str) -> str:
    """Compute SHA-256 hex digest of a raw API key."""
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def redact_secrets(text: str) -> str:
    """Redact API keys and Bearer tokens from log strings."""
    return _KEY_REDACT_RE.sub("[REDACTED_KEY]", text)


def normalize_uuid(value: str) -> Optional[str]:
    """Return canonical lowercase UUID string if valid, else None."""
    try:
        parsed = uuid.UUID(str(value))
        return str(parsed)
    except (ValueError, TypeError, AttributeError):
        return None


def extract_key_from_request(
    headers: Mapping[str, str],
    query_params: Mapping[str, str],
) -> Tuple[str, str]:
    """Extract API key from headers while strictly forbidding query-string keys."""
    for forbidden_param in ("api_key", "apikey", "key", "token", "access_token"):
        if forbidden_param in query_params:
            raise AuthError(
                400,
                "API key in query parameters is forbidden; use X-API-Key or Authorization header",
            )

    raw_key = headers.get("x-api-key", "").strip()
    if not raw_key:
        auth_header = headers.get("authorization", "").strip()
        auth_lower = auth_header.lower()
        if auth_lower.startswith("bearer "):
            raw_key = auth_header[7:].strip()
        elif auth_lower.startswith("token "):
            raw_key = auth_header[6:].strip()
        elif auth_lower.startswith("api-key "):
            raw_key = auth_header[8:].strip()
        elif auth_lower.startswith("apikey "):
            raw_key = auth_header[7:].strip()
        elif auth_header:
            raw_key = auth_header

    if not raw_key:
        raise AuthError(401, "Missing API key")

    return raw_key, hash_api_key(raw_key)


async def verify_api_key(
    db: Database,
    headers: Mapping[str, str],
    query_params: Mapping[str, str],
    revoked_hashes: Optional[set[str]] = None,
) -> AuthContext:
    """Authenticate request headers against PostgreSQL `api_keys` table."""
    _, key_hash = extract_key_from_request(headers, query_params)

    if revoked_hashes is not None and key_hash in revoked_hashes:
        raise AuthError(401, "Invalid or revoked API key")

    row = await db.fetch_one(
        "SELECT id, project_id, revoked_at FROM api_keys WHERE key_hash = $1",
        key_hash,
    )
    if not row or row.get("revoked_at") is not None:
        if revoked_hashes is not None and row and row.get("revoked_at") is not None:
            revoked_hashes.add(key_hash)
        raise AuthError(401, "Invalid or revoked API key")

    return AuthContext(
        key_id=str(row["id"]),
        project_id=str(row["project_id"]),
        key_hash=key_hash,
    )
