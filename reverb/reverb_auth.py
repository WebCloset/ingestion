"""
Reverb API auth — Bearer token defined directly in code.

NOTE: This keeps things simple for a private project, but you should
avoid committing real tokens to any public repository.

Usage:
  from reverb.reverb_auth import get_token, auth_headers
  headers = auth_headers()
"""

from __future__ import annotations

from typing import Dict

# Hard-coded Reverb API token for this environment.
# This replaces the previous .env-based approach.
REVERB_BEARER_TOKEN = (
    "c3377b241666dd53480d3e88ba65be07c7cfbdb6fd04b90d69089234096b7a4e"
)


def get_token() -> str:
    """Returns the Reverb Bearer token."""
    return REVERB_BEARER_TOKEN


def auth_headers() -> Dict[str, str]:
    """Headers required for Reverb API (Accept-Version 3.0, HAL+JSON, Bearer)."""
    return {
        "Authorization": f"Bearer {get_token()}",
        "Accept": "application/hal+json",
        "Accept-Version": "3.0",
    }
