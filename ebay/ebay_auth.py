

"""
eBay OAuth token helper (Client Credentials flow)

Usage:
  from ebay.token import get_token, auth_header
  token = get_token()
  headers = {"Authorization": f"Bearer {token}", ...}

Env (loaded from ../.env if present):
  EBAY_APP_ID=<client_id>
  EBAY_CERT_ID=<client_secret>
  EBAY_ENV=PRODUCTION  # or SANDBOX
  EBAY_SCOPE=https://api.ebay.com/oauth/api_scope  # optional, space-separated for multiple scopes
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Dict

import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_fixed

# Load env from ingestion/.env when running locally
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

CLIENT_ID = os.getenv("EBAY_APP_ID")
CLIENT_SECRET = os.getenv("EBAY_CERT_ID")
ENV = (os.getenv("EBAY_ENV") or "PRODUCTION").upper()
SCOPE = os.getenv("EBAY_SCOPE") or "https://api.ebay.com/oauth/api_scope"

if not CLIENT_ID or not CLIENT_SECRET:
    raise RuntimeError("EBAY_APP_ID / EBAY_CERT_ID not set. Fill them after eBay approval.")

OAUTH_URL = (
    "https://api.ebay.com/identity/v1/oauth2/token"
    if ENV == "PRODUCTION"
    else "https://api.sandbox.ebay.com/identity/v1/oauth2/token"
)

# Simple in-process cache
_cached_token: str | None = None
_cached_expiry: float = 0.0  # epoch seconds


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def _fetch_new_token() -> tuple[str, int]:
    """
    Request a fresh application access token.
    Returns (token, expires_in_seconds).
    """
    data = {
        "grant_type": "client_credentials",
        "scope": SCOPE,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(OAUTH_URL, auth=(CLIENT_ID, CLIENT_SECRET), data=data, headers=headers, timeout=20)
    resp.raise_for_status()
    j = resp.json()
    token = j.get("access_token")
    expires_in = int(j.get("expires_in", 0))
    if not token or not expires_in:
        raise RuntimeError(f"Failed to obtain eBay token: {j}")
    return token, expires_in


def get_token() -> str:
    """
    Returns a valid Bearer token. Uses a small cache with a 60s safety buffer.
    """
    global _cached_token, _cached_expiry
    now = time.time()
    if _cached_token and now < _cached_expiry - 60:
        return _cached_token

    token, expires_in = _fetch_new_token()
    _cached_token = token
    _cached_expiry = now + expires_in
    return token


def auth_header() -> Dict[str, str]:
    """Convenience header for requests."""
    return {"Authorization": f"Bearer {get_token()}"}


if __name__ == "__main__":
    t = get_token()
    remaining = int(_cached_expiry - time.time())
    print("access_token:", t[:24] + "…")
    print("expires_in_s:", max(0, remaining))
    print("env:", ENV)