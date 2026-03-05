# reverb/config.py
"""
Reverb API configuration.
Env: REVERB_BEARER_TOKEN, REVERB_API_BASE (optional), REVERB_PER_PAGE, REVERB_MAX_PAGES
"""
import os

REVERB_API_BASE = os.getenv("REVERB_API_BASE", "https://api.reverb.com/api")
REVERB_LISTINGS_URL = f"{REVERB_API_BASE.rstrip('/')}/listings/all"

# Pagination
REVERB_PER_PAGE = int(os.getenv("REVERB_PER_PAGE", "50"))
REVERB_MAX_PAGES = int(os.getenv("REVERB_MAX_PAGES", "10"))

# API version (Reverb expects Accept-Version: 3.0)
REVERB_ACCEPT_VERSION = "3.0"
