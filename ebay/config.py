# ebay/config.py
import os

# eBay RSS Configuration (no authentication required)
EBAY_RSS_BASE = "https://www.ebay.com/sch/i.html"

# Your database credentials
EBAY_APP_ID = os.getenv("EBAY_APP_ID")

# Search terms for fashion items
SEARCH_TERMS = [
    "Nike Air Jordan 1",
    "Adidas Yeezy 350", 
    "Supreme hoodie",
    "Gucci sneakers",
    "Louis Vuitton bag"
]

# Simple condition mapping
CONDITION_MAPPING = {
    "new": "new",
    "used": "good",
    "pre-owned": "good",
    "refurbished": "good"
}
