import requests
from bs4 import BeautifulSoup
import re

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9"
}

def extract_brand_from_amazon(url):
    resp = requests.get(url, headers=HEADERS, timeout=15)

    if resp.status_code != 200:
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1️⃣ Preferred: "Visit the ___ Store"
    byline = soup.find(id="bylineInfo")
    if byline:
        text = byline.get_text(strip=True)
        match = re.search(r"Visit the (.+?) Store", text)
        if match:
            return match.group(1)
        match = re.search(r"Brand\s*:\s*(.+)", text, re.I)
        if match:
            return match.group(1)

    # 2️⃣ Brand: Nike (product details)
    for label in soup.find_all(string=re.compile(r"^\s*Brand\s*$", re.I)):
        parent = label.parent
        if parent:
            # Common patterns
            value = parent.find_next_sibling()
            if value:
                brand = value.get_text(strip=True)
                if brand:
                    return url, brand

    # 2️⃣ Fallback: meta / JSON-LD brand
    scripts = soup.find_all("script", type="application/ld+json")
    for s in scripts:
        try:
            data = s.string
            if data and '"brand"' in data:
                match = re.search(r'"name"\s*:\s*"([^"]+)"', data)
                if match:
                    return match.group(1)
        except Exception:
            pass

    return None
print(extract_brand_from_amazon("https://www.amazon.com/dp/B00K5CVSLI"))