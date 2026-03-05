import os
import re
import pandas as pd

import product_attributes_parser
from database import connection

# Path where all category CSVs are stored
DATASET_DIR = os.path.join(os.getcwd(), "amazon_dataset")  # change this to your folder name
print(os.listdir(DATASET_DIR))
# List all CSV files in folder
csv_files = [f for f in os.listdir(DATASET_DIR) if f.endswith(".csv")]

products = []

categories = dict()

UPSERT_SQL = """
INSERT INTO item_source (
  marketplace_code, source_item_id, title, brand, category, price_cents, currency, image_url, seller_url
)
VALUES (
  %(marketplace_code)s, %(source_item_id)s, %(title)s, %(brand)s, %(category)s, %(price_cents)s, %(currency)s, %(image_url)s, %(seller_url)s
)
ON CONFLICT (marketplace_code, source_item_id) DO UPDATE SET
  title = EXCLUDED.title,
  brand = EXCLUDED.brand,
  category = EXCLUDED.category,
  price_cents = EXCLUDED.price_cents,
  currency = EXCLUDED.currency,
  image_url = EXCLUDED.image_url,
  seller_url = EXCLUDED.seller_url;
"""

def split_currency_and_amount(price):
    match = re.match(r"(\D+)(\d+)", price)
    currency = match.group(1)
    amount = int(match.group(2))
    return currency, amount


def canonicalize(row, category_file):
    """Convert a row into a normalized product dictionary."""

    #"ratings": float(row.get("ratings", 0)) if row.get("ratings") and type(row.get("ratings")) is int else 0.0,
    #"no_of_ratings": int(row.get("no of ratings", 0)) if row.get("no of ratings") else 0,

    currency, amount = split_currency_and_amount(row["actual_price"])
    return {
        "marketplace_code": "amazon",
        "source_item_id": row.get("name", ""),
        "title": row.get("name", ""),
        "category": row.get("main_category", ""),
        "sub_category": row.get("sub_category", ""),
        "image_url": row.get("image", ""),
        "seller_url": row.get("link", ""),
        "price_cents": str(amount),
        "currency": currency
    }

def canonicalize_set_2(row, category_file):
    """Convert a row into a normalized product dictionary."""

    #"ratings": float(row.get("ratings", 0)) if row.get("ratings") and type(row.get("ratings")) is int else 0.0,
    #"no_of_ratings": int(row.get("no of ratings", 0)) if row.get("no of ratings") else 0,

    #currency, amount = split_currency_and_amount(row["price"])
    return {
        "marketplace_code": "amazon",
        "source_item_id": row.get("asin", ""),
        "title": row.get("title", ""),
        "category": categories.get(row.get("category_id", "")),
        "image_url": row.get("imgUrl", ""),
        "seller_url": row.get("productURL", ""),
        "price_cents": int(row["price"]),
        "currency": "$"
    }

# -------------------------------
# LOAD CATEGORY ID NAME MAPPING
# -------------------------------
for file in csv_files:
    file_path = os.path.join(DATASET_DIR, file)
    if "categories" in file:
        try:
            df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="latin1", low_memory=False)

        df.fillna("", inplace=True)
        for _, row in df.iterrows():
            categories[row["id"]] = row["category_name"]

# -------------------------------
# LOAD AND PARSE EACH FILE
# -------------------------------
for file in csv_files:
    file_path = os.path.join(DATASET_DIR, file)
    if "Sports Shoes" in file or "categories" in file or "products" in file:
        continue

    try:
        df = pd.read_csv(file_path, encoding="utf-8", low_memory=False, nrows=100)
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding="latin1", low_memory=False, nrows=100)

    df.fillna("", inplace=True)

    for _, row in df.iterrows():
        if row["category_id"] == 110 or row["category_id"] == 116:
            product_obj = canonicalize_set_2(row, file)
            product_attributes = product_attributes_parser.parse_product_attributes(row.get("title", ""), url=row.get("productURL", ""))
            if product_attributes is not None:
                product_obj.update(product_attributes)
                products.append(product_obj)

    connection.upsert_rows(products, UPSERT_SQL)


print(f"Total products parsed: {len(products)}")
print(categories)
# Print sample 2 products
print(products)
