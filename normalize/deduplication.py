import json
import os
import time
import hashlib

from typing import Optional, List
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from urllib.parse import urlparse
import re

from database.connection import get_conn

from es.es_connection import get_es_connection
from elasticsearch import helpers

model = SentenceTransformer('all-MiniLM-L6-v2')
SIMILARITY_THRESHOLD = 0.8  # Adjust depending on how strict you want deduplication
brand_name_mapping = {'louis vuitton': 'Louis Vuitton', 'adidas': 'Adidas', 'gucci': 'Gucci', 'nike': 'Nike'}

es_index = "ebay_canonical"
es = get_es_connection()


@dataclass
class CanonicalItem:
    hash_key: str
    brand: str
    title: str
    category: Optional[str]
    size_norm: Optional[str]
    color_norm: Optional[str]
    image_url: str
    min_price_cents: int
    currency: str
    seller_urls: List[str]
    source_ids: List[int]

def create_hash_key(canonical_string) -> str:
    """Create unique hash key for grouping duplicates"""

    # Generate hash
    return hashlib.md5(canonical_string.encode('utf-8')).hexdigest()

def normalize_text(text):
    """Lowercase, remove punctuation, extra spaces."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9 ]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def canonical_string(item):
    """Concatenate normalized title, brand, and size."""
    title = normalize_text(item['title'])
    brand = normalize_text(item['brand'])
    size = item.get('size', '')
    return f"{brand}|{title}|{size}"

def normalize_prices(group):
    """Normalize prices within a group (min-max scaling) and remove duplicates."""
    prices = sorted(set(item['price_cents'] for item in group))
    min_p, max_p = min(prices), max(prices)
    for item in group:
        item['normalized_price'] = (
            (item['price_cents'] - min_p) / (max_p - min_p)
            if max_p > min_p else 0.0
        )
    return group

def remove_duplicates(group, canonical_string):
    """Remove duplicate items from a group."""
    deduplicated_group = []
    price_list = []
    for item in group:
        price = item['price_cents']
        if price not in price_list:
            price_list.append(price)
            item['hash_key'] = create_hash_key(canonical_string)
            deduplicated_group.append(item)
    return deduplicated_group

def extract_image_host(image_url):
    """Extract host from image URL for grouping"""
    print(image_url)
    if not image_url:
        return "unknown"

    try:
        parsed = urlparse(image_url)
        # Get domain without subdomain for better grouping
        domain_parts = parsed.netloc.split('.')
        if len(domain_parts) >= 2:
            return f"{domain_parts[-2]}.{domain_parts[-1]}"
        return parsed.netloc
    except:
        return "unknown"


def deduplicate_items(items):
    """
    Group similar items into canonical products based on embeddings.
    Returns: dict of canonical_key -> list of variations
    """
    # Step 1: Prepare canonical strings and embeddings
    canonical_texts = [canonical_string(item) for item in items]
    embeddings = model.encode(canonical_texts)

    # Step 2: Initialize groups
    canonical_groups = []
    assigned = [False] * len(items)

    # Step 3: Compare embeddings
    for i, emb in enumerate(embeddings):
        if assigned[i]:
            continue
        group = [items[i]]
        assigned[i] = True

        for j in range(i+1, len(embeddings)):
            if assigned[j]:
                continue
            sim = cosine_similarity([emb], [embeddings[j]])[0][0]
            if sim >= SIMILARITY_THRESHOLD:
                group.append(items[j])
                assigned[j] = True

        canonical_groups.append(group)

    # Step 4: Create canonical keys
    result = {}
    for group in canonical_groups:
        # Pick first item's canonical string as key
        #print("group = ", group)
        key = canonical_string(group[0])
        group = remove_duplicates(group, key)
        result[key] = group

    return result

def get_source_items():
    """Fetch all source items from database"""
    db_url = os.getenv('DATABASE_URL',
                       "postgresql://neondb_owner:npg_5LdJSKuC8bFY@ep-damp-field-aey694y3-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")
    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    id,
                    marketplace_code,
                    source_item_id,
                    title,
                    brand,
                    condition,
                    price_cents,
                    currency,
                    image_url,
                    seller_url,
                    size,
                    color,
                    category
                FROM item_source 
                WHERE price_cents > 0 
                ORDER BY id
            """)
            return [dict(row) for row in cur.fetchall()]

def create_canonical_item(group) -> CanonicalItem:
    """Create canonical item from a group of duplicates"""
    # Use first item as base
    base_item = group[0]
    hash_key = base_item['hash_key']

    # Find item with minimum price
    min_price_item = min(group, key=lambda x: x['price_cents'] or float('inf'))

    # Aggregate seller URLs
    seller_urls = [item['seller_url'] for item in group if item['seller_url']]
    source_ids = [item['id'] for item in group]

    # Choose best title (longest non-empty one)
    best_title = max(
        (item['title'] for item in group if item['title']),
        key=len,
        default=""
    )

    # Choose best brand (most frequent non-empty one)
    brands = [item['brand'] for item in group if item['brand']]
    best_brand = max(set(brands), key=brands.count) if brands else None

    return CanonicalItem(
        hash_key=hash_key,
        brand=best_brand,
        title=best_title,
        category=base_item['category'],
        size_norm=base_item['size'],  # TODO: Implement size normalization
        color_norm=base_item['color'],  # TODO: Implement color normalization
        image_url=base_item['image_url'],
        min_price_cents=min_price_item['price_cents'],
        currency=min_price_item['currency'],
        seller_urls=seller_urls,
        source_ids=source_ids
    )

def save_canonical_items(canonical_items, source_id_canonical_id):
    with get_conn() as connection:
        with connection.cursor() as cur:
            for item in canonical_items:
                # Insert canonical item
                cur.execute("""
                    INSERT INTO item_canonical 
                    (hash_key, brand, title, category, size_norm, color_norm, 
                     image_url, min_price_cents, currency)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    item.hash_key,
                    item.brand,
                    item.title,
                    item.category,
                    item.size_norm,
                    item.color_norm,
                    item.image_url,
                    item.min_price_cents,
                    item.currency
                ))

                canonical_id = cur.fetchone()[0]

                # Insert links to source items
                for i, source_id in enumerate(item.source_ids):
                    source_id_canonical_id[source_id] = canonical_id
                    # Get source item details for this link
                    cur.execute("""
                        SELECT price_cents, seller_url 
                        FROM item_source 
                        WHERE id = %s
                    """, (source_id,))

                    source_data = cur.fetchone()
                    if source_data:
                        price_cents, seller_url = source_data

                        cur.execute("""
                            INSERT INTO item_links 
                            (canonical_id, source_id, is_primary, price_cents, seller_url)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            canonical_id,
                            source_id,
                            i == 0,  # First item is primary
                            price_cents,
                            seller_url
                        ))


items = [
    {"title": "Louis Vuitton Bag XL", "brand": "Louis Vuitton", "price": 1000, "condition": "New", "seller": "A", "size": "XL"},
    {"title": "LV Bag Large", "brand": "Louis Vuitton", "price": 1020, "condition": "New", "seller": "B", "size": "XL"},
    {"title": "Gucci Leather Bag", "brand": "Gucci", "price": 950, "condition": "Used", "seller": "C", "size": ""},
]

def index_canonical_products(canonical_dict):
    operations = []
    for key, group in canonical_dict.items():
        brand, title, size = key.split('|')
        embedding = model.encode(title).tolist()
        doc = {
            "canonical_key": key,
            "brand": brand,
            "title": title,
            "size": size,
            "embedding": embedding,
            "items": group
        }

        # Add to bulk operations
        operations.append({
            "_op_type": "index",
            "_index": es_index,
            "_id": key,
            "_source": doc
        })

    # Execute bulk index
    if operations:
        response = helpers.bulk(es, operations)
        print("response: ", response)
        if response['errors']:
            print(f"Errors during indexing: {response}")
        else:
            print(f"Indexed {len(items)} canonical items")
    es.indices.refresh(index=es_index)

def mapping_index():
    """Create index from mapping JSON safely."""
    print(os.getcwd())
    with open("deduped_products.json", "r") as f:
        mapping = json.load(f)



    # Use ignore=400 to avoid exception if exists (safe in dev)
    es.indices.create(index=es_index, body=mapping, ignore=400)
    #es.indices.refresh(index=es_index)
    #print(f"✅ Created or verified index '{self.es_index}'")
    #print(self.es.indices.get_mapping(index=self.es_index))

def delete_index():
    """Delete the index safely and wait until it's really gone."""
    if es.indices.exists(index=es_index):
        print(f"🧹 Deleting index '{es_index}' ...")
        es.indices.delete(index=es_index)

        # Wait for deletion to complete
        while es.indices.exists(index=es_index):
            time.sleep(0.5)

        print("💥 Index deleted completely!")
    else:
        print(f"ℹ️ Index '{es_index}' does not exist, skipping delete.")

def build_es_query(user_query):
    q_lower = user_query.lower()
    cheap = any(word in q_lower for word in ["cheap", "affordable", "budget", "under"])
    luxury = any(word in q_lower for word in ["luxury", "costliest"])
    range = any(word in q_lower for word in ["range", "between"])
    color = next((c for c in ["red", "blue", "black", "white"] if c in q_lower), None)
    brand = next((b for b in ["louis vuitton", "gucci", "nike", "adidas"] if b in q_lower), None)

    # Step 2: Create semantic embedding
    query_vector = model.encode(user_query).tolist()
    # Step 3: Build ES query dynamically
    nested_filters = []
    filters = []
    if color:
        nested_filters.append({"term": {"items.color.keyword": color}})
    if brand:
        nested_filters.append({"term": {"items.brand.keyword": brand_name_mapping.get(brand, brand)}})
    # Cheap / affordable filter
    if cheap:
        numbers = re.findall(r'-?\d+\.?\d*', q_lower)
        if numbers:
            price = int(numbers[0])
        else:
            price = 40000
        filters.append({"nested":{
            "path": "items",
            "query": {"range": {"items.price_cents": {"lte": price}}},
            "inner_hits": {
                "size": 100
            }
        }
        })
        nested_filters.append({"range": {"items.price_cents": {"lte": price}}})
    if luxury:
        price_filter = "gte"
        numbers = re.findall(r'-?\d+\.?\d*', q_lower)
        if numbers:
            price = int(numbers[0])
        else:
            price = 75000
        nested_filters.append({"range": {"items.price_cents": {"gte": price}}})
    if range:
        price_filter = "gte"
        numbers = re.findall(r'-?\d+\.?\d*', q_lower)
        if numbers:
            low_price = int(numbers[0])
            high_price = int(numbers[1])
        else:
            low_price = 0
            high_price = 100000
        nested_filters.append({"range": {"items.price_cents": {"lte": high_price, "gte": low_price}}})



    must_terms = []
    if "bag" in q_lower:
        nested_filters.append({"match_phrase": {"items.category": "Clothing"}})
    nested_filter = {
                        "nested": {
                        "path": "items",
                        "query": {
                          "bool": {
                            "must": nested_filters
                          }
                        },
                        "inner_hits": {
                          "size": 100
                        }
                        }
                    }
    nested_filter_new = {
        "nested": {
            "path": "items",
            "query": {
                "bool": {
                    "should": nested_filters,
                    "minimum_should_match": 1
                }
            },
            "inner_hits": {
                "size": 100
            }
        }
    }
    print("filters : ",nested_filter)
    base_query = {
        "bool": {
                        "should": [
                            {"multi_match": {
                                "query": user_query,
                                "fields": ["title^3", "brand", "category"],
                                "fuzziness": "AUTO"
                            }}
                        ],
                        "filter": nested_filter
        }
    }
    query = {
        "query": base_query
    }
    query_new = {
      "query": {
        "nested": {
          "path": "items",
          "query": {
            "exists": { "field": "items.price_cents" }
          }
        }
      }
}

    return query

def search_index_new(query_text):
    #query_text = "bag"
    query = build_es_query(query_text)
    res = es.search(index=es_index, body=query)
    print(res)
    for hit in res['hits']['hits']:
        for item in hit['inner_hits']['items']['hits']['hits']:
            print(f"- {item['_source']['title']} | {item['_source']['brand']} | {item['_source']['currency']}{item['_source']['price_cents']} | score={item['_score']:.2f}")
            #print(item)
    return res['hits']['hits']

def search_index():

    res = es.search(index=es_index, query={"match_all": {}})
    #res = self.es.search(index=self.es_index, body=query)
    for hit in res['hits']['hits']:
        print(hit['_source'])

items = get_source_items()
#print(items)
deduped = deduplicate_items(items)
canonical_items = []
for dedupe_key, deduped_value in deduped.items():
    canonical_items.append(create_canonical_item(deduped_value))
source_id_canonical_id = {}
save_canonical_items(canonical_items, source_id_canonical_id)
delete_index()
mapping_index()
index_canonical_products(deduped)

#search_index_new("Men Clothing")
#search_index()

#print(es.indices.get_mapping(index=es_index)[es_index]["mappings"]["properties"])
#imageUrl = "http://i.ebayimg.sandbox.ebay.com/images/g/g2gAAeSweZNoiXjU/s-l225.jpg"
#print(extract_image_host(imageUrl))
