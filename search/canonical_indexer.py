import os
import json
from elasticsearch import Elasticsearch
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Dict, List

class CanonicalIndexer:
    def __init__(self):
        self.db_url = os.getenv("DATABASE_URL")
        self.es_url = os.getenv("ELASTICSEARCH_URL")
        self.es_api_key = os.getenv("ELASTICSEARCH_API_KEY")
        self.es_index = os.getenv("ELASTICSEARCH_INDEX", "products")
        
        # Initialize Elasticsearch
        self.es = Elasticsearch(
            self.es_url,
            api_key=self.es_api_key
        )
        
        # Database connection
        self.conn = psycopg2.connect(self.db_url, cursor_factory=RealDictCursor)
    
    def index_canonical_items(self, batch_size: int = 100):
        """Index canonical items to Elasticsearch"""
        with self.conn.cursor() as cur:
            # Get canonical items with their offers
            cur.execute("""
                SELECT 
                    ic.id,
                    ic.brand,
                    ic.title,
                    ic.category,
                    ic.image_url,
                    ic.min_price_cents,
                    ic.currency,
                    ic.last_seen,
                    COUNT(l.id) as offer_count,
                    ARRAY_AGG(
                        json_build_object(
                            'marketplace', s.marketplace_code,
                            'price_cents', s.price_cents,
                            'seller_url', s.seller_url,
                            'condition', s.condition
                        )
                    ) as offers
                FROM item_canonical ic
                JOIN item_links l ON l.canonical_id = ic.id AND l.active
                JOIN item_source s ON s.id = l.source_id
                GROUP BY ic.id
                ORDER BY ic.id
                LIMIT %s
            """, (batch_size,))
            
            items = cur.fetchall()
            
            if not items:
                print("No items to index")
                return 0
            
            # Prepare bulk operations
            operations = []
            for item in items:
                # Document ID format: "can-{canonical_id}"
                doc_id = f"can-{item['id']}"
                
                # Prepare document
                doc = {
                    "id": item['id'],
                    "brand": item['brand'],
                    "title": item['title'],
                    "category": item['category'],
                    "image_url": item['image_url'],
                    "price_cents": item['min_price_cents'],
                    "currency": item['currency'],
                    "offer_count": item['offer_count'],
                    "offers": item['offers'],
                    "last_seen": item['last_seen'].isoformat() if item['last_seen'] else None
                }
                
                # Add to bulk operations
                operations.append({"index": {"_index": self.es_index, "_id": doc_id}})
                operations.append(doc)
            
            # Execute bulk index
            if operations:
                response = self.es.bulk(operations=operations)
                if response['errors']:
                    print(f"Errors during indexing: {response}")
                else:
                    print(f"Indexed {len(items)} canonical items")
            
            return len(items)
    
    def run_full_index(self):
        """Run full indexing of all canonical items"""
        total = 0
        while True:
            indexed = self.index_canonical_items()
            total += indexed
            if indexed == 0:
                break
        
        print(f"Total items indexed: {total}")
        
        # Refresh index
        self.es.indices.refresh(index=self.es_index)
        
        # Get count
        count = self.es.count(index=self.es_index)
        print(f"Documents in index: {count['count']}")
    
    def close(self):
        self.conn.close()
        self.es.close()

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    
    indexer = CanonicalIndexer()
    try:
        indexer.run_full_index()
    finally:
        indexer.close()
