import re
import hashlib
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import RealDictCursor
import os

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

class DedupeProcessor:
    def __init__(self, db_connection_string: str):
        self.db_url = db_connection_string
    
    def normalize_title(self, title: str) -> str:
        """Normalize title for deduplication"""
        if not title:
            return ""
        
        # Convert to lowercase
        normalized = title.lower()
        
        # Remove common marketplace prefixes/suffixes
        normalized = re.sub(r'\b(pre-owned|used|authentic|genuine|vintage|rare)\b', '', normalized)
        
        # Remove size indicators (we handle size separately)
        normalized = re.sub(r'\b(size|sz)\s*\d+\b', '', normalized)
        
        # Remove condition words
        normalized = re.sub(r'\b(excellent|good|fair|poor|condition)\b', '', normalized)
        
        # Remove extra whitespace and special chars
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def extract_image_host(self, image_url: str) -> str:
        """Extract host from image URL for grouping"""
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
    
    def create_hash_key(self, brand: str, title: str, image_host: str) -> str:
        """Create unique hash key for grouping duplicates"""
        # Normalize brand
        brand_norm = (brand or "").lower().strip()
        
        # Normalize title
        title_norm = self.normalize_title(title)
        
        # Create composite key
        composite = f"{brand_norm}|{title_norm}|{image_host}"
        
        # Generate hash
        return hashlib.md5(composite.encode('utf-8')).hexdigest()
    
    def get_source_items(self) -> List[Dict[str, Any]]:
        """Fetch all source items from database"""
        with psycopg2.connect(self.db_url) as conn:
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
    
    def group_duplicates(self, source_items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group items by hash key to identify duplicates"""
        groups = {}
        
        for item in source_items:
            image_host = self.extract_image_host(item['image_url'])
            hash_key = self.create_hash_key(
                item['brand'], 
                item['title'], 
                image_host
            )
            
            if hash_key not in groups:
                groups[hash_key] = []
            
            groups[hash_key].append({
                **item,
                'hash_key': hash_key,
                'image_host': image_host
            })
        
        return groups
    
    def create_canonical_item(self, group: List[Dict[str, Any]]) -> CanonicalItem:
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
            color_norm=base_item['color'], # TODO: Implement color normalization  
            image_url=base_item['image_url'],
            min_price_cents=min_price_item['price_cents'],
            currency=min_price_item['currency'],
            seller_urls=seller_urls,
            source_ids=source_ids
        )
    
    def save_canonical_items(self, canonical_items: List[CanonicalItem]):
        """Save canonical items and links to database"""
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                # Clear existing canonical data
                cur.execute("TRUNCATE item_canonical, item_links CASCADE")
                
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
                
                conn.commit()
    
    def process_deduplication(self):
        """Main deduplication process"""
        print("Starting deduplication process...")
        
        # Step 1: Get all source items
        print("Fetching source items...")
        source_items = self.get_source_items()
        print(f"Found {len(source_items)} source items")
        
        if not source_items:
            print("No source items found. Make sure eBay fetcher has run.")
            return
        
        # Step 2: Group duplicates
        print("Grouping duplicates...")
        groups = self.group_duplicates(source_items)
        
        # Filter to only groups with duplicates (more than 1 item)
        duplicate_groups = {k: v for k, v in groups.items() if len(v) > 1}
        single_items = {k: v for k, v in groups.items() if len(v) == 1}
        
        print(f"Found {len(duplicate_groups)} groups with duplicates")
        print(f"Found {len(single_items)} unique items")
        
        # Step 3: Create canonical items
        print("Creating canonical items...")
        canonical_items = []
        
        # Process duplicate groups
        for group in duplicate_groups.values():
            canonical_item = self.create_canonical_item(group)
            canonical_items.append(canonical_item)
        
        # Process single items
        for group in single_items.values():
            canonical_item = self.create_canonical_item(group)
            canonical_items.append(canonical_item)
        
        print(f"Created {len(canonical_items)} canonical items")
        
        # Step 4: Save to database
        print("Saving canonical items to database...")
        self.save_canonical_items(canonical_items)
        
        print("Deduplication complete!")
        
        # Print statistics
        original_count = len(source_items)
        final_count = len(canonical_items)
        reduction = original_count - final_count
        reduction_percent = (reduction / original_count) * 100 if original_count > 0 else 0
        
        print(f"\nStatistics:")
        print(f"Original items: {original_count}")
        print(f"Canonical items: {final_count}")
        print(f"Duplicates removed: {reduction} ({reduction_percent:.1f}%)")

def main():
    """Run deduplication process"""
    from dotenv import load_dotenv
    
    # Load environment variables from .env file
    load_dotenv()
    
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("ERROR: DATABASE_URL environment variable not set")
        print("Current working directory:", os.getcwd())
        print("Looking for .env file...")
        return
    
    print(f"Connected to database: {db_url[:50]}...")  # Show first 50 chars
      
    processor = DedupeProcessor(db_url)
    processor.process_deduplication()

if __name__ == "__main__":
    main()
