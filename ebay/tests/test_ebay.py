# test_ebay.py
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def test_environment():
    """Test environment setup."""
    print("🧪 Testing environment setup...")
    
    required_vars = ["DATABASE_URL", "EBAY_APP_ID", "EBAY_USER_TOKEN"]
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            print(f"✅ {var} found")
    
    if missing_vars:
        print(f"❌ Missing variables: {', '.join(missing_vars)}")
        return False
    
    return True

def test_database_connection():
    """Test database connection."""
    print("\n🗄️ Testing database connection...")
    
    try:
        import psycopg2
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM item_source;")
        count = cursor.fetchone()[0]
        
        print(f"✅ Database connected. Current item_source rows: {count}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_ebay_api():
    """Test eBay API connection."""
    print("\n🔍 Testing eBay API connection...")
    
    try:
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from ebay.fetch_ebay import EbayFetcher
        fetcher = EbayFetcher()
        
        items = fetcher.search_items("Nike", limit=3)
        
        if items:
            print(f"✅ eBay API working. Found {len(items)} test items")
            return True
        else:
            print("❌ eBay API returned no results")
            return False
            
    except Exception as e:
        print(f"❌ eBay API test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 WebCloset eBay Fetcher - Setup Test\n")
    
    success = True
    
    if not test_environment():
        success = False
        
    if success and not test_database_connection():
        success = False
        
    if success and not test_ebay_api():
        success = False
    
    print("\n" + "="*50)
    if success:
        print("🎉 All tests passed! Ready to run the fetcher.")
        print("\nNext step: python -m ebay.fetch_ebay")
    else:
        print("❌ Fix the issues above first.")
    print("="*50)

if __name__ == "__main__":
    main()
