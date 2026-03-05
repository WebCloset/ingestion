# eBay Fetcher Module

This folder contains all eBay-related functionality for the ingestion pipeline.

## Structure

```
ebay/
├── README.md           # This documentation
├── __init__.py         # Python module initialization
├── config.py           # Configuration and constants
├── ebay_auth.py        # eBay authentication utilities
├── fetch_ebay.py       # Main eBay fetcher (Browse API)
├── data/
│   └── sample-items.json  # Sample eBay data for testing
├── scripts/
│   └── import-items.js    # Node.js script to import JSON items to DB
└── tests/
    └── test_ebay.py       # Test suite for eBay functionality
```

## Usage

### Run the eBay Fetcher
```bash
python -m ebay.fetch_ebay
```

### Test the Setup
```bash
python ebay/tests/test_ebay.py
```

### Import Sample Data
```bash
node ebay/scripts/import-items.js ebay/data/sample-items.json
```

## Environment Variables

Required in `.env`:
- `DATABASE_URL` - PostgreSQL connection string
- `EBAY_APP_ID` - eBay application ID (client ID)
- `EBAY_CERT_ID` - eBay certificate ID (client secret)
- `EBAY_ENV` - Environment (PRODUCTION or SANDBOX)
- `EBAY_MARKETPLACE` - Marketplace ID (e.g., EBAY_GB)

## Files Description

- **`fetch_ebay.py`** - Main fetcher using eBay Browse API with OAuth authentication
- **`config.py`** - Search terms and condition mappings
- **`ebay_auth.py`** - Authentication helpers and token management
- **`tests/test_ebay.py`** - Comprehensive test suite for environment, DB, and API
- **`scripts/import-items.js`** - Node.js utility to batch import items with canonical linking
- **`data/sample-items.json`** - Sample fashion items for testing and development