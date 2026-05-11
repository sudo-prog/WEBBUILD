#!/bin/bash
set -e

echo "Starting Test Contact Enrichment Pipeline - $(date)"

# Step 1: Scrape Yellow Pages for a single city with 1 page to get a small batch
CITY="Sydney"
STATE="NSW"
PAGES=1

echo "Step 1: Scraping Yellow Pages for $CITY ($STATE) with $PAGES page(s)..."
python 03_Scripts_Code/scripts/scrape_yp_playwright.py $CITY $STATE --pages $PAGES

# Find the output file
OUTPUT_DIR="raw_leads"
OUTPUT_FILE=$(ls -t $OUTPUT_DIR/yp_$CITY\_*.json | head -1)
echo "Scraped leads saved to: $OUTPUT_FILE"

# Step 2: Merge YP data with ABN leads
echo "Step 2: Merging YP data with ABN leads..."
# Create a small ABN sample for testing
mkdir -p data/abn/leads
cat > data/abn/leads/test_abn.jsonl << 'JSONL'
{"abn": "12345678890", "legal_name": "Test Plumbing Pty Ltd", "trading_name": "Test Plumbing", "address_state": "NSW", "address_postcode": "2000", "category": "plumber"}
{"abn": "98765432101", "legal_name": "Sydney Electric Pty Ltd", "trading_name": "Sydney Electricians", "address_state": "NSW", "address_postcode": "2000", "category": "electrician"}
JSONL

# Run merge_yp_abn.py - it will use the test ABN file and the YP file we just created
python 03_Scripts_Code/scripts/merge_yp_abn.py

# Step 3: Import enriched leads into Supabase
echo "Step 3: Importing enriched leads into Supabase..."
# The merge script outputs to data/enriched_leads.json
python 03_Scripts_Code/scripts/import_leads.py data/enriched_leads.json

echo "Test Contact Enrichment Pipeline completed - $(date)"
