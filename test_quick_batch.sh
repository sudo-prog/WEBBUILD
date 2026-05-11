#!/bin/bash
set -e

echo "Starting Quick Test Batch - $(date)"

# Step 1: Scrape Yellow Pages for a single city with 1 page
CITY="Sydney"
STATE="NSW"
PAGES=1

echo "Step 1: Scraping Yellow Pages for $CITY ($STATE) with $PAGES page(s)..."
python 03_Scripts_Code/scripts/scrape_yp_playwright.py $CITY $STATE --pages $PAGES

# Find the output file
OUTPUT_DIR="raw_leads"
OUTPUT_FILE=$(ls -t $OUTPUT_DIR/yp_$CITY\_*.json | head -1)
echo "Scraped leads saved to: $OUTPUT_FILE"

# Temporarily move other YP files to a backup directory so merge only processes our test file
BACKUP_DIR="raw_leads_test_backup"
mkdir -p "$BACKUP_DIR"
mv "$OUTPUT_DIR"/yp_*.json "$BACKUP_DIR/" 2>/dev/null || true
# Restore our test file
mv "$BACKUP_DIR/yp_${CITY}_*.json" "$OUTPUT_DIR/" 2>/dev/null || true

# Step 2: Merge YP data with ABN leads
echo "Step 2: Merging YP data with ABN leads..."
# Create a small ABN sample for testing
cat > /tmp/test_abn_leads.jsonl << 'JSONL'
{"abn": "12345678890", "legal_name": "Test Plumbing Pty Ltd", "trading_name": "Test Plumbing", "address_state": "NSW", "address_postcode": "2000", "category": "plumber"}
{"abn": "98765432100", "legal_name": "Electricians R Us", "trading_name": "Electricians R Us", "address_state": "NSW", "address_postcode": "2000", "category": "electrician"}
JSONL

# Copy test ABN leads to the expected directory
mkdir -p data/abn/leads
cp /tmp/test_abn_leads.jsonl data/abn/leads/test_batch.jsonl

# Run merge with a limit
python 03_Scripts_Code/scripts/merge_yp_abn.py --limit 5

# Step 3: Import enriched leads into Supabase
echo "Step 3: Importing enriched leads into Supabase..."
python 03_Scripts_Code/scripts/import_leads.py --file data/enriched_leads_test.json

# Restore YP files
mv "$BACKUP_DIR"/* "$OUTPUT_DIR/" 2>/dev/null || true
rmdir "$BACKUP_DIR" 2>/dev/null || true

echo "Test batch completed - $(date)"
echo "Check contact_enrichment.log for details."
