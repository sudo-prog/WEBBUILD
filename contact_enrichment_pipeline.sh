#!/bin/bash
set -e

echo "Starting Contact Enrichment Pipeline - $(date)"

# Step 1: Scrape Yellow Pages for all cities
echo "Step 1: Scraping Yellow Pages for all cities..."
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Sydney NSW
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Melbourne VIC
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Brisbane QLD
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Perth WA
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Adelaide SA
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Hobart TAS
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Darwin NT
python 03_Scripts_Code/scripts/scrape_yp_playwright.py Canberra ACT

# Step 2: Merge YP data with ABN leads
echo "Step 2: Merging YP data with ABN leads..."
python 03_Scripts_Code/scripts/merge_yp_abn.py

# Step 3: Import enriched leads into Supabase
echo "Step 3: Importing enriched leads into Supabase..."
python 03_Scripts_Code/scripts/import_leads.py

echo "Contact Enrichment Pipeline completed - $(date)"
