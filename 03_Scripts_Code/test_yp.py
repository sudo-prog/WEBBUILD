#!/usr/bin/env python3
import logging, sys
logging.basicConfig(level=logging.INFO)
sys.path.insert(0, '/home/thinkpad/Projects/supabase_australia')
from ingestion_pipeline import CityFetcher
fetcher = CityFetcher('sydney', logging.getLogger('test'))
# Test single category to speed up
leads = fetcher._fetch_yellow_pages()
print(f"\nYP leads count: {len(leads)}")
for l in leads[:3]:
    print(l)

