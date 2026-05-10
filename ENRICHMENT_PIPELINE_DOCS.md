# Contact Enrichment Pipeline Documentation

## Overview
This pipeline enriches ABN trade leads with contact information (phone/email) using free, targeted lookups per business — avoiding bulk scraping blocks.

## Scripts
- **enrich_contacts_free.py** - Core enrichment logic using free sources (ABN Lookup, DuckDuckGo, White Pages, True Local)
- **run_enrichment_batch.py** - Orchestrates enrichment by city and upserts results to Supabase
- **test_enrichment.py** - Verifies all sources are reachable before running batches

## Key Features
- **Per-business lookups**: Uses known business names/ABNs, not bulk category scraping
- **Free sources only**: No paid APIs required
- **Polite delays**: 2.5 seconds between requests
- **Resume-safe**: Writes each lead immediately after lookup; can be stopped and restarted
- **Website detection**: Added `has_website` flag to indicate if a business website was found
- **Website email extraction**: If no email found but website exists, attempts extraction from homepage

## Usage

### Test sources are reachable
```bash
python scripts/test_enrichment.py
```

### Dry run (preview queries)
```bash
python scripts/run_enrichment_batch.py --city sydney --limit 10 --dry-run
```

### Full enrichment run
```bash
python scripts/run_enrichment_batch.py --city sydney --limit 200
```

### All cities (resumable)
```bash
python scripts/run_enrichment_batch.py --all --limit 200 --resume
```

## Configuration

### Input
- ABN leads in JSONL format (from bulk extract)
- Default location: `~/data/abn/leads/`

### Output
- Enriched leads: `~/data/abn/enriched/`
- Each run creates a new JSONL file with timestamp

### Supabase Connection
Uses environment variables or config/settings.json:
- `PG_HOST` (default: 127.0.0.1)
- `PG_PORT` (default: 6543)
- `PG_DATABASE` (default: postgres)
- `PG_USER` (default: supabase_service)
- `PG_PASSWORD` (required)

## Important Notes

- **Website correlation**: Businesses with websites are more likely to have email addresses
- **Email capture rate**: Limited by free sources; many businesses only provide phone
- **Error handling**: Script logs errors and continues; interruptions can be resumed
- **Rate limiting**: Built-in delays prevent blocks; do not remove

## Troubleshooting

1. **Sources unreachable**: Run `test_enrichment.py` to diagnose
2. **Blocked requests**: Increase delay or check user-agent rotation
3. **Database connection errors**: Verify Supabase credentials and network
4. **No emails found**: Many small businesses don't publish emails publicly

## File Locations
- Scripts: `~/scripts/`
- Requirements: `pip install requests psycopg2-binary beautifulsoup4`
- Enriched output: `~/data/abn/enriched/`

## Monitoring
Check logs in `~/data/abn/enriched/run_log_<timestamp>.json` after each run.

## Performance
- ~2.5 seconds per lead (due to polite delays)
- ~20 leads per minute
- 200 leads ≈ 10-15 minutes

## Safety
- `--resume` flag ensures no work is lost on interruption
- Each lead is written immediately after processing
- Database upserts only occur for leads with new contact data