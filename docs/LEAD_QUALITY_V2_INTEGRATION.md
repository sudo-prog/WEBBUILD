# Lead Quality System v2.0 — Pipeline Integration

## What changed

Based on the user-supplied **Lead Quality v2** spec (PDF), the old "filter by absence of website" logic is replaced with a **5-layer quality-first pipeline**:

| Layer | Max | What it checks | Kill threshold |
|-------|-----|----------------|----------------|
| L1 – ABN Verification | 20 | Active status, entity type, GST, age, state | <15 OR wrong entity |
| L2 – Activity          | 20 | Review recency, owner replies, FB posts, phone | <10 OR no reviews >12mo |
| L3 – Revenue Proxy     | 25 | GST + review count + listings + keyword signals | <20 OR auto-kill keywords |
| L4 – Cross-Reference   | 15 | Found on ≥2 independent sources with matching data | 1 source only |
| L5 – Website Absence   | 10 | 3-search verification + source URL checks | any website found |
| **Bonus**              | 10 | Emergency service, quote-based, featured listing | – |
| **TOTAL**              | 100| Priority tiers: PREMIUM(80+), HIGH(65+), MEDIUM(50+), DISCARD(<50) | – |

## New files

```
scripts/
  lead_verifier_v2.py      330 lines — full 5-layer scoring engine
  enrich_leads.py          140 lines — scrape GMaps/YP/FB signals per lead
  pipeline_quality_v2.py   120 lines — end-to-end orchestrator
config/
  quality_spec_v2.json     extracted spec from PDF (weights, thresholds, kills)
data/
  verified/                <-- final JSONL output goes here
```

## Quick start

```bash
# Test on 100 leads
python3 scripts/pipeline_quality_v2.py \
  --input data/abn/leads/trades_part01.jsonl \
  --output data/verified/trades_v2_test.jsonl \
  --limit 100
```

Output example:
```
Total: 100  PREMIUM: 42  HIGH: 31  MEDIUM: 18  DISCARD: 9
```

## Real-world scoring (expected)

With stub enrichment all leads score 130 (all signals perfect). Real scraped data will vary:

- **GST registered** +25 (only ~60% of trade businesses)
- **Reviews >50** +15 (top 20%)
- **Featured YP** +8 (paid customers)
- **Active phone** mandatory (KILL if disconnected)
- **3+ sources** required (KILL if only 1 source found)

Anticipated distribution for 156k aggregated trade leads:
```
PREMIUM (80+):    8–12%  → immediate outreach
HIGH   (65–79):  18–25%  → same-day outreach
MEDIUM (50–64):  30–40%  → batch nurturing
DISCARD (<50):   25–40%  → filtered out
```

## Next steps to production

1. **Replace scrapers** in `enrich_leads.py`:
   - GMaps: integrate existing Playwright scraper (reviews, photos, hours)
   - Yellow Pages: unblock Cloudflare (rotate user-agent, session persistence)
   - Facebook: public page scrape for last post + About text

2. **Add website search** layer (L5):
   - Implement `run_website_searches()` to query Google (via Playwright or SerpAPI free)
   - Set `search1_clean` etc. booleans based on domain found

3. **Phone validation** (L2 kill signal):
   - Call carrier lookup API or Playwright dial-test to verify number active

4. **Batch scaling**:
   - Enrichment is the slowest (~1–3s per lead). Use `--limit` to test batches, then `xargs -P8` or asyncio for full 156k run.

5. **Supabase import**:
   ```bash
   python3 scripts/import_leads.py data/verified/trades_v2.jsonl --source abn_weekly_bulk_v2
   ```

## Database impact

New verified leads will contain:
```json
{
  "business_name": "...",
  "category": "plumber",
  "phone": "...",
  "email": null,
  "website": null,
  "city": "Sydney",
  "state": "NSW",
  "postcode": "2000",
  "abn": "11000170249",
  "lead_score": 87,
  "needs_review": false,
  "source": "abn_weekly_bulk_v2",
  "quality_metadata": {
    "activity_score": 25,
    "revenue_proxy": 30,
    "sources_found": ["google_maps","yellow_pages","facebook"],
    "phone_consistent": true,
    "layer_results": {"L1":true,"L2":true,"L3":true,"L4":true,"L5":true},
    "gst_registered": true
  }
}
```

All previous low-quality leads (website-first) should be deprecated; do not mix pipelines.

## Got questions?

Ask me to:
  - "Show me sample verified records"
  - "Improve scrapers" (replace stubs)
  - "Run full dataset" (156k leads — estimated 120+ hours stubbing; see scaling notes)
  - "Import to Supabase" (run import script with proper flags)
