# Australian Leads Ingestion Pipeline

End-to-end system for ingesting lead data into Supabase across all 8 Australian capital cities.

## Quick Start

### 1. Prerequisites
```bash
# Python 3.11+
pip install supabase requests

# Supabase CLI (optional, for local dev)
npm i -g supabase
```

### 2. Configure Connection

Edit `config/settings.json` with your Supabase credentials:
```json
{
  "supabase": {
    "url": "https://<your-project>.supabase.co",
    "anon_key": "eyJhbGciOiJIUzI1NiIs...",
    "service_role_key": "eyJhbGciOiJIUzI1NiIs..."
  }
}
```

Or set environment variables:
```bash
export SUPABASE_URL="https://xxx.supabase.co"
export SUPABASE_ANON_KEY="eyJ..."
export SUPABASE_SERVICE_ROLE_KEY="eyJ..."
```

### 3. Deploy Schema

Using Supabase CLI:
```bash
cd supabase_australia
supabase db push schema/001_initial_schema.sql
```

Or apply via SQL in Supabase Studio SQL Editor.

### 4. Run Ingestion

Single city:
```bash
python ingestion_pipeline.py --city sydney
```

All cities:
```bash
python ingestion_pipeline.py --all
```

With specific source:
```bash
python ingestion_pipeline.py --city melbourne --source yellow_pages
```

Dry-run validation (no DB writes):
```bash
python ingestion_pipeline.py --city brisbane --dry-run
```

### 5. Verify

Check ingestion log:
```sql
SELECT * FROM ingestion_log ORDER BY started_at DESC LIMIT 10;
```

Lead summary by city:
```sql
SELECT * FROM v_leads_summary ORDER BY total_leads DESC;
```

## Architecture

### Project Layout
```
supabase_australia/
├── schema/
│   └── 001_initial_schema.sql   # Database schema (leads + ingestion_log)
├── ingestion_pipeline.py        # Main orchestrator
├── config/
│   ├── settings.json            # Connection + city configs
│   └── sources/                 # Source-specific adapters (future)
├── data/
│   ├── inputs/                  # CSV uploads per city (manual source)
│   │   ├── sydney_leads.csv
│   │   ├── melbourne_leads.csv
│   │   └── ...
│   └── outputs/                # Ingestion logs/results (future)
└── README.md
```

### Tables

**leads** — Core lead records
- `id` (uuid PK), `lead_id` (unique external identifier)
- Business: name, abn, category, services
- Location: country, state, city, suburb, postcode, geo coordinates
- Contact: phone, mobile, email, website
- Metrics: rating, review_count, lead_score (0–100), tier
- Timestamps: `first_seen_at`, `last_verified_at`, `created_at`, `updated_at`

**ingestion_log** — Batch audit trail
- `id` (uuid PK), `batch_id` (unique), source/city/state
- Record counts: inserted, updated, skipped, failed
- Error capture: `error_summary`, `error_details` (jsonb)
- Duration tracking, status tracking

### Lead Validation Rules

Applied automatically before insert:
- Business name + category required
- State must be one of 8 Australian states/territories
- Email format validated
- Phone normalized (strip spaces)
- `lead_score` auto-computed if missing
- Duplicate `lead_id` → upsert (update existing)

### 8 Capital Cities

| Key | City | State | Notes |
|-----|------|-------|-------|
| `sydney` | Sydney | NSW | Primary market, 2xxx postcodes |
| `melbourne` | Melbourne | VIC | 3xxx postcodes |
| `brisbane` | Brisbane | QLD | 4xxx postcodes |
| `perth` | Perth | WA | 6xxx postcodes |
| `adelaide` | Adelaide | SA | 5xxx postcodes |
| `hobart` | Hobart | TAS | 7xxx postcodes |
| `darwin` | Darwin | NT | 08xx prefix |
| `canberra` | Canberra | ACT | 26xx/29xx postcodes |

## Multi-Source Strategy

Each city pulls from 3–4 sources per run:

| Source | Type | Fields | Rate Limit |
|--------|------|--------|------------|
| `google_business` | Google Places API (future) | name, category, phone, website, geo, rating | $200 free/month |
| `yellow_pages` | Yellow Pages API/scrape (future) | name, category, phone, address | — |
| `tradie_portal` | hipages / Oneflare (future) | name, mobile, category, services | — |
| `manual` | CSV upload (ready now) | any | unlimited |

To add custom sources, edit `ingestion_pipeline.py` → `CityFetcher._fetch_<source>()`.

## Monitoring & Debugging

### Check recent ingestion log
```sql
SELECT batch_id, source_name, city_target, status, records_inserted, records_failed, started_at
FROM ingestion_log
ORDER BY started_at DESC
LIMIT 20;
```

### Identify bad leads
```sql
SELECT business_name, email, phone, lead_score
FROM leads
WHERE lead_score < 50
ORDER BY created_at DESC
LIMIT 50;
```

### Performance: add per-source stats
```sql
SELECT source_name, count(*) as total, avg(lead_score) as avg_score
FROM leads
GROUP BY source_name;
```

### Raw ingestion errors
```sql
SELECT error_summary, error_details, completed_at
FROM ingestion_log
WHERE status = 'failed';
```

## Troubleshooting

**Supabase connection refused**
- Verify `SUPABASE_URL` is correct
- Check if local dev instance is running: `supabase start`
- For cloud: check IP allow-list includes your IP

**Insert failures (403)**
- Use `SERVICE_ROLE_KEY` not `anon_key` for server-side ingestion
- anon key has RLS restrictions; service key bypasses RLS

**Duplicate key error**
- Fetcher generates deterministic `lead_id` from name+city. If business exists in multiple sources, same ID → upsert occurs.
- To force new entry: provide explicit `lead_id` field in CSV.

**No data in views**
- Views depend on base tables having data. Run at least one successful ingestion first.
- Refresh materialized views if used: `REFRESH MATERIALIZED VIEW v_leads_summary;`

**Port 6543 already in use**
- Another PostgreSQL instance running. Kill it or change `postgres.port` in `settings.json`.
- Docker alternative: `docker run -p 6543:5432 -e POSTGRES_PASSWORD=... postgres`

## Next Steps

1. Integrate real Google Places API — see `ingestion_pipeline.py::_fetch_google_business`
2. Add Yellow Pages scraping or API integration
3. Implement worker pool for parallel city ingestion (`--workers 4`)
4. Webhook notifications on batch completion
5. Scheduler: cron job or Airflow DAG for daily refreshes

---

**Status:** Schema defined ✓ | Ingestion framework ready ✓ | Sample data created ✓ | **Ready to connect and run**
