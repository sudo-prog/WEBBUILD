# Supabase Australia Pipeline - Handover Document

## Project Overview
**Project:** Supabase Australia - Lead generation pipeline for Australian trades businesses
**Goal:** Extract, validate, enrich, and store Australian business leads from ABN data
**Tech Stack:** Python, PostgreSQL, Docker, Playwright, Supabase

## Architecture

### Components
1.  **Data Source:** `raw_leads/` directory (297,738 pre-extracted ABN records)
2.  **Pipeline Scripts:** 
    -   `pipeline_fixed.py` (main pipeline)
    -   `abn_validator.py` (ABN validation)
    -   `abn_enrichment.py` (data enrichment)
3.  **Database:** PostgreSQL 16.2-alpine in Docker container (`supabase_postgres`)
4.  **Web Dashboard:** `lead_generator.html` (served via local web server)

### Data Flow
`raw_leads` → `pipeline` → `PostgreSQL` → `Web Dashboard` → `Extracted CSV`

## Database Configuration

### Connection Details
-   **Container:** `supabase_postgres` running on port 6543
-   **Service Role User:** `supabase_service` / `YOUR_PASSWORD_HERE`
-   **Password:** `YOUR_PASSWORD_HERE` (or from env var `PG_PASSWORD`)

### Tables
-   **leads:** 20,703 records with complete business information
-   **ingestion_log:** Tracks pipeline runs with `needs_review` column

### Connection Commands
```bash
# Connect to database
docker exec -it supabase_postgres psql -U postgres -d postgres

# Check lead count
docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads;"

# Check leads with ABN
docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads WHERE abn IS NOT NULL;"
```

## Pipeline Scripts

### Main Pipeline: `pipeline_fixed.py`
```bash
# Dry run for Sydney
python 03_Scripts_Code/scripts/pipeline_fixed.py --city sydney --dry-run

# Full run with ABN bulk data
python 03_Scripts_Code/scripts/pipeline_fixed.py --city sydney --source abn_bulk --limit 1000

# Run for all cities
python 03_Scripts_Code/scripts/pipeline_fixed.py --all --source abn_bulk
```

### Key Features
-   ✅ ABN validation with error handling
-   ✅ Website enrichment (all leads have generated domains)
-   ✅ Phone number normalization
-   ✅ Category detection
-   ✅ Duplicate prevention (business_name, city)
-   ✅ Automated cron jobs for daily runs



## Supabase Connection Details (Updated)

### PostgreSQL Connection String
**Format:** `postgresql://user:password@host:port/database`

**Specifics:**
- **Host:** db.psnosfonkujbcxdcrnpu.supabase.co
- **Port:** 5432
- **Database:** postgres
- **User:** postgres
- **Password:** `zcM1Z4cDHyUhZg13` (sensitive - store securely)

**Full Connection String:**
```
postgresql://postgres:zcM1Z4cDHyUhZg13@db.psnosfonkujbcxdcrnpu.supabase.co:5432/postgres
```

### Important Security Notes
- 🔐 **Never commit passwords to version control** - Store in environment variables or secure vaults
- 🔐 **Use .env files** for local development (add to .gitignore)
- 🔐 **Rotate credentials** periodically for security
- 🔐 **Use different credentials** for different environments (dev/staging/prod)

#
## Supabase CLI Setup

### Initialize Project
```bash
supabase login
supabase init
supabase link --project-ref psnosfonkujbcxdcrnpu
```

### Connection Details
- **Project URL:** https://psnosfonkujbcxdcrnpu.supabase.co
- **Publishable Key:** sb_publishable_bEFVHakrs1unioHIsB6m8Q_rQmjag0Q
- **Connection String:** postgresql://postgres:[YOUR-PASSWORD]@db.psnosfonkujbcxdcrnpu.supabase.co:5432/postgres
- **Project Ref:** psnosfonkujbcxdcrnpu

### Important Security Notes
- 🔐 Store passwords in environment variables or a password manager
- 🔐 Never commit credentials to version control
- 🔐 Use different keys for different environments

## Environment Variables
```bash
export PG_HOST="db.psnosfonkujbcxdcrnpu.supabase.co"
export PG_PORT="5432"
export PG_DATABASE="postgres"
export PG_USER="postgres"
export PG_PASSWORD="zcM1Z4cDHyUhZg13"  # Or use a secrets manager
```

### MCP Configuration (Already Set Up)
- **Server:** supabase
- **Type:** HTTP
- **URL:** https://mcp.supabase.com/mcp?project_ref=psnosfonkujbcxdcrnpu


## Supabase CLI Setup

### Initialize Project
```bash
supabase login
supabase init
supabase link --project-ref psnosfonkujbcxdcrnpu
```

### Connection Details
- **Project URL:** https://psnosfonkujbcxdcrnpu.supabase.co
- **Publishable Key:** sb_publishable_bEFVHakrs1unioHIsB6m8Q_rQmjag0Q
- **Connection String:** postgresql://postgres:[YOUR-PASSWORD]@db.psnosfonkujbcxdcrnpu.supabase.co:5432/postgres
- **Project Ref:** psnosfonkujbcxdcrnpu

### Important Security Notes
- 🔐 Store passwords in environment variables or a password manager
- 🔐 Never commit credentials to version control
- 🔐 Use different keys for different environments

## Environment Variables

```bash
export PG_PASSWORD="YOUR_PASSWORD_HERE"
export SUPABASE_URL="your-supabase-url"
export SUPABASE_ANON_KEY="your-anon-key"
export SUPABASE_SERVICE_ROLE_KEY="your-service-role-key"
```

## Web Dashboard

### Access
-   **Location:** `03_Scripts_Code/lead_generator.html`
-   **Local Server:** `http://localhost:8000`
-   **Start Server:** `python3 -m http.server 8000` (in project root)

### Features
-   Filter leads by category, state, tier
-   View lead details
-   Export filtered leads to CSV
-   Visual quality scoring (L1-L5)

## Monitoring Setup

### Cron Jobs (Already Configured)
```bash
# Pipeline run - daily at 2:00 AM
0 2 * * * cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia && python3 03_Scripts_Code/scripts/pipeline_fixed.py --all --source abn_bulk --limit 1000

# Database backup - daily at 3:00 AM
0 3 * * * /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/backup_database.sh

# Health monitoring - daily at 12:00 PM
0 12 * * * /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/monitor_health.sh
```

### Health Check Script
The `monitor_health.sh` script checks:
- Database connectivity
- Lead count
- Ingestion log status
- Backup existence

### Backup Script
- Creates compressed PostgreSQL dumps
- Retains 7 days of backups
- Stores in `backups/` directory

## Quality Assurance

### Data Quality Metrics
-   **Total leads:** 20,703
-   **With ABN:** 20,703 (100%)
-   **With website:** 20,703 (100%) - all enriched
-   **Lead scoring:** L1-L5 tiers based on completeness

### Verification
-   ✅ ABN validation against Australian Business Register
-   ✅ Duplicate prevention (business_name, city)
-   ✅ Data enrichment completeness
-   ✅ Automated quality scoring

## Troubleshooting

### Common Issues & Fixes

#### Database Connection Issues
```bash
# Check if container is running
docker ps | grep supabase_postgres

# Check logs
docker logs supabase_postgres

# Test connection
docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT 1;"
```

#### Pipeline Failures
```bash
# Check ingestion_log for errors
docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT * FROM ingestion_log ORDER by completed_at DESC LIMIT 5;"

# Run with verbose logging
export LOG_LEVEL=DEBUG && python pipeline_fixed.py --city sydney --dry-run
```

#### Web Dashboard Not Loading
```bash
# Check if server is running
ps aux | grep "python3 -m http.server"

# Restart server
cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia && python3 -m http.server 8000 &
```

## Automation & Integration

### MCP Configuration
-   **Server:** supabase
-   **Type:** HTTP
-   **URL:** https://mcp.supabase.com/mcp?project_ref=psnosfonkujbcxdcrnpu
-   **Location:** `~/.vscode/mcp.json`

### Agent Skills
-   **Package:** `supabase/agent-skills`
-   **Version:** 1.5.3
-   **Skills:** Postgres Best Practices, Supabase

## Security Notes

### Credentials
-   **Database password:** `YOUR_PASSWORD_HERE`
-   **Supabase API keys:** Stored in `~/.hermes/secrets/api_keys.env`
-   **Never commit real data** - `raw_leads/` directory contains real business information

### Best Practices
-   Use environment variables for credentials
-   Regular backups (automated)
-   Monitor ingestion logs for anomalies
-   Keep `AGENTS_NOTES.md` updated

## Next Steps for Kilo Code

1.  **Familiarize** with the pipeline scripts and database structure
2.  **Test** the pipeline with a dry-run: `python pipeline_fixed.py --city sydney --dry-run`
3.  **Monitor** the daily cron jobs and check logs regularly
4.  **Update** `AGENTS_NOTES.md` with any changes or improvements
5.  **Use** the web dashboard for lead visualization and export

## Support Contacts

-   **Primary:** Hermes Agent (via MCP or direct tasks)
-   **Secondary:** Local development environment documentation
-   **Tertiary:** `AGENTS_NOTES.md` for project history

---

**Document Version:** 2.0  
**Last Updated:** 2026-05-05  
**Project:** WEBBUILD Supabase Australia