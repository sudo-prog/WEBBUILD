# Supabase Australia — Lead Generation Pipeline Documentation

## 📋 Project Overview
This is a production-grade lead generation pipeline for Australian trades businesses. The system extracts real ABN-registered businesses, enriches them with contact data, scores quality, and stores them in PostgreSQL for Supabase consumption.

## 🗄️ Database Configuration

### Connection Details
- **Host**: localhost
- **Port**: 6543
- **Database**: postgres
- **User**: supabase_service
- **Password**: YOUR_PASSWORD_HERE

### Docker Container
```bash
# Start container
docker start supabase_postgres

# Access database
docker exec -it supabase_postgres psql -U postgres -d postgres
```

### Lead Count
```bash
docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads;"
```

## 🚀 Pipeline Scripts

### Main Pipeline
**Location**: `03_Scripts_Code/scripts/pipeline_fixed.py`

**Usage**:
```bash
# Dry run for Sydney
python3 03_Scripts_Code/scripts/pipeline_fixed.py --city sydney --dry-run

# Full run with ABN bulk data
python3 03_Scripts_Code/scripts/pipeline_fixed.py --city sydney --source abn_bulk --limit 1000

# Run for all cities
python3 03_Scripts_Code/scripts/pipeline_fixed.py --all --source abn_bulk --limit 1000
```

**Configuration**: `03_Scripts_Code/config/settings.json`

### Important Scripts
- **backfill_lead_ids.py**: Fix inconsistent lead IDs in existing data
- **lead_id_utils.py**: Canonical lead ID generation
- **pipeline_fixed.py**: Main pipeline with quality scoring

## ⏰ Cron Jobs

### System Crontab (`crontab -l`)
```
0 2  * * *   cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia && python3 03_Scripts_Code/scripts/pipeline_fixed.py --all --source abn_bulk --limit 1000 >> /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/pipeline.log 2>&1
0 3  * * *   /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/backup_database.sh >> /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/backup.log 2>&1
0 12 * * *   /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/monitor_health.sh >> /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/monitor.log 2>&1
```

### Script Locations
- **Pipeline**: Runs daily at 2 AM, inserts 1,000 leads per city
- **Backup**: Runs daily at 3 AM, creates compressed SQL backups in `backups/` directory
- **Monitor**: Runs daily at 12 PM, checks database connectivity, lead count, ingestion runs, and backups

## 🌐 Web Dashboard

**Location**: `03_Scripts_Code/lead_generator.html`

**Access**: http://127.0.0.1:8000/03_Scripts_Code/lead_generator.html

**Features**:
- Generate leads by city, trade category, and filters
- View lead quality scores and tiers
- Upload leads to Supabase
- Export to CSV
- Real-time statistics

**Start Web Server**:
```bash
cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia
python3 -m http.server 8000
```

## 🔧 Troubleshooting

### Database Connection Issues
1. Ensure Docker container is running:
   ```bash
   docker ps | grep supabase_postgres
   ```
2. Check credentials in `config/settings.json` and `03_Scripts_Code/config/settings.json`
3. Verify port 6543 is accessible

### Pipeline Failures
1. Check `pipeline.log` for errors
2. Common issues:
   - Authentication failures: Update credentials in config files
   - Duplicate data: Run `python3 03_Scripts_Code/scripts/backfill_lead_ids.py --commit`
   - Schema issues: Run `schema/001_initial_schema.sql`

### Web Dashboard Not Loading
1. Ensure web server is running
2. Check browser console for JavaScript errors
3. Verify network connectivity to localhost:8000

## 📊 Quality Scoring System

Leads are scored across 5 layers:
- **L1**: ABN Verification (max 20 points)
- **L2**: Activity & Signals (max 20 points)
- **L3**: Revenue Proxy (max 27 points)
- **L4**: Source Count (max 6 points)
- **L5**: Website Presence (max 10 points)

**Priority Tiers**:
- **HIGH**: ≥ 80 points
- **MEDIUM**: ≥ 50 points
- **LOW**: ≥ 0 points
- **DISCARD**: < 50 points

## 🔄 Maintenance Procedures

### Daily Checks
1. Verify cron jobs ran successfully
2. Check lead count in database
3. Review backup logs
4. Monitor system resources

### Weekly Tasks
1. Review ingestion logs for errors
2. Check disk usage
3. Test pipeline with dry-run

### Monthly Tasks
1. Rotate credentials if necessary
2. Verify backup integrity
3. Update dependencies

## 🐛 Common Issues & Fixes

### Authentication Failed for User "postgres"
**Cause**: Config file uses wrong user/password.
**Fix**: Update `config/settings.json` and `03_Scripts_Code/config/settings.json` with:
```json
{
  "postgres": {
    "user": "supabase_service",
    "password": "YOUR_PASSWORD_HERE"
  }
}
```

### Pipeline Fails with Duplicate Key Violation
**Cause**: Inconsistent lead_id values across scripts.
**Fix**: Run lead_id backfill:
```bash
cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia
export PG_HOST="localhost" && export PG_PORT="6543" && export PG_DATABASE="postgres" && export PG_USER="supabase_service" && export PG_PASSWORD="YOUR_PASSWORD_HERE" && python3 03_Scripts_Code/scripts/backfill_lead_ids.py --commit
```

### Web Server Won't Start
**Cause**: Port already in use or permission issues.
**Fix**: Check if another instance is running and kill it, then restart:
```bash
fuser -k 8000/tcp
python3 -m http.server 8000
```

## 📁 Project Structure
```
supabase_australia/
├── config/                 # Database configuration
├── 03_Scripts_Code/        # Pipeline and scripts
│   ├── scripts/            # Individual pipeline components
│   ├── lead_generator.html # Web dashboard
│   └── config/             # Script-specific configuration
├── data/                   # Raw and processed data
├── raw_leads/              # Raw lead files
├── schema/                 # Database schema SQL
└── backups/                # Automated database backups
```

## 🔗 External Dependencies
- **Docker**: PostgreSQL 16.2-alpine container
- **Supabase**: Remote Supabase instance for production (optional)
- **Playwright**: For web scraping tasks
- **psycopg2**: PostgreSQL database adapter

## 📝 Notes
- All pipeline scripts use deterministic lead_id generation via `lead_id_utils.py`
- The system is designed for zero manual intervention once configured
- Monitor logs daily for any errors or warnings
- Keep credentials secure and never commit them to version control

---
*Last updated: May 10, 2026*