#!/bin/bash
# Monitoring script for Supabase Australia project
# Runs daily at noon to check health

PROJECT_ROOT="/home/thinkpad/Projects/active/WEBBUILD/supabase_australia"
BACKUP_DIR="${PROJECT_ROOT}/backups"
LOG_FILE="${PROJECT_ROOT}/monitor.log"

# Function to log messages
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "${LOG_FILE}"
}

# Check if database is running
if ! docker exec supabase_postgres pg_isready -U postgres > /dev/null 2>&1; then
    log "ERROR: Database is not accessible"
    exit 1
fi

# Get lead count
COUNT=$(docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads;" -t -A 2>/dev/null | tr -d ' ')
log "Current lead count: ${COUNT}"

# Check ingestion_log for recent runs
RECENT=$(docker exec supabase_postgres psql -U postgres -d postgres -c "
    SELECT COUNT(*) FROM ingestion_log 
    WHERE started_at > NOW() - INTERVAL '24 hours'
    AND status = 'completed';" -t -A 2>/dev/null | tr -d ' ')
log "Completed ingestion runs in last 24h: ${RECENT}"

# Check for errors in ingestion_log
ERRORS=$(docker exec supabase_postgres psql -U postgres -d postgres -c "
    SELECT COUNT(*) FROM ingestion_log 
    WHERE started_at > NOW() - INTERVAL '24 hours'
    AND status != 'completed';" -t -A 2>/dev/null | tr -d ' ')
if [ "${ERRORS}" -gt 0 ]; then
    log "WARNING: Found ${ERRORS} ingestion runs with errors or not completed"
fi

# Check backup directory exists and has recent backups
if [ -d "${BACKUP_DIR}" ]; then
    RECENT_BACKUP=$(find "${BACKUP_DIR}" -name "*.sql.gz" -mtime -1 2>/dev/null | head -1)
    if [ -n "${RECENT_BACKUP}" ]; then
        log "Recent backup: $(basename "${RECENT_BACKUP}")"
    else
        log "WARNING: No backup found from last 24 hours"
    fi
else
    log "WARNING: Backup directory does not exist"
fi

log "Monitoring completed successfully"
