#!/bin/bash

# Enhanced monitoring script for Supabase Australia pipeline
# Runs comprehensive health checks and generates alerts

PROJECT_ROOT="/home/thinkpad/Projects/active/WEBBUILD/supabase_australia"
BACKUP_DIR="${PROJECT_ROOT}/backups"
LOG_FILE="${PROJECT_ROOT}/enhanced_monitor.log"
ALERT_LOG="${PROJECT_ROOT}/alerts.log"
TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")

# Function to log messages
log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

# Function to send alerts
alert() {
    echo "[$TIMESTAMP] ALERT: $1" >> "$ALERT_LOG"
    log "ALERT: $1"
}

# Function to check database
check_database() {
    log "Checking database connectivity..."
    if docker exec supabase_postgres pg_isready -U postgres > /dev/null 2>&1; then
        log "✅ Database is accessible"
        return 0
    else
        log "❌ ERROR: Database is not accessible"
        alert "Database connection failed"
        return 1
    fi
}

# Function to check lead count
check_lead_count() {
    log "Checking lead count..."
    COUNT=$(docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads;" -t -A 2>/dev/null | tr -d ' ')
    if [ -z "$COUNT" ]; then
        log "❌ ERROR: Could not retrieve lead count"
        alert "Failed to retrieve lead count from database"
        return 1
    else
        log "✅ Total leads: $COUNT"
        echo "lead_count: $COUNT" >> "$LOG_FILE"
        return 0
    fi
}

# Function to check pipeline runs
check_pipeline_runs() {
    log "Checking pipeline runs in last 24 hours..."
    RECENT=$(docker exec supabase_postgres psql -U postgres -d postgres -c "
        SELECT COUNT(*) FROM ingestion_log 
        WHERE started_at > NOW() - INTERVAL '24 hours'
        AND status = 'completed';" -t -A 2>/dev/null | tr -d ' ')
    
    if [ -z "$RECENT" ]; then
        log "❌ ERROR: Could not retrieve pipeline run data"
        alert "Failed to retrieve ingestion_log data"
        return 1
    else
        log "✅ Completed ingestion runs in last 24h: $RECENT"
        echo "pipeline_runs_24h: $RECENT" >> "$LOG_FILE"
        
        # Check for errors
        ERRORS=$(docker exec supabase_postgres psql -U postgres -d postgres -c "
            SELECT COUNT(*) FROM ingestion_log 
            WHERE started_at > NOW() - INTERVAL '24 hours'
            AND status != 'completed';" -t -A 2>/dev/null | tr -d ' ')
        
        if [ "$ERRORS" -gt 0 ]; then
            log "⚠️ WARNING: Found $ERRORS ingestion runs with errors or not completed"
            echo "pipeline_errors_24h: $ERRORS" >> "$LOG_FILE"
            alert "Found $ERRORS pipeline runs with errors in last 24 hours"
        fi
        return 0
    fi
}

# Function to check backups
check_backups() {
    log "Checking backups..."
    if [ -d "$BACKUP_DIR" ]; then
        RECENT_BACKUP=$(find "$BACKUP_DIR" -name "supabase_australia_*.sql.gz" -mtime -1 2>/dev/null | head -1)
        if [ -n "$RECENT_BACKUP" ]; then
            BACKUP_NAME=$(basename "$RECENT_BACKUP")
            log "✅ Recent backup: $BACKUP_NAME"
            echo "recent_backup: $BACKUP_NAME" >> "$LOG_FILE"
        else
            log "❌ WARNING: No backup found from last 24 hours"
            echo "recent_backup: NONE" >> "$LOG_FILE"
            alert "No backup found from last 24 hours"
        fi
    else
        log "❌ WARNING: Backup directory does not exist"
        echo "backup_dir_exists: false" >> "$LOG_FILE"
        alert "Backup directory missing"
    fi
}

# Function to check web server
check_web_server() {
    log "Checking web server..."
    if netstat -tlnp 2>/dev/null | grep -q ':8000'; then
        log "✅ Web server is running on port 8000"
        echo "web_server_status: running" >> "$LOG_FILE"
        
        # Check if dashboard is accessible
        if curl -s --connect-timeout 5 http://localhost:8000/03_Scripts_Code/lead_generator.html > /dev/null 2>&1; then
            log "✅ Web dashboard is accessible"
            echo "dashboard_accessible: true" >> "$LOG_FILE"
        else
            log "❌ WARNING: Web dashboard is not accessible"
            echo "dashboard_accessible: false" >> "$LOG_FILE"
            alert "Web dashboard not accessible on port 8000"
        fi
    else
        log "❌ ERROR: Web server is not running on port 8000"
        echo "web_server_status: stopped" >> "$LOG_FILE"
        alert "Web server is not running on port 8000"
    fi
}

# Function to check pipeline script
check_pipeline_script() {
    log "Checking pipeline script..."
    if [ -f "$PROJECT_ROOT/03_Scripts_Code/scripts/pipeline_fixed.py" ]; then
        log "✅ Pipeline script exists"
        echo "pipeline_script_exists: true" >> "$LOG_FILE"
    else
        log "❌ ERROR: Pipeline script not found"
        echo "pipeline_script_exists: false" >> "$LOG_FILE"
        alert "Pipeline script missing: pipeline_fixed.py"
    fi
}

# Function to check data files
check_data_files() {
    log "Checking data files..."
    if [ -f "$PROJECT_ROOT/extracted_leads.csv" ]; then
        FILE_SIZE=$(du -h "$PROJECT_ROOT/extracted_leads.csv" | cut -f1)
        log "✅ Extracted leads CSV exists (size: $FILE_SIZE)"
        echo "extracted_leads_csv: exists" >> "$LOG_FILE"
    else
        log "⚠️ INFO: No extracted leads CSV found"
        echo "extracted_leads_csv: missing" >> "$LOG_FILE"
    fi
}

# Function to check disk space
check_disk_space() {
    log "Checking disk space..."
    DISK_USAGE=$(df -h "$PROJECT_ROOT" | tail -1 | awk '{print $5}' | tr -d '%')
    if [ "$DISK_USAGE" -gt 90 ]; then
        log "⚠️ WARNING: Disk usage is ${DISK_USAGE}% (threshold: 90%)"
        echo "disk_usage_percent: $DISK_USAGE" >> "$LOG_FILE"
        alert "Disk usage above 90%: ${DISK_USAGE}%"
    else
        log "✅ Disk usage: ${DISK_USAGE}%"
        echo "disk_usage_percent: $DISK_USAGE" >> "$LOG_FILE"
    fi
}

# Main execution
log "=== Starting Enhanced Health Monitoring ==="
log "Timestamp: $TIMESTAMP"

check_database
check_lead_count
check_pipeline_runs
check_backups
check_web_server
check_pipeline_script
check_data_files
check_disk_space

log "=== Health Monitoring Complete ==="
log "See $LOG_FILE for full details"

# Send summary if there were issues
if grep -q "ALERT:" "$LOG_FILE"; then
    alert "Health monitoring completed with issues. Check logs for details."
fi