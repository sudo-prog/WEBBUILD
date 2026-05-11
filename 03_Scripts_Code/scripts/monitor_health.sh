#!/bin/bash
# Monitor Health Script for Supabase Australia Pipeline
# Runs daily to check system health and integrity

PROJECT_DIR="/home/thinkpad/Projects/active/WEBBUILD/supabase_australia"
LOG_FILE="$PROJECT_DIR/monitor_health.log"
BACKUP_DIR="$PROJECT_DIR/backups"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

# Function to log messages
log_message() {
    echo "[$DATE] $1" | tee -a "$LOG_FILE"
}

# Function to check database connectivity
check_database() {
    log_message "Checking database connectivity..."
    if docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT 1;" > /dev/null 2>&1; then
        log_message "✅ Database is accessible"
        return 0
    else
        log_message "❌ Database connection failed"
        return 1
    fi
}

# Function to check lead count
check_lead_count() {
    log_message "Checking lead count..."
    local lead_count=$(docker exec supabase_postgres psql -U postgres -d postgres -c "SELECT COUNT(*) FROM leads;" -t 2>/dev/null | xargs)
    if [ -n "$lead_count" ]; then
        log_message "✅ Total leads: $lead_count"
        echo "lead_count: $lead_count" >> "$LOG_FILE"
    else
        log_message "❌ Failed to retrieve lead count"
    fi
}

# Function to check recent pipeline runs
check_ingestion_log() {
    log_message "Checking recent pipeline runs..."
    local recent_runs=$(docker exec supabase_postgres psql -U postgres -d postgres -c "
        SELECT 
            record_count, 
            status, 
            started_at, 
            error_summary 
        FROM ingestion_log 
        ORDER BY started_at DESC 
        LIMIT 3;
    " -t 2>/dev/null | grep -v "^$" | wc -l)
    
    if [ "$recent_runs" -gt 0 ]; then
        log_message "✅ Found $recent_runs recent pipeline runs"
    else
        log_message "⚠️  No recent pipeline runs found"
    fi
}

# Function to check backups
check_backups() {
    log_message "Checking backups..."
    local backup_count=$(find "$BACKUP_DIR" -name "*.sql.gz" -type f 2>/dev/null | wc -l)
    log_message "✅ Found $backup_count backup(s) in $BACKUP_DIR"
    
    # Check if backup from last 24 hours exists
    if find "$BACKUP_DIR" -name "*.sql.gz" -mtime -1 2>/dev/null | grep -q .; then
        log_message "✅ Recent backup (within 24h) exists"
    else
        log_message "⚠️  No recent backup found (last 24h)"
    fi
}

# Function to check web dashboard
check_web_dashboard() {
    log_message "Checking web dashboard..."
    if ps aux | grep "python3 -m http.server 8000" | grep -v grep > /dev/null; then
        log_message "✅ Web server is running on port 8000"
    else
        log_message "⚠️  Web server not running"
    fi
}

# Main execution
log_message "=== Starting Health Monitoring ==="
check_database
check_lead_count
check_ingestion_log
check_backups
check_web_dashboard
log_message "=== Health Monitoring Complete ==="
