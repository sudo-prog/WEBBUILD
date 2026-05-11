#!/bin/bash
# Database Backup Script for Supabase Australia Pipeline
# Creates compressed PostgreSQL dumps and retains 7 days of backups

PROJECT_DIR="/home/thinkpad/Projects/active/WEBBUILD/supabase_australia"
BACKUP_DIR="$PROJECT_DIR/backups"
LOG_FILE="$PROJECT_DIR/backup.log"
DATE=$(date '+%Y-%m-%d_%H%M%S')
HOST="localhost"
PORT="6543"
DATABASE="postgres"
USER="postgres"

# Create backup directory if it doesn't exist
mkdir -p "$BACKUP_DIR"

# Function to log messages
log_message() {
    echo "[$DATE] $1" | tee -a "$LOG_FILE"
}

# Check if Docker container is running
log_message "Starting database backup..."
if ! docker ps | grep -q supabase_postgres; then
    log_message "❌ Docker container not running, starting..."
    if ! docker start supabase_postgres; then
        log_message "❌ Failed to start Docker container"
        exit 1
    fi
    sleep 5  # Wait for container to start
fi

# Create backup file name
BACKUP_FILE="$BACKUP_DIR/postgres_backup_$DATE.sql.gz"

# Perform backup
log_message "Creating backup: $BACKUP_FILE"
if docker exec supabase_postgres pg_dump -U postgres -d postgres | gzip > "$BACKUP_FILE"; then
    log_message "✅ Backup completed successfully"
    echo "Backup size: $(du -h "$BACKUP_FILE" | cut -f1)" >> "$LOG_FILE"
    
    # Clean up backups older than 7 days
    find "$BACKUP_DIR" -name "*.sql.gz" -type f -mtime +7 -delete
    log_message "✅ Cleaned up old backups (older than 7 days)"
    
    # Verify backup
    if gzip -t "$BACKUP_FILE" 2>/dev/null; then
        log_message "✅ Backup file is valid"
    else
        log_message "❌ Backup file is corrupted"
    fi
else
    log_message "❌ Backup failed"
    exit 1
fi
