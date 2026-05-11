#!/bin/bash
# Backup script for Supabase Australia database
# Runs daily at 3 AM

BACKUP_DIR="/home/thinkpad/Projects/active/WEBBUILD/supabase_australia/backups"
DATE=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="${BACKUP_DIR}/supabase_australia_${DATE}.sql.gz"

# Ensure backup directory exists
mkdir -p "${BACKUP_DIR}"

# Dump and compress the database
docker exec supabase_postgres pg_dump -U postgres postgres | gzip > "${BACKUP_FILE}"

# Optional: copy to another location (e.g., external drive)
# cp "${BACKUP_FILE}" /path/to/remote/backup/

# Keep only last 7 days of backups
find "${BACKUP_DIR}" -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: ${BACKUP_FILE}"
