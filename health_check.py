#!/usr/bin/env python3
"""Supabase Australia Daily Health Monitor"""

import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime, timedelta
import os
import re
import glob

# Database configuration
SUPABASE_URL = "localhost"
SUPABASE_PORT = 6543
SUPABASE_DB = "postgres"
SUPABASE_USER = "postgres"
SUPABASE_PASS = "supabase_service_1777905407"

def connect_db():
    try:
        conn = psycopg2.connect(
            host=SUPABASE_URL, port=SUPABASE_PORT, dbname=SUPABASE_DB,
            user=SUPABASE_USER, password=SUPABASE_PASS
        )
        return conn, None
    except Exception as e:
        return None, str(e)

def check_database():
    conn, error = connect_db()
    if conn:
        conn.close()
        return True, "Connected successfully"
    else:
        return False, f"Connection failed: {error}"

def count_leads():
    conn, error = connect_db()
    if not conn:
        return 0, error
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM leads WHERE is_active = true;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count, None
    except Exception as e:
        return 0, str(e)

def get_recent_ingestions(hours=24):
    conn, error = connect_db()
    if not conn:
        return 0, error
    
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) 
            FROM ingestion_log 
            WHERE started_at > NOW() - INTERVAL '%s hours'
        """, (hours,))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count, None
    except Exception as e:
        return 0, str(e)

def check_recent_backup():
    backup_files = glob.glob("supabase_australia_*.sql.gz")
    
    if not backup_files:
        return None, "No backup files found"
    
    latest_backup = max(backup_files, key=os.path.getctime)
    
    match = re.search(r'supabase_australia_(\d{8})_(\d{6})\.sql\.gz', latest_backup)
    if match:
        date_str = match.group(1)
        time_str = match.group(2)
        backup_time = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M%S")
        return latest_backup, backup_time
    else:
        return latest_backup, "Unknown timestamp"

def check_errors():
    try:
        with open("monitor.log", "r") as f:
            lines = f.readlines()
        
        errors = [line for line in lines if "ERROR" in line]
        return len(errors), errors[-5:] if errors else []
    except Exception as e:
        return 0, [f"Could not read log file: {e}"]

def main():
    border = "#" * 60
    print(border)
    print("# Supabase Australia Health Check")
    print("# Date: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    print(border)
    print()
    
    db_status, db_msg = check_database()
    print("Database Status: " + ("✓ UP" if db_status else "✗ DOWN"))
    print("  Message: " + db_msg)
    print()
    
    if db_status:
        lead_count, lead_error = count_leads()
        if lead_error:
            print("Lead Count: ✗ Error - " + lead_error)
        else:
            print("Total Active Leads: {:,}".format(lead_count))
        print()
        
        ingestion_count, ingestion_error = get_recent_ingestions(24)
        if ingestion_error:
            print("Recent Ingestions (24h): ✗ Error - " + ingestion_error)
        else:
            print("Ingested records in last 24h: {:,}".format(ingestion_count))
        print()
        
        backup_file, backup_time = check_recent_backup()
        if backup_file:
            if isinstance(backup_time, datetime):
                time_ago = datetime.now() - backup_time
                hours_ago = time_ago.total_seconds() / 3600
                print("Recent Backup: ✓ " + backup_file)
                print("  Backup Time: " + backup_time.strftime('%Y-%m-%d %H:%M:%S'))
                print("  Hours Ago: {:.1f}".format(hours_ago) + " hours")
            else:
                print("Recent Backup: ✓ " + backup_file + " (timestamp unknown)")
        else:
            print("Backup Status: ✗ No backup found")
        print()
    
    error_count, recent_errors = check_errors()
    print("Error Count (in monitor.log): " + str(error_count))
    if error_count > 0:
        print("Recent Errors:")
        for err in recent_errors:
            print("  • " + err.strip())
    
    print(border)
    print("# Health Check Completed")
    print(border)

if __name__ == "__main__":
    main()
