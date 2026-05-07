#!/usr/bin/env python3
"""Enrichment Monitor - Monitors the enrichment process for errors and restarts."""

import os
import sys
import time
import subprocess
import logging

# Add Hermes tools to path (not needed for internal monitoring)
# sys.path.append('/home/thinkpad/.hermes/hermes-agent/venv/lib/python3.11/site-packages')

# Configuration
PROCESS_CMD = "cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/03_Scripts_Code && python3 enrich_all_cities.py"
LOG_FILE = "/tmp/enrichment_monitor.log"
CHECK_INTERVAL = 30  # seconds

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def log(message):
    logger = logging.getLogger("monitor")
    logger.info(message)

def is_process_running():
    try:
        result = subprocess.run(
            ["pgrep", "-f", "enrich_all_cities.py"],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception as e:
        log(f"Error checking process: {e}")
        return False

def monitor():
    log("Monitor started")
    print(f"Enrichment monitor started. Log: {LOG_FILE}")
    
    while True:
        running = is_process_running()
        if not running:
            log("Enrichment process stopped!")
            # Attempt to restart?
            # For now, just log and exit
            break
        
        # Check for errors in the process output (simplified)
        # In a real implementation, we would parse the output for error patterns
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    setup_logging()
    monitor()