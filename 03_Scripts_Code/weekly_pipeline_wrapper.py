#!/usr/bin/env python3
"""
Wrapper for weekly enrichment pipeline - runs only on Mondays.
This allows us to use a daily cron job to trigger the pipeline, but only execute it on Mondays.
"""
import datetime
import sys
import subprocess

# Only run on Mondays (weekday() returns 0 for Monday)
if datetime.datetime.now().weekday() != 0:
    print(f"Today is {datetime.datetime.now().strftime('%A')}. Exiting - pipeline runs on Mondays only.")
    sys.exit(0)

# Run the weekly pipeline on Mondays
print(f"Today is {datetime.datetime.now().strftime('%A')}. Starting weekly enrichment pipeline...")
sys.exit(subprocess.call([sys.executable, "weekly_abn_pipeline.py", "--dry-run"], cwd='/home/thinkpad/Projects/active/WEBBUILD/supabase_australia'))
