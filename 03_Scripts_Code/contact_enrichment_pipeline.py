#!/usr/bin/env python3
"""Contact Enrichment Pipeline - Orchestrates Google/Yellow Pages cross-referencing
to add phone/email to leads in Supabase."""
import subprocess
import sys
import os
from pathlib import Path

def run_command(cmd, cwd=None):
    """
    Run a command and return its output.
    """
    print(f"▶ Running: {' '.join(cmd)}")
    print(f"    in directory: {cwd}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    print(f"⏱  Command completed with exit code {result.returncode}")
    if result.stdout:
        print("stdout (first line):", result.stdout.splitlines()[0] if result.stdout.splitlines() else "")
    if result.stderr:
        print("stderr (first line):", result.stderr.splitlines()[0] if result.stderr.splitlines() else "")
    return result

def main():
    project_root = Path(__file__).parent.parent
    cwd = str(project_root)
    print(f"DEBUG: project_root = {project_root}")
    print(f"DEBUG: cwd = {cwd}")
    
    print("=" * 70)
    print("CONTACT ENRICHMENT PIPELINE STARTED")
    print("=" * 70)
    
    # Define cities and states to process
    cities = [
        {"city": "Sydney", "state": "NSW"},
        {"city": "Melbourne", "state": "VIC"},
        {"city": "Brisbane", "state": "QLD"},
        {"city": "Perth", "state": "WA"},
        {"city": "Adelaide", "state": "SA"},
        {"city": "Hobart", "state": "TAS"},
        {"city": "Darwin", "state": "NT"},
        {"city": "Canberra", "state": "ACT"},
    ]
    
    # Step 1: Scrape YP per city
    print("[1/3] Running scrape_yp_playwright.py for all cities...")
    for city_info in cities:
        print(f"  → Running for {city_info['city']} ({city_info['state']})...")
        result = run_command([
            "python", "03_Scripts_Code/scripts/scrape_yp_playwright.py",
            city_info["city"], city_info["state"]
        ], cwd=cwd)
        if result.returncode != 0:
            print(f"❌ Scrape failed for {city_info['city']}, exiting.")
            sys.exit(1)
    
    # Step 2: Merge YP data with ABN leads
    print("[2/3] Running merge_yp_abn.py...")
    result = run_command([
        "python", "03_Scripts_Code/scripts/merge_yp_abn.py"
    ], cwd=cwd)
    if result.returncode != 0:
        print("❌ Merge failed, exiting.")
        sys.exit(1)
    
    # Step 3: Import enriched leads into Supabase
    print("[3/3] Running import_leads.py...")
    # Find the most recent consolidated file
    import glob
    pattern = project_root / "data" / "enriched_leads_*.json"
    files = list(glob.glob(str(pattern)))
    if not files:
        pattern = project_root / "data" / "weekly_leads_*.json"
        files = list(glob.glob(str(pattern)))
    
    if files:
        latest_file = max(files, key=os.path.getctime)
        print(f"Using latest enriched file: {latest_file}")
        result = run_command([
            "python", "03_Scripts_Code/scripts/import_leads.py",
            latest_file
        ], cwd=cwd)
        if result.returncode != 0:
            print("❌ Import failed, exiting.")
            sys.exit(1)
    else:
        print("❌ No enriched leads file found, exiting.")
        sys.exit(1)
    
    print("\n" + "=" * 70)
    print("✅ CONTACT ENRICHMENT PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

if __name__ == "__main__":
    main()
