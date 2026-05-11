#!/usr/bin/env python3
import re
from pathlib import Path

base = Path("/home/thinkpad/Projects/active/WEBBUILD/supabase_australia")
p = base / "ingestion_pipeline.py"
orig = p.read_text()
p.with_suffix(".py.bak").write_text(orig)

# Load replacement blocks
google_new = (base / "google_fetch.txt").read_text()
yellow_new = (base / "yellow_fetch.txt").read_text()
tradie_new = (base / "tradie_fetch.txt").read_text()

orig = re.sub(
    r'    def _fetch_google_business\(self\) -> List\[Dict\]:.*?(?=\n    def _fetch_yellow)',
    google_new, orig, flags=re.DOTALL)
print("[1] google_business replaced")

orig = re.sub(
    r'    def _fetch_yellow_pages\(self\) -> List\[Dict\]:.*?(?=\n    def _fetch_tradie)',
    yellow_new, orig, flags=re.DOTALL)
print("[2] yellow_pages replaced")

orig = re.sub(
    r'    def _fetch_tradie_portal\(self\) -> List\[Dict\]:.*?(?=\n    def _fetch_manual)',
    tradie_new, orig, flags=re.DOTALL)
print("[3] tradie_portal replaced")

p.write_text(orig)
print(f"[4] Pipeline written: {{len(orig)}} chars")
