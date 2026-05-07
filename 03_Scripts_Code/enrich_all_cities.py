#!/usr/bin/env python3
"""
Enrichment Pipeline Monitor Wrapper
Starts the enrichment process and automatically launches the monitor.
"""

import subprocess
import sys
import time

# Start the monitor in the background
monitor_cmd = (
    "cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/03_Scripts_Code && "
    "python3 monitor_enrichment.py"
)
subprocess.Popen(monitor_cmd, shell=True)

# Start the enrichment process
enrichment_cmd = (
    "cd /home/thinkpad/Projects/active/WEBBUILD/supabase_australia/03_Scripts_Code && "
    "python3 abn_enrichment.py --city all"
)
result = subprocess.run(enrichment_cmd, shell=True, capture_output=True, text=True)

# Output the result
print(result.stdout)
if result.stderr:
    print(f"ERROR: {result.stderr}", file=sys.stderr)
sys.exit(result.returncode)