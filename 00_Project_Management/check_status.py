#!/usr/bin/env python3
"""Status report for Supabase Australia pipeline."""

import os, sys, socket, subprocess
from pathlib import Path

PROJECT = Path(__file__).parent.parent / 'supabase_australia'

def check_file(path, desc):
    exists = Path(path).exists()
    status = "OK" if exists else "MISSING"
    symbol = "✅" if exists else "❌"
    print(f"  {symbol} {desc}: {path}")
    return exists

def check_port(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.5)
        try:
            s.connect(('127.0.0.1', port))
            return True
        except:
            return False

print("=" * 60)
print("  SUPABASE AUSTRALIA — STATUS REPORT")
print("=" * 60)

print("\nProject files:")
base = PROJECT
files_ok = []
files_ok.append(check_file(base / 'README.md', 'Documentation'))
files_ok.append(check_file(base / 'ingestion_pipeline.py', 'Main pipeline'))
files_ok.append(check_file(base / 'requirements.txt', 'Python deps'))
files_ok.append(check_file(base / 'config' / 'settings.json', 'Configuration'))
files_ok.append(check_file(base / 'schema' / '001_initial_schema.sql', 'SQL schema'))
files_ok.append(check_file(base / 'Makefile', 'Makefile'))

print("\nCity CSV inputs (sample data):")
csv_ok = []
for city in ['sydney','melbourne','brisbane','perth','adelaide','hobart','darwin','canberra']:
    csv_ok.append(check_file(base / 'data' / 'inputs' / f'{city}_leads.csv', city.title() + ' sample'))

print("\nServices:")
p6543 = check_port(6543)
p8080 = check_port(8080)
print(f"  {'✅' if p6543 else '❌'} PostgreSQL port 6543: {'OPEN' if p6543 else 'CLOSED (run ./start_supabase.sh)'}")
print(f"  {'✅' if p8080 else '⚪'} Studio UI port 8080: {'OPEN' if p8080 else 'not running'}")

print("\nPython packages:")
for pkg in ['supabase', 'requests']:
    r = subprocess.run([sys.executable, '-c', f'import {pkg}'], capture_output=True)
    print(f"  {'✅' if r.returncode == 0 else '❌'} {pkg}")

print("\nInventory:")
file_count = sum(1 for _ in base.rglob('*') if _.is_file())
print(f"  Total files: {file_count}")
print(f"  SQL schema size: {(base / 'schema' / '001_initial_schema.sql').stat().st_size:,} bytes")
print(f"  Pipeline code: {(base / 'ingestion_pipeline.py').stat().st_size:,} bytes")

all_ok = all(files_ok) and all(csv_ok)
print("\n" + "=" * 60)
if all_ok:
    print("  SYSTEM READY — All components in place")
    print("  Start: ./start_supabase.sh  |  Or: make start")
else:
    print("  INCOMPLETE — Re-run creation steps")
print("=" * 60)
