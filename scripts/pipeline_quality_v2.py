#!/usr/bin/env python3
"""
End-to-end Lead Quality v2 pipeline:
  1) Enrich — scrape Google Maps / YP / Facebook for activity/revenue signals
  2) Verify — run 5-layer quality scoring
  3) Export — JSONL ready for Supabase import

Usage:
  python3 pipeline_quality_v2.py --input data/abn/leads/trades_part01.jsonl \\
                                 --output data/verified/leads_v2.jsonl \\
                                 [--limit 1000]

Prereqs:
  - ABN reference DB built  : scripts/abn_stream_parser.py + build_abn_db.py
  - Enrichment scrapers       : scripts/enrich_leads.py (stub → real Playwright)
  - Quality verifier          : scripts/lead_verifier_v2.py
"""

import json, sys, argparse, subprocess, shlex
from pathlib import Path

def run(cmd: str):
    result = subprocess.run(shlex.split(cmd), capture_output=True, text=True, cwd=str(Path(__file__).parent.parent))
    print(result.stdout)
    if result.returncode != 0:
        print("STDERR:", result.stderr[:500])
        raise RuntimeError(f"Command failed: {cmd}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",   required=True, help="Filtered ABN trade leads JSONL")
    ap.add_argument("--output",  required=True, help="Final verified leads JSONL")
    ap.add_argument("--limit",   type=int, default=0, help="Max leads (0=all)")
    args = ap.parse_args()

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    tmp_enriched = args.output.replace(".jsonl","") + "_enriched.jsonl"

    print("=== STAGE 1: Enrichment ===")
    run(f"python3 scripts/enrich_leads.py --input {args.input} --output {tmp_enriched} --limit {args.limit or 0}")

    print("\n=== STAGE 2: Quality Verification ===")
    run(f"python3 scripts/lead_verifier_v2.py --input {tmp_enriched} --output {args.output} --limit {args.limit or 0}")

    # Final summary
    print("\n=== RESULTS SUMMARY ===")
    subprocess.run(["python3","-c",
        f"import json; data=[json.loads(l) for l in open('{args.output}')]; "
        "tot=len(data); prem=sum(1 for d in data if d['priority']=='PREMIUM'); high=sum(1 for d in data if d['priority']=='HIGH'); med=sum(1 for d in data if d['priority']=='MEDIUM'); disc=sum(1 for d in data if d['priority']=='DISCARD'); "
        f\"print('Total: {{}}  PREMIUM: {{}}  HIGH: {{}}  MEDIUM: {{}}  DISCARD: {{}}'.format(tot, prem, high, med, disc))\"])

    print(f"\nFinal verified leads: {args.output}")

if __name__ == "__main__":
    main()
