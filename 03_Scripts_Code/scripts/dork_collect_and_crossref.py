#!/usr/bin/env python3
"""
Dorking Name Collector — manual browser workflow
Collects business names from Google dork results and cross-references with ABN DB.
"""

import json, argparse, re, sqlite3, sys
from datetime import datetime
from pathlib import Path

DB = "/home/thinkpad/data/abn/abn_reference.db"

def abn_lookup(name: str, state='NSW'):
    conn = sqlite3.connect(DB); conn.row_factory=sqlite3.Row; cur=conn.cursor()
    tokens = [t for t in re.split(r'\W+', name.lower()) if len(t) > 2]
    hits = []
    for tok in tokens[:3]:
        cur.execute("""
          SELECT * FROM abn_records
          WHERE (LOWER(trading_name) LIKE ? OR LOWER(legal_name) LIKE ?)
            AND address_state = ?
          LIMIT 3
        """, (f'%{tok}%', f'%{tok}%', state))
        rows = cur.fetchall()
        if rows:
            hits = [dict(r) for r in rows]
            break
    conn.close()
    return hits

def score_sim(biz_name: str, abn_rec):
    an = (abn_rec.get('trading_name') or abn_rec.get('legal_name') or '').lower()
    at = set(re.split(r'\W+', an))
    bt = set(re.split(r'\W+', biz_name.lower()))
    return len(at & bt) / max(len(at), len(bt), 1)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", required=True)
    ap.add_argument("--input",    required=True, help="Text file: one business name per line")
    ap.add_argument("--output",   required=True, help="Verified leads JSONL")
    ap.add_argument("--min-score", type=float, default=0.4)
    args = ap.parse_args()

    names = [ln.strip() for ln in open(args.input) if ln.strip()]
    print(f"Loaded {len(names)} names from {args.input}")

    leads = []
    for name in names:
        hits = abn_lookup(name)
        if not hits:
            print(f"  ✗ {name[:35]:35}  no ABN")
            continue
        best = max(hits, key=lambda r: score_sim(name, r))
        sim  = score_sim(name, best)
        if sim < args.min_score:
            print(f"  ? {name[:35]:35}  low sim ({sim:.2f}) — skip")
            continue
        lead = {
            "business_name": name, "abn": best.get('abn'), "legal_name": best.get('legal_name'),
            "trading_name": best.get('trading_name'), "category": args.category,
            "state": best.get('address_state'), "postcode": best.get('address_postcode'),
            "city": best.get('city') or 'Sydney',
            "gst_registered": best.get('gst_status') == 'ACT',
            "abn_status": best.get('abn_status'), "entity_type": best.get('entity_type_text'),
            "website_verified": False, "source": "dork_manual_v1",
            "discovered_at": datetime.now().isoformat(),
            "abn_match_score": round(sim,2),
        }
        leads.append(lead)
        print(f"  ✓ {name[:35]:35}  ABN={best.get('abn')}  sim={sim:.2f}")

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output,'w') as f:
        for l in leads: f.write(json.dumps(l)+"\n")
    print(f"\n✓ {len(leads)} leads → {args.output}")
    print("Step 2: run quality pipeline:")
    print(f"  python3 scripts/pipeline_quality_v2.py --input {args.output} --output data/verified/{args.category}_verified.jsonl")

if __name__ == "__main__":
    main()
