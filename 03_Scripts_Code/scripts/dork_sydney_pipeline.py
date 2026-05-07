#!/usr/bin/env python3
"""
Sydney Trade Dork Pipeline — v1.0

Strategy:  For each trade, generate 3 dork queries per suburb, scrape results
(manual or automated), extract business names, then batch cross-reference
with ABN reference DB to produce verified leads ready for quality scoring.

Since automated Google search is blocked by CAPTCHA, this script supports
two modes:

  MODE A — Manual (recommended for testing):
    1. Script prints dork queries to run
    2. User copies each into Google, collects business names from results
    3. Paste names into a text file (one per line)
    4. Script cross-references with ABN DB and outputs leads

  MODE B — Automated (when you have a SERP API key):
    Provide --serpapi-key to auto-run queries and parse results.

Usage manual:
  python3 dork_sydney_pipeline.py --category plumber --mode manual --output leads.jsonl

Usage auto:
  python3 dork_sydney_pipeline.py --category electrician --mode auto --serpapi-key $KEY --output leads.jsonl
"""

import json, argparse, sqlite3, re, sys, os
from datetime import datetime
from pathlib import Path
from typing import List, Dict

DB_PATH = "/home/thinkpad/data/abn/abn_reference.db"

# Full trade map (12 categories) — subsets for batch runs
TRADE_KEYWORDS = {
    "plumber":        ["plumber","plumbing","drain","blocked drain","leak detection","hot water"],
    "electrician":    ["electrician","electrical","sparky","wiring","switchboard","lighting"],
    "builder":        ["builder","carpenter","construction","renovation","extensions","home builder"],
    "painter":        ["painter","painting","decorator","wallpaper","stripping"],
    "roofer":         ["roofer","roofing","tiling","guttering","downpipes","metal roof"],
    "air conditioning": ["air conditioning","hvac","ducted","split system","cooling","evaporative"],
    "kitchen":        ["kitchen","bathroom","joinery","cabinet","benchtop","outdoor kitchen"],
    "flooring":       ["flooring","tiles","laminate","carpet","polished concrete","timber floor"],
    "solar":          ["solar","solar panel","photovoltaic","pv","solar power","battery"],
    "pest control":   ["pest control","termite","exterminator","rodent","fumigation"],
    "gardener":       ["gardener","landscaper","tree","lawn mowing","hedge trimming","garden design"],
    "mechanic":       ["mechanic","auto repair","vehicle","car service","mechanical","tyre"],
}

SYDNEY_SUBURBS = [
    "Sydney CBD","Parramatta","Chatswood","Hurstville","Bankstown",
    "Blacktown","Bondi","Cronulla","Newcastle","Penrith"
]

DORK_TEMPLATES = [
    '{trade} {suburb} -site:.au -site:.com -site:http -site:https',
    '{trade} {suburb} "official website"',
    '{trade} {suburb} .com.au',
]

# ─── ABN DB HELPERS ─────────────────────────────────────────────────────────────
def abn_search(name: str, state: str = "NSW", limit: int = 3) -> List[Dict]:
    conn = sqlite3.connect(DB_PATH); conn.row_factory=sqlite3.Row; cur=conn.cursor()
    tokens = [t for t in re.split(r'\W+', name.lower()) if len(t) > 2]
    hits = []
    for tok in tokens[:3]:
        cur.execute(f"""
          SELECT * FROM abn_records
          WHERE (LOWER(trading_name) LIKE ? OR LOWER(legal_name) LIKE ?)
            AND address_state = ?
          LIMIT ?
        """, (f'%{tok}%', f'%{tok}%', state, limit))
        rows = cur.fetchall()
        if rows:
            hits = [dict(r) for r in rows]
            break
    conn.close()
    return hits

def similarity(a: str, b: str) -> float:
    at = set(re.split(r'\W+', a.lower()))
    bt = set(re.split(r'\W+', b.lower()))
    return len(at & bt) / max(len(at), len(bt), 1)

# ─── MODE A: MANUAL DORK LIST GENERATOR ────────────────────────────────────────
def generate_dork_list(category: str, out_file: str):
    keywords = TRADE_KEYWORDS[category]
    queries = []
    for kw in keywords:
        for sub in SYDNEY_SUBURBS:
            for tpl in DORK_TEMPLATES:
                queries.append(tpl.format(trade=kw, suburb=sub))
    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    with open(out_file,'w') as f:
        for q in queries:
            f.write(q + "\n")
    print(f"✓ {len(queries)} dork queries written to {out_file}")
    print("\nInstructions:")
    print("  1. Open the file and copy queries in batches into Google")
    print("  2. From each results page, extract business names (titles of listings)")
    print("  3. Paste names into a text file: data/dork_results/plumber_names.txt")
    print("  4. Return here with --mode manual --input that file to cross-reference ABN")

# ─── MODE A: MANUAL NAME FILE → ABN CROSS-REF ───────────────────────────────────
def process_name_file(name_file: str, category: str, out_file: str):
    if not Path(name_file).exists():
        print(f"File not found: {name_file}")
        sys.exit(1)
    with open(name_file) as f:
        names = [line.strip() for line in f if line.strip()]
    print(f"Loaded {len(names)} business names")

    verified = []
    for name in names:
        hits = abn_search(name, state='NSW')
        if hits:
            # Pick best match
            best = max(hits, key=lambda r: similarity(name, r.get('trading_name','') or r.get('legal_name','')))
            sim = similarity(name, best.get('trading_name','') or best.get('legal_name',''))
            if sim >= 0.4:   # threshold for match confidence
                lead = {
                    "business_name":      name,
                    "abn":                best.get('abn'),
                    "legal_name":         best.get('legal_name'),
                    "trading_name":       best.get('trading_name'),
                    "category":           category,
                    "state":              best.get('address_state'),
                    "postcode":           best.get('address_postcode'),
                    "city":               best.get('city','Sydney'),
                    "gst_registered":     best.get('gst_status') == 'ACT',
                    "abn_status":         best.get('abn_status'),
                    "entity_type":        best.get('entity_type_text'),
                    "website_verified":   False,   # will be set in L5
                    "source":             "dork_manual_v1",
                    "discovered_at":      datetime.now().isoformat(),
                    "abn_match_score":    round(sim, 2),
                }
                verified.append(lead)
                print(f"✓ {name[:35]:35}  ABN={best.get('abn')}  sim={sim:.2f}")
            else:
                print(f"✗ {name[:35]:35}  low similarity ({sim:.2f})")
        else:
            print(f"✗ {name[:35]:35}  no ABN match")

    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    with open(out_file,'w') as f:
        for v in verified:
            f.write(json.dumps(v) + "\n")
    print(f"\n✓ {len(verified)} verified leads → {out_file}")

# ─── MODE B: AUTOMATED SERPAPI ──────────────────────────────────────────────────
def process_serpapi(category: str, serpapi_key: str, out_file: str):
    import requests
    keywords = TRADE_KEYWORDS[category]
    verified = []
    for kw in keywords:
        for suburb in SYDNEY_SUBURBS:
            for tpl in DORK_TEMPLATES[:1]:  # only the noise-filter query
                query = tpl.format(trade=kw, suburb=suburb)
                print(f"Searching: {query}")
                resp = requests.get("https://serpapi.com/search", params={
                    "q": query, "engine":"google", "api_key": serpapi_key, "num":10
                })
                if resp.ok:
                    data = resp.json()
                    for result in data.get('organic_results', []):
                        title = result.get('title','')
                        # Extract business name
                        name = re.split(r' \|| - ', title)[0].strip()
                        if not name: continue
                        # ABN cross-ref
                        hits = abn_search(name)
                        if hits:
                            best = max(hits, key=lambda r: similarity(name, r.get('trading_name','') or r.get('legal_name','')))
                            sim = similarity(name, best.get('trading_name','') or best.get('legal_name',''))
                            if sim >= 0.4:
                                verified.append({
                                    "business_name": name, "abn": best.get('abn'),
                                    "category": category, "state": best.get('address_state'),
                                    "gst_registered": best.get('gst_status')=='ACT',
                                    "source": "dork_serpapi_v1", "abn_match_score": round(sim,2),
                                })
                else:
                    print(f"  SERP error {resp.status_code}")
    with open(out_file,'w') as f:
        for v in verified:
            f.write(json.dumps(v)+"\n")
    print(f"✓ {len(verified)} leads → {out_file}")

# ─── CLI ────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--category",  required=True, choices=TRADE_KEYWORDS.keys())
    ap.add_argument("--mode",      choices=["manual","auto"], default="manual")
    ap.add_argument("--input",     help="[manual] text file with one business name per line")
    ap.add_argument("--output",    default="data/leads/sydney_{category}_dorked.jsonl")
    ap.add_argument("--serpapi-key", help="[auto] SerpAPI key for automated searches")
    args = ap.parse_args()

    out_path = args.output.format(category=args.category)

    if args.mode == "manual":
        if not args.input:
            # Generate dork list
            query_list = f"data/dork_queries/sydney_{args.category}_queries.txt"
            generate_dork_list(args.category, query_list)
            print(f"\n→ Next: run these queries, collect names, then re-run with --input names.txt")
        else:
            process_name_file(args.input, args.category, out_path)
    else:
        if not args.serpapi_key:
            print("Need --serpapi-key for auto mode"); sys.exit(1)
        process_serpapi(args.category, args.serpapi_key, out_path)
