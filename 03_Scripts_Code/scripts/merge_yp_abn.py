#!/usr/bin/env python3
"""Combine Yellow Pages scraped leads with ABN reference data."""

import json, re, sys
from pathlib import Path
from collections import defaultdict

YP_DIR        = Path("/home/thinkpad/Projects/supabase_australia/raw_leads/yellow_pages")
ABN_LEADS_DIR = Path("/home/thinkpad/data/abn/leads")
OUTPUT_PATH   = Path("/home/thinkpad/Projects/supabase_australia/data/weekly_leads.json")

# Check input paths exist before proceeding
if not YP_DIR.exists():
    print(f"[FATAL] YP_DIR does not exist: {YP_DIR}")
    sys.exit(1)
if not ABN_LEADS_DIR.exists():
    print(f"[FATAL] ABN_LEADS_DIR does not exist: {ABN_LEADS_DIR}")
    sys.exit(1)

STOPWORDS = {'pty','ltd','limited','pl','co','&','and','the','service','services','solutions','group','holdings','enterprises','aust','trading','t/as','tradingas','prop','property','investments','management','tr','ft','trust','family','super','fund','partnership','partners'}

def normalize_name(name):
    if not name:
        return ''
    name = name.lower()
    # Replace any non-alphanumeric with space, then strip stopwords
    name = re.sub(r'[^a-z0-9]+', ' ', name)
    tokens = [t for t in name.split() if len(t) >= 3 and t not in STOPWORDS]
    return ' '.join(sorted(tokens))

def name_similarity(a, b):
    ta, tb = set(a.split()), set(b.split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

print("Loading ABN trade leads...")
abn_records = []
for f in sorted(ABN_LEADS_DIR.glob('trades_*.jsonl')):
    with f.open() as fh:
        for line in fh:
            r = json.loads(line)
            r['_norm_legal'] = normalize_name(r.get('legal_name',''))
            r['_norm_trading'] = normalize_name(r.get('trading_name',''))
            abn_records.append(r)
print(f"  Loaded {len(abn_records):,} ABN trade records")

by_state = defaultdict(list)
for r in abn_records:
    by_state[r.get('address_state','')].append(r)

print("Loading YP leads...")
yp_leads = []
for f in sorted(YP_DIR.glob('yp_*.json')):
    with f.open() as fh:
        data = json.load(fh)
        if isinstance(data, list):
            yp_leads.extend(data)
        elif isinstance(data, dict) and 'results' in data:
            yp_leads.extend(data['results'])
print(f"  Loaded {len(yp_leads):,} YP leads")

no_web_leads = [l for l in yp_leads if not l.get('has_website', True)]
print(f"  No-website YP leads: {len(no_web_leads):,}")

MATCH_THRESHOLD = 0.6
matched = []
unmatched = []

for yp in no_web_leads:
    state = yp.get('state','').upper()
    candidates = by_state.get(state, [])
    yp_norm = normalize_name(yp.get('business_name',''))
    best_match = None
    best_score = 0.0
    for cand in candidates:
        if yp_norm == cand['_norm_legal'] or yp_norm == cand['_norm_trading']:
            best_match = cand; best_score = 1.0; break
        score = name_similarity(yp_norm, cand['_norm_legal'])
        if score > best_score:
            best_score = score; best_match = cand
        score2 = name_similarity(yp_norm, cand['_norm_trading'])
        if score2 > best_score:
            best_score = score2; best_match = cand
    if best_match and best_score >= MATCH_THRESHOLD:
        lead = {
            'business_name': yp['business_name'],
            'abn': best_match.get('abn'),
            'category': yp.get('category','trade'),
            'state': state,
            'city': best_match.get('city') or yp.get('city','').title(),
            'postcode': best_match.get('address_postcode'),
            'suburb': None,
            'address_full': yp.get('address') or '',
            'phone': yp.get('phone'),
            'mobile': None,
            'email': None,
            'website': None,
            'source': 'yellow_pages_no_website',
            'lead_score': 50,
            'needs_review': False,
        }
        matched.append(lead)
    else:
        unmatched.append(yp)

print(f"Matched: {len(matched):,} | Unmatched: {len(unmatched):,}")
if matched:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(matched, indent=2))
    print(f"Written {len(matched)} leads → {OUTPUT_PATH}")
if unmatched:
    (OUTPUT_PATH.parent / 'unmatched_yp_leads.json').write_text(json.dumps(unmatched, indent=2))
    print(f"Unmatched saved ({len(unmatched)})")
