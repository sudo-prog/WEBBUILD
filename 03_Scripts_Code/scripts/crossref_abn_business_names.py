#!/usr/bin/env python3
"""
Cross-reference a list of business names against the ABN reference database.
Takes plain business names (one per line or JSON array), looks up matching
active ABN records, and outputs enriched leads for quality verification.

Usage:
  python crossref_abn_business_names.py --input my_businesses.txt --output results.jsonl
  python crossref_abn_business_names.py --input businesses.json --format json
  cat names.txt | python crossref_abn_business_names.py --format lines --output enriched.jsonl
"""

import sys, re, json, sqlite3, csv, argparse, pathlib
from datetime import datetime

# ─── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ABN_DB_PATH   = pathlib.Path("/home/thinkpad/data/abn/abn_reference.db")

# ─── SQLite connection ─────────────────────────────────────────────────────────
def connect_abn():
    if not ABN_DB_PATH.exists():
        sys.exit(f"ABN reference DB not found at {ABN_DB_PATH}. Run build_abn_reference_db.py first.")
    return sqlite3.connect(str(ABN_DB_PATH))

# ─── Normalisation helpers ─────────────────────────────────────────────────────
def norm(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[\&/+\-]', ' ', s)
    s = re.sub(r'\b(p/l|pty|ltd|ptn|abn|acn)\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

STOPWORDS = {'the', 'a', 'an', 'and', '&', 'of', 'for', 'in', 'on', 'at', 'by'}

def norm_tokens(s: str):
    return {t for t in norm(s).split() if t not in STOPWORDS and len(t) > 1}

def token_overlap(a: str, b: str, threshold: float = 0.5) -> bool:
    ta, tb = norm_tokens(a), norm_tokens(b)
    if not ta or not tb:
        return False
    return len(ta & tb) / len(ta | tb) >= threshold

# ─── ABN lookup ────────────────────────────────────────────────────────────────
def lookup_by_name(cur: sqlite3.Cursor, name: str, state: str | None = None, limit: int = 10):
    tokens = norm_tokens(name)
    if not tokens:
        return []

    placeholders = ','.join('?' * len(tokens))
    sql_parts = []
    params = []
    for t in tokens:
        sql_parts.append("lower(legal_name) LIKE ?")
        sql_parts.append("lower(trading_name) LIKE ?")
        params.extend([f"%{t}%", f"%{t}%"])
    where_clause = " OR ".join(sql_parts)

    sql = f'''
        SELECT abn, abn_status, entity_type_text, legal_name, trading_name, address_state, gst_status
        FROM abn_records
        WHERE abn_status = 'ACT'
          AND ({where_clause})
    '''
    if state:
        sql += " AND address_state = ?"
        params.append(state.upper())
    sql += " LIMIT ?"
    params.append(limit)

    cur.execute(sql, params)
    rows = cur.fetchall()

    results = []
    for row in rows:
        abn, status, entity, legal, trading, addr_state, gst = row
        match_name = trading or legal
        if token_overlap(name, match_name):
            results.append({
                "abn":             abn,
                "abn_status":      status,
                "entity_type":     entity,
                "legal_name":      legal,
                "trading_name":    trading,
                "address_state":   addr_state,
                "gst_registered":  gst == 'ACT',
                "matched_name":    match_name,
            })
    def sort_key(r):
        gst_bonus = 0 if r['gst_registered'] else 1
        entity_rank = 0 if r['entity_type'] and 'Company' in r['entity_type'] else 2
        return (gst_bonus, entity_rank)
    results.sort(key=sort_key)
    return results[:limit]

# ─── Duplicate check against Supabase ─────────────────────────────────────────
def already_in_supabase(cur, business_name: str, abn: str | None = None) -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=6543, dbname="postgres",
            user="supabase_service", password="supabase_service_1777698346"
        )
        with conn:
            with conn.cursor() as c:
                c.execute('''
                    SELECT 1 FROM leads
                    WHERE lower(business_name) = lower(%s)
                       OR (abn IS NOT NULL AND abn = %s)
                    LIMIT 1
                ''', (business_name, abn))
                return c.fetchone() is not None
    except Exception as e:
        print(f"[WARN] Supabase check failed: {e}", file=sys.stderr)
        return False

# ─── Main pipeline ─────────────────────────────────────────────────────────────
def process_business_names(names, output, include_dupes: bool = False, state_filter: str | None = None):
    abn_conn = connect_abn()
    abn_cur  = abn_conn.cursor()

    for name in names:
        name = name.strip()
        if not name:
            continue

        matches = lookup_by_name(abn_cur, name, state=state_filter, limit=5)
        if not matches:
            record = {
                "input_name":     name,
                "matched":        False,
                "abn":            None,
                "entity_type":    None,
                "address_state":  state_filter,
                "quality_flags":  ["NO_ABN_MATCH"],
                "discovered_at":  datetime.now().isoformat(),
            }
            output.write(json.dumps(record) + "\n")
            continue

        best = matches[0]
        dup  = already_in_supabase(None, name, best['abn'])
        record = {
            "input_name":     name,
            "matched":        True,
            "abn":            best['abn'],
            "legal_name":     best['legal_name'],
            "trading_name":   best['trading_name'],
            "entity_type":    best['entity_type'],
            "address_state":  best['address_state'],
            "gst_registered": best['gst_registered'],
            "abn_status":     best['abn_status'],
            "in_supabase":    dup,
            "quality_flags":  [],
            "discovered_at":  datetime.now().isoformat(),
        }
        if dup and not include_dupes:
            record["quality_flags"].append("ALREADY_IN_SUPABASE")

        output.write(json.dumps(record) + "\n")

    abn_conn.close()

# ─── CLI ───────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Cross-reference business names with ABN DB")
    ap.add_argument('--input', type=argparse.FileType('r'), default=sys.stdin)
    ap.add_argument('--output', type=argparse.FileType('w'), default=sys.stdout)
    ap.add_argument('--format', choices=['lines', 'json', 'csv'], default='lines')
    ap.add_argument('--state', help="Filter ABN matches by state (e.g. NSW, VIC)")
    ap.add_argument('--include-dupes', action='store_true')
    ap.add_argument('--limit', type=int, default=1000)
    args = ap.parse_args()

    names = []
    if args.format == 'lines':
        names = [line.strip() for line in args.input]
    elif args.format == 'json':
        data = json.load(args.input)
        names = [str(n) for n in data] if isinstance(data, list) else sys.exit("JSON must be array")
    elif args.format == 'csv':
        reader = csv.DictReader(args.input)
        col = 'business_name' if 'business_name' in reader.fieldnames else (reader.fieldnames[0] if reader.fieldnames else 'name')
        names = [row.get(col, '').strip() for row in reader]

    names = names[:args.limit]
    print(f"[INFO] Processing {len(names)} names — state={args.state or 'any'}", file=sys.stderr)
    process_business_names(names, args.output, args.include_dupes, args.state)
    print(f"[DONE] Wrote {len(names)} records", file=sys.stderr)

if __name__ == '__main__':
    main()
