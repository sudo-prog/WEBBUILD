#!/usr/bin/env python3
import json, sqlite3, re
from pathlib import Path

ABN_DIR = Path("/home/thinkpad/data/abn/processed")
DB_PATH = Path("/home/thinkpad/data/abn/abn_reference.db")

# Postcode->City mapping (capital cities)
POSTCODE_CITY = {}
ranges = {
    'Sydney':    [(2000,2234),(2250,2268),(2550,2759)],
    'Melbourne': [(3000,3207),(3305,3977)],
    'Brisbane':  [(4000,4011),(4034,4044),(4064,4157),(4500,4576),(4720,4721)],
    'Perth':     [(6000,6038),(6050,6182),(6208,6209),(6503,6770)],
    'Adelaide':  [(5000,5199),(5800,5962)],
    'Canberra':  [(2600,2618),(2900,2920)],
    'Hobart':    [(7000,7054)],
    'Darwin':    [(800,999)],
}
for city, rng in ranges.items():
    for lo, hi in rng:
        for pc in range(lo, hi+1):
            POSTCODE_CITY[pc] = city

def city_from_postcode(pc_str):
    try:
        return POSTCODE_CITY.get(int(pc_str), 'Unknown')
    except:
        return 'Unknown'

# Tokenisation: remove corporate stopwords, punctuation; split on whitespace
STOPWORDS = {'pty', 'ltd', 'limited', 'pl', 'co', '&', 'and', 'the', 'service', 'services',
             'solutions', 'group', 'holdings', 'enterprises', 'Australia', 'aust', 'trading',
             'as', 't/as', 'tradingas', 'prop', 'property', 'investments', 'management'}

def tokenize(text):
    if not text:
        return []
    # Lowercase, replace separators with space, split on non-alphanum
    cleaned = re.sub(r'[^a-z0-9]+', ' ', text.lower())
    tokens = [t for t in cleaned.split() if len(t) >= 3 and t not in STOPWORDS]
    return tokens

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Recreate tables
cur.execute('DROP TABLE IF EXISTS abn_records')
cur.execute('DROP TABLE IF EXISTS abn_word_index')
cur.execute('''
CREATE TABLE abn_records (
    abn TEXT PRIMARY KEY,
    abn_status TEXT,
    abn_status_from TEXT,
    record_last_updated TEXT,
    replaced TEXT,
    entity_type_ind TEXT,
    entity_type_text TEXT,
    legal_name TEXT,
    trading_name TEXT,
    address_state TEXT,
    address_postcode TEXT,
    city TEXT,
    asic_number TEXT,
    asic_number_type TEXT,
    gst_status TEXT,
    gst_status_from TEXT
)
''')
cur.execute('CREATE INDEX idx_state_name ON abn_records(address_state, lower(legal_name))')
cur.execute('CREATE INDEX idx_postcode ON abn_records(address_postcode)')
cur.execute('CREATE INDEX idx_trading_name ON abn_records(lower(trading_name))')
cur.execute('CREATE INDEX idx_city ON abn_records(city)')

# Word index table
cur.execute('''
CREATE TABLE abn_word_index (
    word TEXT,
    abn TEXT,
    state TEXT,
    PRIMARY KEY (word, abn)
)
''')
cur.execute('CREATE INDEX idx_word_state ON abn_word_index(word, state)')
cur.execute('CREATE INDEX idx_abn_word ON abn_word_index(abn)')

conn.commit()

files = sorted(ABN_DIR.glob("leads_part*.jsonl"))
print(f"Importing {len(files)} files into enhanced DB...")
total = 0
for f in files:
    with f.open() as fh:
        record_rows = []
        word_rows = []
        for line in fh:
            r = json.loads(line)
            abn = r.get('abn')
            state = r.get('address_state','')
            pc = r.get('address_postcode','')
            city = city_from_postcode(pc)
            legal = r.get('legal_name') or ''
            trading = r.get('trading_name') or ''
            record_rows.append((
                abn, r.get('abn_status'), r.get('abn_status_from'),
                r.get('record_last_updated'), r.get('replaced'),
                r.get('entity_type_ind'), r.get('entity_type_text'),
                legal, trading, state, pc, city,
                r.get('asic_number'), r.get('asic_number_type'),
                r.get('gst_status'), r.get('gst_status_from')
            ))
            # Tokenize names
            for token in set(tokenize(legal) + tokenize(trading)):
                word_rows.append((token, abn, state))
        cur.executemany('INSERT OR REPLACE INTO abn_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', record_rows)
        cur.executemany('INSERT OR IGNORE INTO abn_word_index VALUES (?,?,?)', word_rows)
        conn.commit()
        total += len(record_rows)
        print(f"  imported {len(record_rows):,} from {f.name}  (word tokens: {len(word_rows)})")

print(f"✅ Total ABN records: {total:,}")
# Stats on index
cur.execute('SELECT COUNT(DISTINCT word) FROM abn_word_index')
word_count = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM abn_word_index')
index_rows = cur.fetchone()[0]
print(f"   word index: {word_count:,} unique words, {index_rows:,} entries")
conn.close()
