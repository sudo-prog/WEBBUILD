#!/usr/bin/env python3
import json, sqlite3, re
from pathlib import Path

ABN_DIR = Path("/home/thinkpad/data/abn/processed")
DB_PATH = Path("/home/thinkpad/data/abn/abn_reference.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS abn_records (
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
    asic_number TEXT,
    asic_number_type TEXT,
    gst_status TEXT,
    gst_status_from TEXT
)
''')
cur.execute('CREATE INDEX IF NOT EXISTS idx_state_name ON abn_records(address_state, lower(legal_name))')
cur.execute('CREATE INDEX IF NOT EXISTS idx_postcode ON abn_records(address_postcode)')
cur.execute('CREATE INDEX IF NOT EXISTS idx_trading_name ON abn_records(lower(trading_name))')
conn.commit()

files = sorted(ABN_DIR.glob("leads_part*.jsonl"))
print(f"Importing {len(files)} files...")
total = 0
for f in files:
    with f.open() as fh:
        rows = []
        for line in fh:
            r = json.loads(line)
            rows.append((
                r.get('abn'), r.get('abn_status'), r.get('abn_status_from'),
                r.get('record_last_updated'), r.get('replaced'),
                r.get('entity_type_ind'), r.get('entity_type_text'),
                r.get('legal_name'), r.get('trading_name'),
                r.get('address_state'), r.get('address_postcode'),
                r.get('asic_number'), r.get('asic_number_type'),
                r.get('gst_status'), r.get('gst_status_from')
            ))
        cur.executemany('INSERT OR REPLACE INTO abn_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
        conn.commit()
        total += len(rows)
        print(f"  imported {len(rows):,} from {f.name}")

print(f"✅ Total ABN records in DB: {total:,}")
conn.close()
