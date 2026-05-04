#!/usr/bin/env python3
import sqlite3, json, os
from datetime import datetime

DB_PATH  = "/home/thinkpad/data/abn/abn_reference.db"
OUT_DIR  = "/home/thinkpad/data/abn/leads/construction_trades"
os.makedirs(OUT_DIR, exist_ok=True)

CITIES = ["Brisbane", "Sydney", "Gold Coast", "Melbourne", "Darwin", "Perth"]

RAW_KEYWORDS = [
    "construction manager", "site manager", "construction supervisor",
    "builder", "project manager", "site supervisor", "foreman",
    "building supervisor", "contract manager",
    "electrician", "electrical contractor", "auto electrician",
    "industrial electrician", "residential electrician", "commercial electrician",
    "electrical installer", "solar electrician",
    "plumber", "gas fitter", "plumbing contractor", "gas installer",
    "hot water", "water plumber", "drainage", "septic", "roof plumber",
    "boilermaker", "welder", "metal fabricator", "structural steel",
    "welding contractor", "boiler maker",
    "hvac", "air conditioning", "refrigeration mechanic",
    "heating and cooling", "ducting", "mechanical services",
    "concreter", "concrete contractor", "concreting", "flooring",
    "floor sander", "epoxy flooring", "polished concrete", "tiling",
    "tradesperson", "trades assistant", "mechanic", "carpenter",
    "joiner", "cabinet maker", "handyman", "maintenance trades"
]
STOP_WORDS = {"and","or","the","a","an","co","ltd","pty","services","contractor","installer","specialist","group"}
KEYWORDS = set()
for phrase in RAW_KEYWORDS:
    for word in phrase.lower().split():
        if word not in STOP_WORDS and len(word) > 2:
            KEYWORDS.add(word)

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    kw_ph   = ','.join('?' * len(KEYWORDS))
    city_ph = ','.join('?' * len(CITIES))
    sql = (
        "SELECT r.* FROM abn_records r "
        "WHERE r.abn IN (SELECT abn FROM abn_word_index WHERE word IN (" + kw_ph + ")) "
        "AND r.city IN (" + city_ph + ") "
        "AND r.abn_status = 'ACT' "
        "AND r.entity_type_text LIKE '%Private%'"
    )
    cur.execute(sql, list(KEYWORDS) + CITIES)
    rows = cur.fetchall()
    print("[*] {} records after FTS join + city + ACT + Private".format(len(rows)))

    matched, by_city = [], {c:0 for c in CITIES}
    for row in rows:
        rec  = dict(row)
        name = (rec.get('trading_name') or rec.get('legal_name') or '').strip()
        city_key = next((c for c in CITIES if c.lower() == (rec.get('city') or '').lower()), None)
        if city_key:
            by_city[city_key] += 1
            lead = {
                "abn": rec['abn'], "business_name": name,
                "legal_name": rec['legal_name'], "trading_name": rec['trading_name'],
                "category": "trades", "city": rec['city'], "state": rec['address_state'],
                "postcode": rec['address_postcode'], "entity_type": rec['entity_type_text'],
                "gst_status": rec['gst_status'], "abn_status_from": rec['abn_status_from'],
                "source": "abn_reference_db"
            }
            matched.append(lead)

    ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
    master = os.path.join(OUT_DIR, "combined_trades_{}.jsonl".format(ts))
    with open(master,'w') as f:
        for lead in matched: f.write(json.dumps(lead)+'\n')
    print("\nCombined: {}  ({} total)\n".format(master, len(matched)))
    for c in CITIES: print("  {}: {}".format(c, by_city.get(c,0)))
    if matched:
        sample = json.dumps(matched[0], indent=2)
        print("\nSample:", sample[:500])
    conn.close()

if __name__ == "__main__": main()
