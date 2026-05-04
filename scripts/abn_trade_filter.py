#!/usr/bin/env python3
import json
from pathlib import Path
from collections import Counter

IN_DIR = Path("/home/thinkpad/data/abn/processed")
OUT_DIR = Path("/home/thinkpad/data/abn/leads")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEYWORD_TO_CATEGORY = {'plumber': 'plumbing', 'plumbing': 'plumbing', 'drain': 'plumbing', 'blocked drain': 'plumbing', 'leak detection': 'plumbing', 'hot water': 'plumbing', 'electrician': 'electrician', 'electrical': 'electrician', 'sparky': 'electrician', 'wiring': 'electrician', 'switchboard': 'electrician', 'lighting': 'electrician', 'builder': 'builder', 'carpenter': 'builder', 'construction': 'builder', 'renovation': 'builder', 'extensions': 'builder', 'home builder': 'builder', 'painter': 'painter', 'painting': 'painter', 'decorator': 'painter', 'wallpaper': 'painter', 'stripping': 'painter', 'roofer': 'roofer', 'roofing': 'roofer', 'tiling': 'roofer', 'guttering': 'roofer', 'downpipes': 'roofer', 'metal roof': 'roofer', 'air conditioning': 'air conditioning', 'hvac': 'air conditioning', 'ducted': 'air conditioning', 'split system': 'air conditioning', 'cooling': 'air conditioning', 'evaporative': 'air conditioning', 'kitchen': 'kitchen', 'bathroom': 'kitchen', 'joinery': 'kitchen', 'cabinet': 'kitchen', 'benchtop': 'kitchen', 'outdoor kitchen': 'kitchen', 'flooring': 'flooring', 'tiles': 'flooring', 'laminate': 'flooring', 'carpet': 'flooring', 'polished concrete': 'flooring', 'timber floor': 'flooring', 'solar': 'solar', 'solar panel': 'solar', 'photovoltaic': 'solar', 'pv': 'solar', 'solar power': 'solar', 'battery': 'solar', 'pest control': 'pest control', 'termite': 'pest control', 'exterminator': 'pest control', 'rodent': 'pest control', 'fumigation': 'pest control', 'gardener': 'gardener', 'landscaper': 'gardener', 'tree': 'gardener', 'lawn mowing': 'gardener', 'hedge trimming': 'gardener', 'garden design': 'gardener', 'mechanic': 'mechanic', 'auto repair': 'mechanic', 'vehicle': 'mechanic', 'car service': 'mechanic', 'mechanical': 'mechanic', 'tyre': 'mechanic'}

def detect_category(name, trading):
    text = f"{name} {trading}".lower()
    for kw, cat in KEYWORD_TO_CATEGORY.items():
        if kw in text:
            return cat
    return None

all_files = sorted(IN_DIR.glob("leads_part*.jsonl"))
total_in = 0
total_out = 0
cat_counts = Counter()
state_city_counts = {}  # state -> city (derived from postcode) -> count

# Simple postcode-to-city mapping for capital cities (major suburbs)
POSTCODE_CITY = {}
# Sydney
for pc in list(range(2000,2235)) + list(range(2250,2269)) + list(range(2550,2760)):
    POSTCODE_CITY[pc] = 'Sydney'
# Melbourne
for pc in list(range(3000,3208)) + list(range(3305,3978)):
    POSTCODE_CITY[pc] = 'Melbourne'
# Brisbane
for pc in list(range(4000,4012)) + list(range(4034,4045)) + list(range(4064,4158)) + list(range(4500,4577)) + list(range(4720,4722)):
    POSTCODE_CITY[pc] = 'Brisbane'
# Perth
for pc in list(range(6000,6039)) + list(range(6050,6183)) + list(range(6208,6210)) + list(range(6503,6771)):
    POSTCODE_CITY[pc] = 'Perth'
# Adelaide
for pc in list(range(5000,5200)) + list(range(5800,5963)):
    POSTCODE_CITY[pc] = 'Adelaide'
# Canberra
for pc in list(range(2600,2619)) + list(range(2900,2921)):
    POSTCODE_CITY[pc] = 'Canberra'
# Hobart
for pc in list(range(7000,7055)):
    POSTCODE_CITY[pc] = 'Hobart'
# Darwin
for pc in list(range(800,1000)):
    POSTCODE_CITY[pc] = 'Darwin'

for f in all_files:
    out_f = OUT_DIR / f.name.replace("leads_", "trades_")
    with f.open() as fh, open(out_f, 'w', encoding='utf-8') as out:
        for line in fh:
            total_in += 1
            r = json.loads(line)
            legal = (r.get('legal_name') or '')
            trading = (r.get('trading_name') or '')
            category = detect_category(legal, trading)
            if category:
                total_out += 1
                # Derive city from postcode
                pc_str = r.get('address_postcode','')
                try:
                    pc = int(pc_str)
                    city = POSTCODE_CITY.get(pc, 'Unknown')
                except:
                    city = 'Unknown'
                state = r.get('address_state','UNK')
                # Build lead object matching old schema
                lead = {
                    'abn': r.get('abn'),
                    'business_name': trading if trading else legal,
                    'legal_name': legal,
                    'trading_name': trading,
                    'category': category,
                    'state': state,
                    'city': city,
                    'postcode': pc_str,
                    'address_full': None,  # not available from ABN
                    'phone': None,
                    'email': None,
                    'website': None,
                    'gst_status': r.get('gst_status'),
                    'entity_type': r.get('entity_type_ind'),
                    'abn_status': r.get('abn_status'),
                    'source': 'abn_bulk_extract',
                    'extracted_at': r.get('record_last_updated')
                }
                out.write(json.dumps(lead, ensure_ascii=False) + '\n')
                cat_counts[category] += 1
                state_city_counts.setdefault(state, {}).setdefault(city, 0)
                state_city_counts[state][city] += 1

print(f"Total ABN records scanned: {total_in:,}")
print(f"Trade-filtered leads: {total_out:,}")
print("\nLeads by category:")
for cat, cnt in cat_counts.most_common():
    print(f"  {cat}: {cnt:,}")
print("\nLeads by state/city:")
for st, cities in state_city_counts.items():
    total_st = sum(cities.values())
    print(f"  {st} ({total_st}): " + ", ".join(f"{city}={count}" for city,count in sorted(cities.items())))
