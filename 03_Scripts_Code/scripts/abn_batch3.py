#!/usr/bin/env python3
import json, zipfile
from pathlib import Path
from lxml import etree

ZIP_PATH = "/home/thinkpad/data/abn/public_split_11_20.zip"
OUT_DIR = Path("/home/thinkpad/data/abn/processed")
CAPITAL_POSTCODES = set()
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
for rng in ranges.values():
    for lo, hi in rng:
        CAPITAL_POSTCODES.update(range(lo, hi+1))
KEEP_ENTITY_TYPES = {'PRV','IND','FPT','SMF','PTR','DTT','DIT','FUT','STR','OIE'}

def parse_abr(elem):
    rec = {}
    rec['record_last_updated'] = elem.get('recordLastUpdatedDate')
    rec['replaced'] = elem.get('replaced')
    abn_el = elem.find('ABN')
    if abn_el is None: return None
    rec['abn'] = abn_el.text.strip()
    rec['abn_status'] = abn_el.get('status')
    rec['abn_status_from'] = abn_el.get('ABNStatusFromDate')
    et = elem.find('EntityType')
    if et is not None:
        rec['entity_type_ind'] = et.findtext('EntityTypeInd')
        rec['entity_type_text'] = et.findtext('EntityTypeText')
    else:
        rec['entity_type_ind'] = None
        rec['entity_type_text'] = None
    main = elem.find('MainEntity')
    if main is not None:
        mn = main.find('NonIndividualName')
        if mn is not None:
            rec['legal_name'] = mn.findtext('NonIndividualNameText')
        other = elem.find('OtherEntity')
        if other is not None:
            trd = other.find('NonIndividualName')
            if trd is not None:
                rec['trading_name'] = trd.findtext('NonIndividualNameText')
        else:
            rec['trading_name'] = None
    else:
        legal = elem.find('LegalEntity')
        if legal is not None:
            indiv = legal.find('IndividualName')
            if indiv is not None:
                given = [g.text for g in indiv.findall('GivenName') if g.text]
                family = indiv.findtext('FamilyName')
                rec['legal_name'] = ' '.join(given + ([family] if family else []))
            rec['trading_name'] = None
        else:
            return None
    state_el = elem.find('.//State')
    rec['address_state'] = state_el.text if state_el is not None else None
    post_el = elem.find('.//Postcode')
    rec['address_postcode'] = post_el.text if post_el is not None else None
    return rec

def should_keep(rec):
    if rec.get('abn_status') != 'ACT': return False
    if rec.get('entity_type_ind') not in KEEP_ENTITY_TYPES: return False
    if not rec.get('address_state'): return False
    try:
        return int(rec.get('address_postcode','')) in CAPITAL_POSTCODES
    except:
        return False

z = zipfile.ZipFile(ZIP_PATH)
xml_files = sorted([n for n in z.namelist() if n.endswith('.xml')])
targets = xml_files[0:5]   # Public11-Public15
print(f"Batch 3: {[t.split('/')[-1] for t in targets]}")
total = 0
for xml_name in targets:
    part_id = xml_name.split('.')[0].split('_')[-1]
    out_path = OUT_DIR / f"leads_part{part_id}.jsonl"
    fh = z.open(xml_name)
    context = etree.iterparse(fh, events=('end',), tag='ABR', recover=True)
    kept = 0
    with open(out_path, 'w', encoding='utf-8') as out:
        for event, elem in context:
            rec = parse_abr(elem)
            if rec and should_keep(rec):
                out.write(json.dumps(rec, ensure_ascii=False) + '\n')
                kept += 1
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]
    print(f"  part{part_id}: {kept:,}")
    total += kept
print(f"\n✅ Batch 3 total: {total:,}")
