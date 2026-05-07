#!/usr/bin/env python3
import json, zipfile
from pathlib import Path
from lxml import etree

ZIP_PATH = "/home/thinkpad/data/abn/public_split_1_10.zip"
OUT_DIR = Path("/home/thinkpad/data/abn/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEEP_ENTITY_TYPES = {'PRV', 'IND', 'FPT', 'PTR', 'OIE'}

CAPITAL_POSTCODES = {
    'Sydney':    list(range(2000, 2235)) + list(range(2250, 2269)) + list(range(2550, 2760)),
    'Melbourne': list(range(3000, 3208)) + list(range(3305, 3978)),
    'Brisbane':  list(range(4000, 4012)) + list(range(4034, 4045)) + list(range(4064, 4158)) + list(range(4500, 4577)) + list(range(4720, 4722)),
    'Perth':     list(range(6000, 6039)) + list(range(6050, 6183)) + list(range(6208, 6210)) + list(range(6503, 6771)),
    'Adelaide':  list(range(5000, 5200)) + list(range(5800, 5963)),
    'Canberra':  list(range(2600, 2619)) + list(range(2900, 2921)),
    'Hobart':    list(range(7000, 7055)),
    'Darwin':    list(range(800, 1000)),  # NT 0800-0999 -> numeric 800-999
}
ALL_CAPITAL_POSTCODES = set()
for v in CAPITAL_POSTCODES.values():
    ALL_CAPITAL_POSTCODES.update(v)

def parse_abr(elem):
    rec = {}
    rec['record_last_updated'] = elem.get('recordLastUpdatedDate')
    rec['replaced'] = elem.get('replaced')
    abn_el = elem.find('ABN')
    if abn_el is None:
        return None
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
    asic = elem.find('ASICNumber')
    if asic is not None:
        rec['asic_number'] = asic.text
        rec['asic_number_type'] = asic.get('ASICNumberType')
    gst = elem.find('GST')
    if gst is not None:
        rec['gst_status'] = gst.get('status')
        rec['gst_status_from'] = gst.get('GSTStatusFromDate')
    return rec

def should_keep(rec):
    if rec.get('abn_status') != 'ACT':
        return False
    if rec.get('entity_type_ind') not in KEEP_ENTITY_TYPES:
        return False
    if not rec.get('address_state'):
        return False
    pc_str = rec.get('address_postcode', '')
    try:
        pc = int(pc_str)
        return pc in ALL_CAPITAL_POSTCODES
    except (ValueError, TypeError):
        return False

def main():
    z = zipfile.ZipFile(ZIP_PATH)
    xml_names = sorted([n for n in z.namelist() if n.endswith('.xml')])
    grand_total = 0
    for xml_name in xml_names:
        part_id = xml_name.split('.')[0].split('_')[-1]
        out_path = OUT_DIR / f"leads_part{part_id}.jsonl"
        print(f"Parsing {xml_name}...")
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
        print(f"  Kept {kept:,} → {out_path.name}")
        grand_total += kept
        context = None
        fh.close()
    print(f"\n✅ Part 1 total kept: {grand_total:,}")

if __name__ == '__main__':
    main()
