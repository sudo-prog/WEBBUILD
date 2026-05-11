#!/usr/bin/env python3
import json, zipfile, re
from pathlib import Path
from lxml import etree

ZIP_PATH = "/home/thinkpad/data/abn/public_split_1_10.zip"
OUT_DIR = Path("/home/thinkpad/data/abn/processed")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def parse_abr(elem):
    rec = {}
    rec['record_last_updated'] = elem.get('recordLastUpdatedDate')
    rec['replaced'] = elem.get('replaced')
    abn_el = elem.find('ABN')
    if abn_el is not None:
        rec['abn'] = abn_el.text
        rec['abn_status'] = abn_el.get('status')
        rec['abn_status_from'] = abn_el.get('ABNStatusFromDate')
    et = elem.find('EntityType')
    if et is not None:
        rec['entity_type_ind'] = et.findtext('EntityTypeInd')
        rec['entity_type_text'] = et.findtext('EntityTypeText')
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
    # Fix: use .// to search descendants for State/Postcode
    state_el = elem.find('.//State')
    if state_el is not None:
        rec['address_state'] = state_el.text
    post_el = elem.find('.//Postcode')
    if post_el is not None:
        rec['address_postcode'] = post_el.text
    asic = elem.find('ASICNumber')
    if asic is not None:
        rec['asic_number'] = asic.text
        rec['asic_number_type'] = asic.get('ASICNumberType')
    gst = elem.find('GST')
    if gst is not None:
        rec['gst_status'] = gst.get('status')
        rec['gst_status_from'] = gst.get('GSTStatusFromDate')
    return rec

z = zipfile.ZipFile(ZIP_PATH)
xml_name = "20260429_Public01.xml"
out_path = OUT_DIR / "abn_part01_fixed.jsonl"
fh = z.open(xml_name)
context = etree.iterparse(fh, events=('end',), tag='ABR', recover=True)
written = 0
with open(out_path, 'w', encoding='utf-8') as out:
    for event, elem in context:
        rec = parse_abr(elem)
        if rec:
            out.write(json.dumps(rec, ensure_ascii=False) + '\n')
            written += 1
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]
print(f"Wrote {written:,} records → {out_path}")
