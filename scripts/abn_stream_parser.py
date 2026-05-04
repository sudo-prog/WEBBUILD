#!/usr/bin/env python3
"""
ABN BulkExtract XML Parser — streams all 20 part files, extracts every ABN record,
and writes to JSONL (one record per line) for downstream enrichment/import.

Fields extracted per record:
  - abn (str)
  - abn_status (ACT/CAN)
  - abn_status_from (date str)
  - record_last_updated (date str)
  - replaced (Y/N)
  - entity_type_ind (PUB/PRV/IND/...)
  - entity_type_text (full description)
  - legal_name (from MainEntity NonIndividualName type=MN, or Individual Given+Family)
  - trading_name (from OtherEntity NonIndividualName type=TRD, if present)
  - address_state ( NSW|VIC|QLD|WA|SA|TAS|NT|ACT|AAT )
  - address_postcode (str)
  - asic_number (str)
  - asic_number_type (str)
  - gst_status (ACT/CAN/NON)
  - gst_status_from (date str)

Output: /home/thinkpad/data/abn/processed/abn_records_<part>.jsonl
"""

import json
import re
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

# Faster chunked reading without full DOM load
def stream_abn_records(xml_path):
    """Yield raw <ABR>...</ABR> strings from a large XML file without loading full DOM."""
    with open(xml_path, 'r', encoding='utf-8', errors='replace') as f:
        buffer = ''
        depth = 0
        in_abr = False
        for line in f:
            i = 0
            while i < len(line):
                if not in_abr:
                    idx = line.find('<ABR', i)
                    if idx == -1:
                        break
                    in_abr = True
                    depth = 1
                    start = idx
                    buffer = line[idx:]
                    i = idx + 1
                else:
                    buffer += line[i]
                    # Track simple tag depth increment/decrement
                    for j, ch in enumerate(line[i:]):
                        if ch == '<' and (i+j+1) < len(line) and line[i+j+1] != '/':
                            depth += 1
                        elif ch == '<' and (i+j+1) < len(line) and line[i+j+1] == '/':
                            depth -= 1
                            if depth == 0:
                                # End of this ABR
                                yield buffer
                                in_abr = False
                                buffer = ''
                                i += j
                                break
                    i = len(line)  # consume rest of line after finding closing tag or partial
        if buffer.strip():
            yield buffer

def parse_abr_record(abr_xml):
    """Parse a single <ABR> XML fragment into a clean dict."""
    rec = {}
    
    # Helper: extract text of first matching sub-element
    def extract(tag, parent=None):
        if parent is None:
            parent = abr_xml
        m = re.search(rf'<{tag}[^>]*>([^<]*)</{tag}>', parent)
        return m.group(1).strip() if m else None

    # ABN tag with attributes
    m = re.search(r'<ABN[^>]*status="([^"]+)"[^>]*ABNStatusFromDate="([^"]+)"[^>]*>([^<]+)</ABN>', abr_xml)
    if m:
        rec['abn_status'] = m.group(1)
        rec['abn_status_from'] = m.group(2)
        rec['abn'] = m.group(3)
    else:
        return None  # malformed

    # Attributes on <ABR>
    rec['record_last_updated'] = re.search(r'recordLastUpdatedDate="([^"]+)"', abr_xml).group(1) if re.search(r'recordLastUpdatedDate="([^"]+)"', abr_xml) else None
    rec['replaced'] = re.search(r'replaced="([^"]+)"', abr_xml).group(1) if re.search(r'replaced="([^"]+)"', abr_xml) else None

    # Entity type
    rec['entity_type_ind'] = extract('EntityTypeInd')
    rec['entity_type_text'] = extract('EntityTypeText')

    # Names and address — two structures: MainEntity or LegalEntity
    # LegalEntity (individuals)
    if '<LegalEntity>' in abr_xml:
        legal = re.search(r'<LegalEntity>(.*?)</LegalEntity>', abr_xml, re.DOTALL).group(1)
        # IndividualName: may have multiple GivenName + FamilyName
        given_names = re.findall(r'<GivenName>([^<]+)</GivenName>', legal)
        family = extract('FamilyName', legal)
        rec['legal_name'] = ' '.join(given_names + ([family] if family else [])) or None
        rec['trading_name'] = None  # individuals usually no separate trading name
    else:
        # MainEntity (non-individuals)
        main = re.search(r'<MainEntity>(.*?)</MainEntity>', abr_xml, re.DOTALL).group(1)
        legal_name = extract('NonIndividualNameText', main)
        rec['legal_name'] = legal_name
        # Trading name from OtherEntity?
        other_match = re.search(r'<OtherEntity>(.*?)</OtherEntity>', abr_xml, re.DOTALL)
        if other_match:
            trading_name = re.search(r'<NonIndividualNameText>([^<]+)</NonIndividualNameText>', other_match.group(1))
            rec['trading_name'] = trading_name.group(1) if trading_name else None
        else:
            rec['trading_name'] = None

    # Address: always BusinessAddress/AddressDetails/{State,Postcode}
    addr_match = re.search(r'<BusinessAddress>(.*?)</BusinessAddress>', abr_xml, re.DOTALL)
    if addr_match:
        addr = addr_match.group(1)
        rec['address_state'] = extract('State', addr)
        rec['address_postcode'] = extract('Postcode', addr)

    # ASIC
    asic = re.search(r'<ASICNumber[^>]*ASICNumberType="([^"]+)"[^>]*>([^<]+)</ASICNumber>', abr_xml)
    if asic:
        rec['asic_number'] = asic.group(2)
        rec['asic_number_type'] = asic.group(1)

    # GST
    gst = re.search(r'<GST[^>]*status="([^"]+)"[^>]*GSTStatusFromDate="([^"]+)"', abr_xml)
    if gst:
        rec['gst_status'] = gst.group(1)
        rec['gst_status_from'] = gst.group(2)

    return rec

def main():
    zip_path = Path("/home/thinkpad/data/abn/public_split_1_10.zip")
    out_dir = Path("/home/thinkpad/data/abn/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    import zipfile
    z = zipfile.ZipFile(zip_path)
    xml_files = [n for n in z.namelist() if n.endswith('.xml')]
    print(f"Found {len(xml_files)} XML files in {zip_path.name}")
    
    for xml_name in xml_files:
        part_id = xml_name.split('.')[0].split('_')[-1]  # Public01 -> 01
        out_path = out_dir / f"abn_part{part_id}.jsonl"
        print(f"\nProcessing {xml_name} → {out_path.name}...")
        
        count = 0
        with z.open(xml_name) as fh:
            # Decode line by line
            buffer = ''
            for raw_line in fh:
                line = raw_line.decode('utf-8', errors='replace')
                # Quick check: does line contain <ABR start?
                if '<ABR' not in line:
                    continue
                # Use streaming parser on the line to extract complete <ABR> blocks
                # Since the file is one continuous line often, we need a streaming stateful parser
                # Let's re-implement: read entire file but iterate with XML pull parser
                pass
        
        # Better: use iterparse on the whole file (still memory efficient)
        # Fallback: just extract all records via regex since it's large but doable in chunks
        # Reset: read full file content
        content = z.read(xml_name).decode('utf-8', errors='replace')
        abrs = re.findall(r'<ABR[^>]*>.*?</ABR>', content, re.DOTALL)
        print(f"  Found {len(abrs):,} records")
        
        with open(out_path, 'w', encoding='utf-8') as out:
            for abr in abrs:
                rec = parse_abr_record(abr)
                if rec:
                    out.write(json.dumps(rec, ensure_ascii=False) + '\n')
                    count += 1
        print(f"  Wrote {count:,} records to {out_path}")

if __name__ == '__main__':
    main()
