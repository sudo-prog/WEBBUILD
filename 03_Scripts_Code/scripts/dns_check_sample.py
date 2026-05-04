#!/usr/bin/env python3
import socket, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

SAMPLE_PATH = Path("/home/thinkpad/data/abn/leads/trades_part01.jsonl")
SAMPLE_SIZE = 50

def domain_variants(name):
    clean = name.lower()
    for s in [' pty ltd',' pty limited',' ltd',' limited',' co',' co.']:
        clean = clean.replace(s,'')
    clean = clean.strip()
    words = clean.split()
    if not words: return []
    variants = set()
    cat = ''.join(words)
    hyp = '-'.join(words)
    initials = ''.join(w[0] for w in words) if len(words)>1 else ''
    for base in [cat, hyp] + ([initials] if initials else []):
        for tld in ['com.au','com','net.au','net','org.au','org']:
            variants.add(f"{base}.{tld}")
    return list(variants)

def check(domain):
    try:
        socket.getaddrinfo(domain, None, socket.AF_INET)
        return domain
    except socket.gaierror:
        return None

sample = []
with SAMPLE_PATH.open() as f:
    for i,line in enumerate(f):
        if i>=SAMPLE_SIZE: break
        sample.append(json.loads(line))

print(f"Checking {len(sample)} businesses with threaded DNS...")
results = []
with ThreadPoolExecutor(max_workers=50) as pool:
    for rec in sample:
        name = rec.get('legal_name') or rec.get('trading_name') or ''
        abn = rec.get('abn')
        variants = domain_variants(name)
        futures = {pool.submit(check, d): d for d in variants}
        found = None
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                found = res
                break
        results.append({'abn':abn,'name':name,'has_website': found is not None,'domain':found})
        print(f"  {abn} | {(name[:30]):30} | {'FOUND '+found if found else 'none'}")

yes = sum(1 for r in results if r['has_website'])
print(f"\n{yes}/{len(results)} have detectable domains ({yes/len(results)*100:.0f}%)")
