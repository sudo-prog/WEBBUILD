#!/usr/bin/env python3
import socket, json, sys
from pathlib import Path

SAMPLE_PATH = Path("/home/thinkpad/data/abn/leads/trades_part01.jsonl")
OUT = Path("/home/thinkpad/data/abn/website_audit_sample.jsonl")

def domain_variants(name):
    clean = name.lower()
    for suffix in [' pty ltd', ' pty limited', ' ltd', ' limited', ' co', ' co.']:
        clean = clean.replace(suffix, '')
    clean = clean.strip()
    words = clean.split()
    if not words:
        return []
    variants = set()
    cat = ''.join(words)
    hyp = '-'.join(words)
    initials = ''.join(w[0] for w in words) if len(words) > 1 else ''
    base_variants = [cat, hyp] + ([initials] if initials else [])
    for base in base_variants:
        for tld in ['com.au', 'com', 'net.au', 'net', 'org.au', 'org']:
            variants.add(f"{base}.{tld}")
        # without TLD just base (for checking naked domain? we need TLD)
    return list(variants)

def has_domain(domain):
    try:
        socket.getaddrinfo(domain, None, socket.AF_INET)
        return True
    except socket.gaierror:
        return False

sample = []
with SAMPLE_PATH.open() as f:
    for i, line in enumerate(f):
        if i >= 200:
            break
        r = json.loads(line)
        sample.append(r)

print(f"Checking {len(sample)} businesses for domain presence...")
results = []
for rec in sample:
    name = rec.get('legal_name') or rec.get('trading_name') or ''
    abn = rec.get('abn')
    found = False
    found_domain = None
    for dom in domain_variants(name):
        try:
            if has_domain(dom):
                found = True
                found_domain = dom
                break
        except:
            continue
    results.append({
        'abn': abn,
        'business_name': name,
        'has_website_domain': found,
        'detected_domain': found_domain,
    })
    print(f"  {abn} | {(name[:30]):30} | {'DOMAIN' if found else 'none':8}")

with OUT.open('w') as f:
    for r in results:
        f.write(json.dumps(r) + '\n')
print(f"\nWrote {len(results)} to {OUT}")
yes = sum(1 for r in results if r['has_website_domain'])
print(f"Websites detected: {yes}/{len(results)} ({yes/len(results)*100:.1f}%)")
