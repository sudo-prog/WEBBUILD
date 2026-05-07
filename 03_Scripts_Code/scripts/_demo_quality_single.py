#!/usr/bin/env python3
import sys, json
sys.path.insert(0, '.')

from lead_verifier_v2 import layer1, lookup_abn

# One genuine ABN match from cross-reference
lead = {
    "business_name": "HOT WATER PLUMBING",
    "abn": "11003214451",
    "address_state": "WA",
    "gst_registered": True,
    "entity_type": "Family Partnership",
    "abn_status": "ACT",
    "abn_status_from": "2020-01-15",
}

abn_rec = lookup_abn(lead.get("abn"))
res = layer1(lead, abn_rec)
print(json.dumps(res, indent=2))
