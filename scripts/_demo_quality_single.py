#!/usr/bin/env python3
import sys, json
sys.path.insert(0, '.')

from lead_verifier_v2 import layer1

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

res = layer1(lead)
print(json.dumps(res, indent=2))
