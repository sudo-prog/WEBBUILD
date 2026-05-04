#!/usr/bin/env python3
"""
Lead Quality System v2.0 — 5-Layer Verification Pipeline
Based on user-supplied specification PDF (Lead Quality v2).
Schema matches our SQLite ABN reference DB (from bulk extract).
"""

import json, sqlite3, re, sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

DB_PATH   = "/home/thinkpad/data/abn/abn_reference.db"

MIN_LAYER_SCORE = {"L1":15, "L2":5, "L3":20, "L4":2, "L5":8}
TIER_CUTS       = {"PREMIUM":80, "HIGH":65, "MEDIUM":50, "LOW":0}

HARD_KILL_ENTITY = {"non-profit", "government", "superannuation", "public benevolent", "charity"}
AUTO_KILL_KEYWORDS = ["part time", "weekends only", "retired", "hobby", "one man band", "odd jobs"]

# ─── DB ────────────────────────────────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def lookup_abn(abn: str) -> Optional[Dict]:
    conn = get_conn()
    row = conn.cursor().execute("SELECT * FROM abn_records WHERE abn=?", (abn,)).fetchone()
    conn.close()
    return dict(row) if row else None

# ─── LAYER 1 — ABN ─────────────────────────────────────────────────────────────
def layer1(lead: Dict, abn: Optional[Dict]) -> Dict:
    r = {"passed":False, "score":0, "reasons":[], "flags":{}}
    if not abn:
        r["reasons"].append("ABN not found in reference DB")
        return r

    status = (abn.get("abn_status") or "").upper()
    entity = (abn.get("entity_type_text") or "").lower()
    gst     = (abn.get("gst_status") or "").upper() == "ACT"
    state   = abn.get("address_state")
    abn_date= abn.get("abn_status_from")

    if status not in ("ACT",):
        r["reasons"].append(f"ABN status={status}")
        return r

    if any(k in entity for k in HARD_KILL_ENTITY):
        r["reasons"].append(f"Entity={entity}")
        return r

    score = 15
    if gst:
        score += 5
        r["flags"]["gst_registered"] = True

    # ABN age check removed — no longer a hard kill criterion
    if abn_date:
        try:
            reg  = datetime.strptime(abn_date, "%Y-%m-%d")
            days = (datetime.now() - reg).days
            # Still flag for visibility but don't kill
            if days < 365:
                r["flags"]["abn_age"] = f"{days}d"
        except Exception:
            pass

    r.update({"passed":True, "score":score})
    return r

# ─── LAYER 2 — ACTIVITY ────────────────────────────────────────────────────────
def layer2(enr: Dict) -> Dict:
    r = {"passed":False, "score":0, "reasons":[]}
    score = 0
    rev_cnt  = enr.get("google_reviews_count",0)
    last_rev = enr.get("google_last_review")

    if rev_cnt == 0:
        r["reasons"].append("Zero reviews")
        # No hard kill — continue to evaluate other signals (phone, FB, owner replies)
    else:
        if last_rev:
            days = (datetime.now() - last_rev).days if isinstance(last_rev, datetime) else 999
            if   days <= 30: score += 15
            elif days <= 90: score += 10
            elif days <= 180:score += 5
            elif days <= 365:score += 2
            else:
                r["reasons"].append(f"Reviews old={days}d")
        else:
            r["reasons"].append("No review date")

    if enr.get("google_owner_replies"): score += 10

    fb_post = enr.get("facebook_last_post")
    if fb_post:
        days = (datetime.now() - fb_post).days if isinstance(fb_post, datetime) else 999
        if   days <= 30: score += 8
        elif days <= 60: score += 5
        elif days <= 90: score += 2

    if enr.get("phone_active"):       score += 5
    else:
        r["reasons"].append("Phone inactive")
        return r

    if enr.get("phone_matches_abn"): score += 5

    r["score"]  = score
    r["passed"] = score >= MIN_LAYER_SCORE["L2"]
    if not r["passed"]:
        r["reasons"].append("Activity={} < {}".format(score, MIN_LAYER_SCORE["L2"]))
    return r

# ─── LAYER 3 — REVENUE ─────────────────────────────────────────────────────────
def layer3(gst_registered: bool, enr: Dict) -> Dict:
    r = {"passed":False, "score":0, "reasons":[]}
    score = 0
    if gst_registered: score += 25

    rev_cnt = enr.get("google_reviews_count",0)
    if   rev_cnt > 50:  score += 15
    elif rev_cnt > 20:  score += 10
    elif rev_cnt > 9:   score += 5

    if enr.get("yp_featured"):        score += 8
    elif enr.get("yp_listing_type")=="basic": score += 2

    # Accept both flat keys (from direct scraper) and nested (from enrich wrapper)
    yp_desc = enr.get("yp_description") or (enr.get("yellow_pages") or {}).get("yp_description") or ""
    fb_about = enr.get("facebook_about") or (enr.get("facebook") or {}).get("facebook_about") or ""
    blob = " ".join([enr.get("google_reviews_text",""), yp_desc, fb_about]).lower()

    if "team" in blob or "the guys" in blob:           score += 10
    if "scaffolding" in blob or "commercial" in blob:  score += 8
    if any(p in blob for p in ["$150","$200","$250","$300"]): score += 5
    if "24 hour" in blob or "emergency" in blob:       score += 5

    if any(k in blob for k in AUTO_KILL_KEYWORDS):
        r["reasons"].append("Auto-kill keyword")
        return r

    r["score"]  = score
    r["passed"] = score >= MIN_LAYER_SCORE["L3"]
    if not r["passed"]:
        r["reasons"].append("Revenue={} < {}".format(score, MIN_LAYER_SCORE["L3"]))
    return r

# ─── LAYER 4 — CROSS-REFERENCE ─────────────────────────────────────────────────
def layer4(sources: Dict) -> Dict:
    r = {"passed":False, "score":0, "sources_found":[], "reasons":[]}
    present = [k for k,v in sources.items() if v and v.get("found")]
    r["sources_found"] = present
    count = len(present)

    if count < 2:
        r["reasons"].append("Sources={} < 2".format(count))
        return r

    norm  = lambda s: re.sub(r"[^a-z0-9]","", (s or "").lower())
    keys  = ["business_name", "phone", "suburb"]
    matches = sum(1 for k in keys if len({norm(sources[s].get(k)) for s in present}) == 1)

    if matches >= 2:
        r["score"]  = 20 if count >= 3 else 12
        r["passed"] = True
    else:
        r["score"]  = 6
        r["passed"] = True
        r["reasons"].append("Minor discrepancies")
    return r

# ─── LAYER 5 — WEBSITE ABSENCE ─────────────────────────────────────────────────
def layer5(lead: Dict, enr: Dict) -> Dict:
    r = {"verified":False, "score":0, "checks":[], "reasons":[]}

    # Pass if any website URL is directly found
    if enr.get("gmaps_website") or enr.get("yp_url") or enr.get("fb_url"):
        r["score"], r["verified"] = 10, True
        r["checks"] = [k for k,v in [("gmaps_website", enr.get("gmaps_website")), ("yp_url", enr.get("yp_url")), ("fb_url", enr.get("fb_url"))] if v]
        return r

    # Fallback: require all 3 search queries to be clean (no website found anywhere)
    clean = sum(1 for k in ["search1_clean","search2_clean","search3_clean"] if enr.get(k))
    if clean == 3:
        r["score"], r["verified"] = 10, True
    elif clean >= 2:
        r["score"], r["verified"] =  6, True
    else:
        r["reasons"].append("Only {}/3 website searches clean".format(clean))
    r["checks"] = [k for k in ["search1_clean","search2_clean","search3_clean"] if enr.get(k)]
    return r

# ─── ORCHESTRATE ───────────────────────────────────────────────────────────────
def verify(lead: Dict, enriched: Dict) -> Dict:
    rec = {
        "abn": lead.get("abn"), "business_name": lead.get("business_name"),
        "category": lead.get("category"), "city": lead.get("city"), "state": lead.get("state"),
        "quality_score":0, "priority":"DISCARD", "kill_reasons":[],
        "layer_results":{}, "quality_metadata":{}
    }

    abn_rec = lookup_abn(lead["abn"]) if lead.get("abn") else None
    L1 = layer1(lead, abn_rec)
    rec["layer_results"]["L1"] = L1["passed"]
    rec["abn_active"]         = L1["passed"]
    rec["gst_registered"]     = L1["flags"].get("gst_registered", False)
    rec["abn_status"]         = abn_rec.get("abn_status") if abn_rec else None
    if not L1["passed"]:
        rec["kill_reasons"].extend(L1["reasons"])
        # continue — other layers may still contribute to total

    L2 = layer2(enriched)
    rec["layer_results"]["L2"] = L2["passed"]
    rec["activity_score"]      = L2["score"]
    if not L2["passed"]:
        rec["kill_reasons"].extend(L2["reasons"])
        # continue — do not early return

    # L3 needs gst_registered from L1
    enriched["gst_registered_flag"] = rec.get("gst_registered", False)
    L3 = layer3(enriched["gst_registered_flag"], enriched)
    rec["layer_results"]["L3"] = L3["passed"]
    rec["revenue_proxy"]       = L3["score"]
    if not L3["passed"]:
        rec["kill_reasons"].extend(L3["reasons"])
        # continue

    sources = {
        "google_maps": enriched.get("google_maps",{}),
        "yellow_pages":enriched.get("yellow_pages",{}),
        "facebook":    enriched.get("facebook",{})
    }
    L4 = layer4(sources)
    rec["layer_results"]["L4"] = L4["passed"]
    rec["sources_found"]       = L4["sources_found"]
    rec["phone_consistent"]    = all(s.get("phone")==sources["google_maps"].get("phone")
                                    for s in sources.values() if s and s.get("phone"))
    if not L4["passed"]:
        rec["kill_reasons"].extend(L4["reasons"])
        # continue

    L5 = layer5(lead, enriched)
    rec["layer_results"]["L5"] = L5["verified"]
    rec["website_checks"]      = L5["checks"]
    if not L5["verified"]:
        rec["kill_reasons"].extend(L5["reasons"])
        # continue

    total = (L1["score"] + min(L2["score"],35) + min(L3["score"],40) + L4["score"] + L5["score"])
    bonus = 0
    if enriched.get("emergency_service"):  bonus += 5
    if enriched.get("quote_based"):        bonus += 3
    if enriched.get("featured_listing"):   bonus += 2
    total += bonus
    rec["quality_score"] = total

    rec["quality_metadata"] = {
        "activity_score":    L2["score"],
        "revenue_proxy":     L3["score"],
        "sources_found":     L4["sources_found"],
        "phone_consistent":  rec["phone_consistent"],
        "layer_results":     rec["layer_results"],
        "gst_registered":    rec["gst_registered"],
    }
    rec["needs_review"] = False

    if   total >= TIER_CUTS["PREMIUM"]: rec["priority"] = "PREMIUM"
    elif total >= TIER_CUTS["HIGH"]:    rec["priority"] = "HIGH"
    elif total >= TIER_CUTS["MEDIUM"]:  rec["priority"] = "MEDIUM"
    elif total >= TIER_CUTS["LOW"]:     rec["priority"] = "LOW"
    else:                               rec["priority"] = "DISCARD"

    return rec

# ─── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage:  python3 lead_verifier_v2.py --input enriched.jsonl --output verified.jsonl [--limit N]")
        sys.exit(1)

    inp, out, limit = None, None, None
    args = sys.argv[1:]
    for i, a in enumerate(args):
        if a == "--input":    inp    = args[i+1]
        if a == "--output":   out    = args[i+1]
        if a == "--limit":    limit  = int(args[i+1])

    if not inp or not out:
        print("Missing --input or --output"); sys.exit(1)

    passed, total = 0, 0
    with open(inp) as fin, open(out, "w") as fout:
        for line in fin:
            if limit and total >= limit: break
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Extract enriched dict from _enriched field; remainder is the lead
            enriched = record.get("_enriched", {})
            # Remove _enriched from lead fields
            lead = {k: v for k, v in record.items() if k != "_enriched"}

            rec  = verify(lead, enriched)
            fout.write(json.dumps(rec) + "\n")
            total += 1
            stat  = "✓" if rec["priority"] != "DISCARD" else "✗"
            print("{} {:<35}  score={:3}  {:8}".format(
                stat, lead.get('trading_name') or lead.get('business_name','?')[:35],
                rec['quality_score'], rec['priority']))
            if rec["priority"] != "DISCARD": passed += 1

    print("Processed {} → {} verified ({:.1f}%)".format(total, passed, passed/total*100 if total else 0))
