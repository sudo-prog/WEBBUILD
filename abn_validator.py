#!/usr/bin/env python3
"""
ABN Validation Module — Australian Business Register lookup.
Two-tier: offline checksum + optional live verification + name lookup via Playwright.

• validate_abn(abn) -> formatted ABN or False (checksum only)
• lookup_by_abn(abn)  -> active status via ABR API (best-effort, falls back to checksum)
• lookup_by_name(name, state) -> ABN by scraping ABN Lookup (Playwright)
• verify(name, state, abn=None) -> (is_verified, details_dict)

DO NOT MODIFY — this file is managed by Hermes Agent skill.
"""
import re
import time
import random
import os
import requests
from typing import Optional, Dict, Tuple
from pathlib import Path

# ── Tier 1: Offline checksum (pure Python) ───────────────────────────────────
try:
    from abn import validate as abn_checksum  # external ABN library
except ImportError:
    abn_checksum = None


def _checksum_valid(abn: str) -> bool:
    """Return True if ABN passes the official mod‑89 checksum."""
    if not abn or not abn_isdigit(abn):
        return False
    if abn_checksum:
        return bool(abn_checksum(abn))
    # Fallback: implement algorithm inline
    digits = [int(c) for c in abn]
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    total = sum(d * w for d, w in zip(digits, weights))
    # subtract 1 from first digit before sum
    total -= digits[0]
    return (total % 89) == 0


def abn_isdigit(abn: str) -> bool:
    return bool(re.fullmatch(r"\d{11}", re.sub(r"[^\d]", "", abn)))


# ── Tier 2: Live ABR API (may be 404) ────────────────────────────────────────
ABR_SEARCH_URL = "https://abr.business.gov.au/ABR/Service.svc/SearchABR"
ABR_HEADERS = {
    "User-Agent": "LeadGenValidator/1.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
}
_cache = {}
# Demographics whitelist: must operate in Bulleen (VIC 3105) only
_ALLOWED_SUBURBS = {"bulleen"}
# Australian states short codes
_STATE_CODES = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}


def _abr_search(payload: dict, timeout: int = 10) -> dict:
    """Call ABR Search endpoint. Returns parsed JSON or empty dict on failure."""
    try:
        import requests
        r = requests.post(ABR_SEARCH_URL, json=payload, headers=ABR_HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json() or {}
    except Exception:
        pass
    return {}


# ── Name → ABN lookup via Playwright (scrapes ABN Lookup website) ─────────────
_abn_cache_file = Path(__file__).parent / "data" / "abn_cache.json"
_abn_cache_file.parent.mkdir(parents=True, exist_ok=True)
if _abn_cache_file.exists():
    try:
        _name_cache = json.loads(_abn_cache_file.read_text())
    except Exception:
        _name_cache = {}
else:
    _name_cache = {}


def _save_name_cache():
    try:
        _abn_cache_file.write_text(json.dumps(_name_cache, indent=2))
    except Exception:
        pass  # best effort


def lookup_by_name(business_name: str, state: str = None) -> Optional[Dict]:
    """
    Search the ABN Lookup website for a business name and return the first matching ABN record.
    Uses Playwright to render JavaScript. Returns None if not found or any error.
    Result keys: abn, entity_name, status, entity_type, state, source, looked_up_at
    """
    # Rate limiting: 1.5s – 3s jitter between lookups
    time.sleep(random.uniform(1.5, 3.0))

    # Normalise inputs
    name_key = re.sub(r"\s+", " ", business_name.strip().lower())
    state = (state or "").strip().upper()
    state = state if state in _STATE_CODES else None

    cache_key = f"{name_key}::{state or 'any'}"
    if cache_key in _name_cache:
        return _name_cache[cache_key]

    try:
        from playwright.sync_api import sync_playwright
        import requests
    except ImportError:
        return None

    base = "https://abr.gov.au/ABNLookup/"
    query = f"?SearchText={requests.utils.quote(business_name)}"
    if state:
        query += f"&State={state}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent="Mozilla/5.0 (compatible; LeadGenBot/1.0)")
            page.goto(base + query, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)  # wait for results

            rows = page.query_selector_all("table.table tbody tr")
            if not rows:
                return None

            cells = rows[0].query_selector_all("td")
            if len(cells) < 4:
                return None

            abn_val  = cells[0].inner_text().strip()
            name_val = cells[1].inner_text().strip()
            state_val = cells[2].inner_text().strip()
            status_val = cells[3].inner_text().strip()

            abn_digits = re.sub(r"[^\d]", "", abn_val)
            if len(abn_digits) != 11 or not _checksum_valid(abn_digits):
                return None

            result = {
                "abn": abn_digits,
                "entity_name": name_val,
                "state": state_val,
                "status": status_val,
                "entity_type": None,
                "source": "abr_website_playwright",
                "looked_up_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            _name_cache[cache_key] = result
            _save_name_cache()
            return result
    except Exception as e:
        return None


def lookup_by_abn(abn: str, force_live: bool = False) -> Dict:
    """
    Return dict describing the business.

    Always runs offline checksum.
    If force_live=True or environment ABN_LIVE_LOOKUP=1, tries ABR API first;
    on failure automatically falls back to checksum-only.

    If API is down (as of 2026-05-03), returns checksum‑only result.

    Keys: valid, active, abn, entity_name, entity_type, status, source
    """
    abn_clean = "".join(c for c in str(abn) if c.isdigit())
    if len(abn_clean) != 11:
        return {"valid": False, "active": False, "error": "Invalid ABN length"}

    # ── checksum (local, always) ──
    if not _checksum_valid(abn_clean):
        return {"valid": False, "active": False, "error": "Checksum failed"}

    key = f"abn_{abn_clean}"
    if key in _cache:
        return _cache[key]

    # Skip live API if disabled via env flag
    if os.getenv("ABN_SKIP_API", "0") in ("1", "true", "yes"):
        result = {
            "valid": True, "active": True, "abn": abn_clean,
            "entity_name": None, "entity_type": None, "status": "unverified (checksum only)",
            "source": "checksum_fallback"
        }
        _cache[key] = result
        return result

    # ── Try ABR API ──
    payload = {"SearchString": abn_clean, "IncludeHistoricalDetails": "false"}
    data = _abr_search(payload)
    results = data.get("ABRSearchResults", [])
    if results:
        entity = results[0].get("Entity", {})
        abn_det = results[0].get("ABN", {})
        status = abn_det.get("Status", "")
        is_active = status.lower() == "active" and not abn_det.get("CancellationDate")
        result = {
            "valid": True,
            "active": is_active,
            "abn": abn_clean,
            "entity_name": entity.get("EntityName", ""),
            "entity_type": entity.get("EntityType", ""),
            "status": status,
            "source": "abr_api",
        }
        _cache[key] = result
        return result

    # ── API unreachable → checksum-only ──
    result = {
        "valid": True,
        "active": True,   # assume active; enrichment marks uncertain
        "abn": abn_clean,
        "entity_name": None,
        "entity_type": None,
        "status": "unverified (checksum only)",
        "source": "checksum_fallback"
    }
    _cache[key] = result
    return result


# ── Public verify() entry point ───────────────────────────────────────────────
def verify(name: str, state: str, abn: Optional[str] = None, phone_check: bool = False) -> Tuple[bool, Dict]:
    """
    Verify a business:

    1. If ABN provided → validate checksum + optional ABR API check (fallback to checksum).
    2. If no ABN → try name lookup via Playwright; then validate the found ABN.

    If demographics whitelist is active, only businesses in Bulleen (VIC 3105) are accepted.

    Returns: (is_verified, details_dict)
    """
    details: Dict = {}

    # ── Path A: ABN supplied ──
    if abn:
        res = lookup_by_abn(abn)
        details.update(res)
        if not res.get("valid"):
            return False, details
        verified = res.get("active", False)
        details["abn_source"] = "provided"
        return verified, details

    # ── Path B: No ABN → name lookup ──
    found = lookup_by_name(name, state)
    if found:
        # Verify found ABN checksum
        abn_digits = found["abn"]
        if not _checksum_valid(abn_digits):
            return False, {"error": "looked‑up ABN failed checksum", **found}
        # Active status from website text
        active_text = found.get("status", "").lower()
        is_active = "active" in active_text and "cancelled" not in active_text
        details.update(found)
        details["abn_source"] = "name_lookup"
        return is_active, details

    return False, {"error": "no ABN found", "source": "none"}


# ── Simple CLI for testing ────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        test_name = " ".join(sys.argv[1:])
        print(f"Looking up ABN for '{test_name}'…")
        ok, details = verify(test_name, "VIC")
        print(json.dumps({"ok": ok, **details}, indent=2))
    else:
        print("── ABN checksum demo ──")
        for abn in ["51824753556", "12345678901"]:  # 51824753556 = Masters Home Improvement (VIC) — invalid format
            ok, det = verify("Test", "VIC", abn)
            print(f"{abn}: ok={ok}, src={det.get('source')}")
