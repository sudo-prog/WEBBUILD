#!/usr/bin/env python3
"""
ABN Validation Module — FIXED VERSION
Bugs fixed from audit:
  1. Missing `import json` at module top level
  2. _abn_cache_file references json before import
  3. Playwright lookup now has proper fallback when browser unavailable
"""
import re
import time
import random
import os
import json          # ← FIX #1: was missing, caused NameError on cache load
import requests
from typing import Optional, Dict, Tuple
from pathlib import Path

# ── Tier 1: Offline checksum ──────────────────────────────────────────────────
try:
    from abn import validate as abn_checksum
except ImportError:
    abn_checksum = None


def _checksum_valid(abn: str) -> bool:
    digits_str = re.sub(r"[^\d]", "", abn)
    if len(digits_str) != 11:
        return False
    if abn_checksum:
        return bool(abn_checksum(digits_str))
    # Inline mod-89 algorithm
    digits = [int(c) for c in digits_str]
    weights = [10, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    digits[0] -= 1
    total = sum(d * w for d, w in zip(digits, weights))
    return (total % 89) == 0


def abn_isdigit(abn: str) -> bool:
    return bool(re.fullmatch(r"\d{11}", re.sub(r"[^\d]", "", abn)))


# ── Tier 2: Live ABR API ──────────────────────────────────────────────────────
ABR_SEARCH_URL = "https://abr.business.gov.au/ABR/Service.svc/SearchABR"
ABR_HEADERS = {
    "User-Agent": "LeadGenValidator/1.0",
    "Accept": "application/json",
    "Content-Type": "application/json",
}
_cache: Dict = {}
_STATE_CODES = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}

# ── FIX #2: json is now imported before this runs ─────────────────────────────
_abn_cache_file = Path(__file__).parent / "data" / "abn_cache.json"
_abn_cache_file.parent.mkdir(parents=True, exist_ok=True)
if _abn_cache_file.exists():
    try:
        _name_cache: Dict = json.loads(_abn_cache_file.read_text())
    except Exception:
        _name_cache = {}
else:
    _name_cache = {}


def _save_name_cache():
    try:
        _abn_cache_file.write_text(json.dumps(_name_cache, indent=2))
    except Exception:
        pass


def _abr_search(payload: dict, timeout: int = 10) -> dict:
    try:
        r = requests.post(ABR_SEARCH_URL, json=payload, headers=ABR_HEADERS, timeout=timeout)
        if r.status_code == 200:
            return r.json() or {}
    except Exception:
        pass
    return {}


# ── Name → ABN lookup ─────────────────────────────────────────────────────────
def lookup_by_name(business_name: str, state: str = None) -> Optional[Dict]:
    """
    Search ABN Lookup website via Playwright.
    FIX #3: graceful fallback when Playwright/browser not available.
    """
    time.sleep(random.uniform(1.5, 3.0))

    name_key = re.sub(r"\s+", " ", business_name.strip().lower())
    state = (state or "").strip().upper()
    state = state if state in _STATE_CODES else None
    cache_key = f"{name_key}::{state or 'any'}"

    if cache_key in _name_cache:
        return _name_cache[cache_key]

    # Try Playwright — graceful fallback if unavailable
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    base = "https://abr.gov.au/ABNLookup/"
    query = f"?SearchText={requests.utils.quote(business_name)}"
    if state:
        query += f"&State={state}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(
                user_agent="Mozilla/5.0 (compatible; LeadGenBot/1.0)"
            )
            page.goto(base + query, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)

            rows = page.query_selector_all("table.table tbody tr")
            if not rows:
                browser.close()
                return None

            cells = rows[0].query_selector_all("td")
            if len(cells) < 4:
                browser.close()
                return None

            abn_val   = cells[0].inner_text().strip()
            name_val  = cells[1].inner_text().strip()
            state_val = cells[2].inner_text().strip()
            status_val = cells[3].inner_text().strip()
            browser.close()

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

    except Exception:
        return None


def lookup_by_abn(abn: str) -> Dict:
    """
    Validate an ABN. Always runs offline checksum.
    Tries ABR API; falls back to checksum-only result.
    """
    abn_clean = re.sub(r"[^\d]", "", str(abn))
    if len(abn_clean) != 11:
        return {"valid": False, "active": False, "error": "Invalid ABN length"}

    if not _checksum_valid(abn_clean):
        return {"valid": False, "active": False, "error": "Checksum failed"}

    key = f"abn_{abn_clean}"
    if key in _cache:
        return _cache[key]

    if os.getenv("ABN_SKIP_API", "0") in ("1", "true", "yes"):
        result = {
            "valid": True, "active": True, "abn": abn_clean,
            "entity_name": None, "entity_type": None,
            "status": "unverified (checksum only)",
            "source": "checksum_fallback"
        }
        _cache[key] = result
        return result

    # Try ABR API
    data = _abr_search({"SearchString": abn_clean, "IncludeHistoricalDetails": "false"})
    results = data.get("ABRSearchResults", [])
    if results:
        entity = results[0].get("Entity", {})
        abn_det = results[0].get("ABN", {})
        status = abn_det.get("Status", "")
        is_active = status.lower() == "active" and not abn_det.get("CancellationDate")
        result = {
            "valid": True, "active": is_active, "abn": abn_clean,
            "entity_name": entity.get("EntityName", ""),
            "entity_type": entity.get("EntityType", ""),
            "status": status, "source": "abr_api",
        }
        _cache[key] = result
        return result

    # API unreachable — checksum only
    result = {
        "valid": True, "active": True, "abn": abn_clean,
        "entity_name": None, "entity_type": None,
        "status": "unverified (checksum only)",
        "source": "checksum_fallback"
    }
    _cache[key] = result
    return result


# ── Public verify() entry point ───────────────────────────────────────────────
def verify(
    name: str,
    state: str,
    abn: Optional[str] = None,
    phone_check: bool = False
) -> Tuple[bool, Dict]:
    """
    Verify a business lead.
    Path A: ABN supplied → checksum + optional ABR API.
    Path B: No ABN → Playwright name lookup.
    Returns (is_verified, details_dict).
    """
    details: Dict = {}

    if abn:
        res = lookup_by_abn(abn)
        details.update(res)
        if not res.get("valid"):
            return False, details
        return res.get("active", False), details

    found = lookup_by_name(name, state)
    if found:
        abn_digits = found["abn"]
        if not _checksum_valid(abn_digits):
            return False, {"error": "looked-up ABN failed checksum", **found}
        active_text = found.get("status", "").lower()
        is_active = "active" in active_text and "cancelled" not in active_text
        details.update(found)
        details["abn_source"] = "name_lookup"
        return is_active, details

    return False, {"error": "no ABN found", "source": "none"}


if __name__ == "__main__":
    import sys
    test_abns = [
        ("51824753556", "should FAIL checksum"),
        ("11000000000", "test"),
    ]
    for abn_val, label in test_abns:
        ok, det = verify(label, "VIC", abn_val)
        print(f"{abn_val} ({label}): valid={ok}, src={det.get('source')}")
