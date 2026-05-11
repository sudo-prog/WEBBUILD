#!/usr/bin/env python3
"""
lead_id_utils.py — Canonical lead_id generation for WEBBUILD pipeline.

WHY THIS EXISTS:
  Three scripts were generating lead_id independently with subtly different
  slug logic, producing different IDs for the same business:

    pipeline_fixed.py:       re.sub(r'[^a-z0-9]', '-', name)[:40]   ← strips punctuation
    ingestion_pipeline.py:   name.replace(' ', '-')[:50]              ← keeps & . / etc.
    import_leads.py:         no lead_id at all                        ← NULL in DB
    abn_enrichment.py:       no lead_id at all                        ← NULL in DB

  Result: "J & D Plumbing" got three different IDs:
    - pipeline_fixed:      nsw-j---d-plumbing-services-a1b2c3d4
    - ingestion_pipeline:  nsw-j-&-d-plumbing-services-a1b2c3d4
    - import_leads:        NULL

  The ON CONFLICT(lead_id) upsert then failed to find the existing row,
  creating a duplicate — which then crashed on the (business_name, city)
  UNIQUE constraint added by schema_patch_v2.sql.

FIX:
  Single canonical function used everywhere. The slug:
    1. Lowercases the name
    2. Collapses ALL non-alphanumeric chars to a single hyphen
    3. Strips leading/trailing hyphens
    4. Truncates to 48 chars
    5. Appends a UUID4 suffix (8 chars) for global uniqueness

  The suffix is derived deterministically from (state, name) using
  uuid5(DNS, key) so the same business always gets the same ID —
  making the function idempotent across pipeline runs.

  uuid5 is used instead of random uuid4 so re-running a scrape on the
  same business produces the same lead_id, enabling true upsert behaviour
  on the lead_id conflict key.

USAGE:
    from lead_id_utils import make_lead_id, normalise_slug

    lead_id = make_lead_id("NSW", "J & D Plumbing Services Pty Ltd")
    # → "nsw-j-d-plumbing-services-pty-ltd-3f8a21b0"
    #   (always the same for this state+name pair)
"""

import re
import uuid
from typing import Optional


# Namespace UUID for deterministic uuid5 generation.
# This is a fixed value — do NOT change it or all existing IDs will shift.
_WEBBUILD_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # uuid.NAMESPACE_DNS


def normalise_slug(text: str) -> str:
    """
    Convert arbitrary business name text into a URL-safe slug.

    Steps:
      1. Lowercase
      2. Replace ALL non-alphanumeric sequences with a single hyphen
      3. Strip leading/trailing hyphens
      4. Truncate to 48 characters (on a word boundary where possible)

    Examples:
      "J & D Plumbing Services Pty Ltd" → "j-d-plumbing-services-pty-ltd"
      "J & D Plumbing Services Pty Ltd" → same as above (idempotent)
      "Smith's A/C & Heating"           → "smith-s-a-c-heating"
    """
    if not text:
        return "unknown"
    slug = text.lower().strip()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    if len(slug) > 48:
        # Truncate at last hyphen within 48 chars to avoid cutting mid-word
        truncated = slug[:48]
        last_hyphen = truncated.rfind("-")
        slug = truncated[:last_hyphen] if last_hyphen > 20 else truncated
    return slug or "unknown"


def make_lead_id(state: str, business_name: str) -> str:
    """
    Generate a canonical, deterministic lead_id for a business.

    The ID is stable: calling this function twice with the same arguments
    always returns the same string. This makes ON CONFLICT(lead_id) work
    correctly across pipeline runs.

    Format:  <state_lower>-<slug>-<8_char_uuid5_suffix>

    Args:
        state:         Two-letter Australian state code (e.g. "NSW")
        business_name: Raw business name as scraped/extracted

    Returns:
        A string like "nsw-j-d-plumbing-services-3f8a21b0"
    """
    state_part = (state or "unk").lower().strip()
    slug = normalise_slug(business_name)
    # Deterministic suffix: uuid5 of "state:name" ensures same business
    # always maps to same ID regardless of which script generates it.
    key = f"{state_part}:{business_name.lower().strip()}"
    suffix = str(uuid.uuid5(_WEBBUILD_NS, key)).replace("-", "")[:8]
    return f"{state_part}-{slug}-{suffix}"


def lead_id_from_raw(raw: dict, city_config: Optional[dict] = None) -> str:
    """
    Convenience wrapper: extract state + name from a raw lead dict and
    return the canonical lead_id.

    Falls back gracefully if fields are missing.
    """
    city_config = city_config or {}
    state = (raw.get("state") or city_config.get("state") or "unk").strip().upper()
    name = (raw.get("business_name") or "").strip()
    if not name:
        # Last resort: use a random UUID so the row can still be inserted
        # (it just won't de-duplicate against future runs)
        return f"{state.lower()}-unknown-{str(uuid.uuid4())[:8]}"
    return make_lead_id(state, name)


# ---------------------------------------------------------------------------
# Patch functions — drop these into each script's validate_lead() to replace
# the old ad-hoc lead_id generation in one line.
# ---------------------------------------------------------------------------

def patch_validate_lead_id(raw: dict, city_config: dict) -> str:
    """
    Replacement for the lead_id block in validate_lead() across all scripts.

    Old code (pipeline_fixed.py):
        lead_id = raw.get("lead_id") or (
            f"{state.lower()}-{re.sub(r'[^a-z0-9]', '-', name.lower())[:40]}-{str(uuid.uuid4())[:8]}"
        )

    Old code (ingestion_pipeline.py):
        slug = business_name.lower().replace(' ', '-')[:50]
        lead_id = f"{city_config.get('state', 'UNK').lower()}-{slug}-{str(uuid.uuid4())[:8]}"

    New code (everywhere):
        lead_id = patch_validate_lead_id(raw, city_config)
    """
    existing = (raw.get("lead_id") or "").strip()
    # If a lead_id already exists AND matches our canonical format, keep it.
    # Pattern: <2-3 char state>-<slug>-<8 hex chars>
    if existing and re.match(r"^[a-z]{2,3}-.+-[0-9a-f]{8}$", existing):
        return existing
    # Otherwise generate canonical ID
    return lead_id_from_raw(raw, city_config)


if __name__ == "__main__":
    # Quick smoke-test
    test_cases = [
        ("NSW", "J & D Plumbing Services Pty Ltd"),
        ("VIC", "Smith's A/C & Heating"),
        ("QLD", "North Shore Electrical"),
        ("WA",  ""),
        ("NSW", "J & D Plumbing Services Pty Ltd"),  # duplicate — must match first
    ]
    print("Smoke test — same input must always produce same ID:\n")
    results = {}
    for state, name in test_cases:
        lead_id = make_lead_id(state, name)
        key = (state, name)
        if key in results:
            match = "✓ MATCH" if results[key] == lead_id else "✗ MISMATCH"
            print(f"  {match}: {lead_id!r}")
        else:
            results[key] = lead_id
            print(f"  FIRST:  {lead_id!r}")

    print("\nSlug normalisation:")
    names = [
        "J & D Plumbing Services Pty Ltd",
        "j & d plumbing services pty ltd",   # same, different case
        "Smith's A/C & Heating",
        "Melbourne---Central---Electrical",
    ]
    for n in names:
        print(f"  {n!r:45} → {normalise_slug(n)!r}")
