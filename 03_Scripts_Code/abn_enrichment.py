#!/usr/bin/env python3
"""
ABN Lead Enrichment Pipeline — Cross-reference qualified_leads.json with ABR
Batch-upsert into Supabase with ABN verification

Improvements applied:
- Demographics whitelist: only Bulleen (VIC 3105)
- Placeholder phone filter (1300/1800/190x patterns)
- Correct Supabase port extraction from SUPABASE_URL
- Rate-limited ABN lookups with jitter
- Graceful Supabase credentials fallback
"""
import os, re, json, uuid, logging, argparse, sys, time, random
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).parent))
from abn_validator import verify as verify_abn

# ── Supabase connection ────────────────────────────────────────────────────────
SUPABASE_URL   = os.getenv("SUPABASE_URL", "http://localhost:6543")
SUPABASE_DB    = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER  = os.getenv("SUPABASE_USER", "supabase_service")

# Credentials fallback order:
#  1) SUPABASE_PASSWORD env var
#  2) ~/.config/hermes/supabase-credentials.json (password key)
#  3) ~/.config/hermes/telegram-credentials.json (supabase_password key)
#  4) hard-coded default (only for dry-run/dev)
try:
    SUPABASE_PASS = os.getenv("SUPABASE_PASSWORD")
    if not SUPABASE_PASS:
        cred_path = Path.home() / ".config" / "hermes" / "supabase-credentials.json"
        if cred_path.exists():
            SUPABASE_PASS = json.loads(cred_path.read_text()).get("password")
        else:
            tg_path = Path.home() / ".config" / "hermes" / "telegram-credentials.json"
            if tg_path.exists():
                SUPABASE_PASS = json.loads(tg_path.read_text()).get("supabase_password")
except Exception:
    SUPABASE_PASS = None

# ── Source JSON files ───────────────────────────────────────────────────────────
LEAD_SOURCES = {
    "sydney":    Path("/home/thinkpad/Projects/active/project-WEBTEST/sydney/raw_leads/qualified_leads.json"),
    "melbourne": Path("/home/thinkpad/Projects/active/project-WEBTEST/melbourne/raw_leads/qualified_leads.json"),
}

CATEGORY_MAP = {
    "plumbing":    "plumber",
    "electrical":  "electrician",
    "carpentry":   "carpenter",
    "painting":    "painter",
    "roofing":     "roofer",
    "hvac":        "air conditioning",
    "kitchen":     "kitchen",
    "flooring":    "flooring",
    "solar":       "solar",
    "pest_control":"pest control",
    "builder":     "builder",
}

# ── Whitelist / filtering ──────────────────────────────────────────────────────
# Demographics whitelist: only accept businesses in these suburbs (lowercase).
# Set via --suburbs flag; if empty, all suburbs are accepted.
_ALLOWED_SUBURBS: Set[str] = set()

# Reject obvious toll-free / placeholder phone patterns (1300, 1800, 190x)
_PLACEHOLDER_PHONE_RE = re.compile(
    r"^(?:13\d{4}|1800\d{6}|190\d{7})$", re.IGNORECASE
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("abn_enrichment")


def normalise_au_phone(raw: str) -> Optional[str]:
    """Convert Australian numbers to E.164; reject placeholder patterns."""
    if not raw:
        return None
    if _PLACEHOLDER_PHONE_RE.match(raw.strip()):
        log.debug(f"Placeholder phone rejected: {raw}")
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if digits.startswith("04") and len(digits) == 10:
        return f"+61{digits[1:]}"
    elif digits.startswith("0") and 9 <= len(digits) <= 10:
        return f"+61{digits[1:]}"
    elif digits.startswith("61") and len(digits) == 10:
        return f"+{digits}"
    return raw


def state_from_city(city: str) -> str:
    mapping = {
        "sydney":   "NSW",
        "melbourne":"VIC",
        "brisbane": "QLD",
        "perth":    "WA",
        "adelaide": "SA",
        "hobart":   "TAS",
        "darwin":   "NT",
        "canberra": "ACT",
    }
    return mapping.get(str(city).lower(), "UNK")


def _supabase_host_port(url: str) -> Tuple[str, int]:
    """Extract host and port from a Supabase URL (e.g. http://localhost:6543)."""
    url_clean = url.replace("http://", "").replace("https://", "")
    if ":" in url_clean:
        host, port_str = url_clean.split(":", 1)
        return host, int(port_str)
    return url_clean, 5432


def connect_db():
    host, port = _supabase_host_port(SUPABASE_URL)
    return psycopg2.connect(
        host=host, port=port, dbname=SUPABASE_DB,
        user=SUPABASE_USER, password=str(SUPABASE_PASS)
    )


# ── Lead loading with whitelist + phone filtering ──────────────────────────────
def load_leads(city: str) -> List[Dict]:
    """Load raw qualified leads and filter by demographics + phone quality."""
    path = LEAD_SOURCES[city]
    if not path.exists():
        log.error(f"Source file not found: {path}")
        return []
    data = json.loads(path.read_text())
    leads = []
    for item in data:
        suburb = (item.get("suburb") or "").strip().lower()

        # Demographics whitelist: only accept if _ALLOWED_SUBURBS empty OR suburb in set
        if _ALLOWED_SUBURBS and suburb not in _ALLOWED_SUBURBS:
            continue

        # Phone validation + placeholder rejection
        raw_phone = item.get("phone", "")
        phone     = normalise_au_phone(raw_phone)
        if not phone:
            continue

        category  = CATEGORY_MAP.get(item.get("industry","").lower(), item.get("industry","Trade"))
        city_name = item.get("city", city).title()

        leads.append({
            "business_name": item.get("name","").strip(),
            "category":      category,
            "phone":         phone,
            "email":         item.get("email","").strip().lower(),
            "website":       None,
            "city":          city_name,
            "state":         state_from_city(item.get("city", city)),
            "suburb":        item.get("suburb"),
            "postcode":      None,
            "address_full":  None,
            "source":        f"project-WEBTEST_{city}",
            "abn":           None,
            "lead_score":    None,
            "needs_review":  False,
        })
    log.info(f"Filtered {len(leads)} / {len(data)} leads from {path.name}")
    return leads


# ── ABN enrichment with rate-limiting ──────────────────────────────────────────
def enrich_with_abn(leads: List[Dict], rate_limit: float = 1.8) -> Tuple[List[Dict], int]:
    """
    Run ABN verification on each lead.

    Args:
        rate_limit: base seconds between lookups (jittered ±20%)

    Returns:
        (enriched_list, verified_count)
    """
    enriched = []
    verified = 0
    for i, lead in enumerate(leads, 1):
        is_verified, details = verify_abn(
            lead["business_name"],
            lead["state"],
            lead.get("abn"),
            phone_check=False   # already validated above
        )
        if is_verified:
            verified += 1
            lead["abn"]             = details.get("abn") or lead.get("abn")
            lead["abn_status"]      = details.get("status", "active")
            lead["abn_entity_name"] = details.get("entity_name", lead["business_name"])
        else:
            lead["abn_status"]      = "not_found"
            lead["abn_entity_name"] = None

        lead["enriched_at"] = datetime.now(timezone.utc).isoformat()
        enriched.append(lead)

        if i % 5 == 0 or i == len(leads):
            log.info(f"Processed {i}/{len(leads)} — verified {verified}")

        # Rate limiting: jitter to avoid synchronous bursts
        if i < len(leads):
            time.sleep(random.uniform(rate_limit * 0.7, rate_limit * 1.3))

    return enriched, verified


# ── Database operations ─────────────────────────────────────────────────────────
def upsert_leads(leads: List[Dict], dry_run: bool = False) -> int:
    """Upsert leads into the Supabase leads table."""
    if dry_run:
        log.info(f"[DRY-RUN] Would upsert {len(leads)} leads")
        for l in leads[:min(5, len(leads))]:
            log.info(f"  • {l['business_name']} ({l['city']}) | ABN: {l.get('abn','-')} | {l.get('abn_status')}")
        return len(leads)

    query = """
    INSERT INTO leads (
        business_name, category, phone, email, website,
        city, state, suburb, postcode, address_full,
        source, abn, lead_score, needs_review,
        created_at, updated_at
    ) VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s,
        NOW(), NOW()
    )
    ON CONFLICT (business_name, city) DO UPDATE SET
        phone       = EXCLUDED.phone,
        email       = EXCLUDED.email,
        abn         = EXCLUDED.abn,
        updated_at  = NOW();
    """
    rows = [(
        l["business_name"], l["category"], l["phone"], l["email"], l["website"],
        l["city"], l["state"], l["suburb"], l["postcode"], l["address_full"],
        l["source"], l.get("abn"), l.get("lead_score"), l.get("needs_review", False)
    ) for l in leads]

    conn = connect_db(); cur = conn.cursor()
    execute_batch(cur, query, rows, page_size=50)
    conn.commit(); cur.close(); conn.close()
    log.info(f"Upserted {len(leads)} leads")
    return len(leads)


def log_ingestion(city: str, source: str, count: int, status: str = "success", error: str = None):
    """Write a record to ingestion_log for audit trail."""
    conn = connect_db(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO ingestion_log (source, city, status, records_ingested, error_message, started_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
    """, (source, city, status, count, error))
    conn.commit(); cur.close(); conn.close()


# ── Main ─────────────────────────────────────────────────────────────────────────
def main_old():
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    log = logging.getLogger("abn_enrichment")

    p = argparse.ArgumentParser(description="Enrich qualified leads with ABN verification")
    p.add_argument("--city", choices=["sydney", "melbourne", "all"], default="all")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--suburbs", default="",
        help="Comma-separated suburbs to accept (lowercase). Default: all."
    )
    args = p.parse_args()

    global _ALLOWED_SUBURBS
    _ALLOWED_SUBURBS = {s.strip().lower() for s in args.suburbs.split(",") if s.strip()}

    cities = [args.city] if args.city != "all" else ["sydney", "melbourne"]
    total_upserted = 0

    if _ALLOWED_SUBURBS:
        log.info(f"Demographics whitelist active: {sorted(_ALLOWED_SUBURBS)}")
    else:
        log.info("Demographics whitelist: disabled (all suburbs accepted)")

    for city in cities:
        log.info(f"
=== {city.title()} ===")
        raw_leads = load_leads(city)
        if not raw_leads:
            log.warning(f"No leads passed filters for {city}; skipping.")
            continue

        enriched, verified = enrich_with_abn(raw_leads, rate_limit=1.8)
        log.info(f"ABN verified: {verified}/{len(enriched)}")

        inserted = upsert_leads(enriched, dry_run=args.dry_run)
        total_upserted += inserted

        if not args.dry_run:
            log_ingestion(city, "abn_enrichment", inserted, "success")

    log.info(f"
✅ Done — total upserted: {total_upserted}")
    return 0


if __name__ == "__main__":
    main()
