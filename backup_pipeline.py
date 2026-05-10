#!/usr/bin/env python3
"""
WEBBUILD — Fixed Ingestion Pipeline  [patched: lead_id canonicalised]

Bug fixed in this version:
  - lead_id was generated with re.sub(r'[^a-z0-9]', '-', name)[:40] + random uuid4 suffix.
  - ingestion_pipeline.py used name.replace(' ', '-')[:50] + random uuid4 suffix.
  - The two produced DIFFERENT IDs for the same business (e.g. "J & D Plumbing"
    became "j---d-plumbing..." vs "j-&-d-plumbing..."), so ON CONFLICT(lead_id)
    never matched, inserting duplicates that then crashed on the
    (business_name, city) UNIQUE constraint.
  - Fix: use lead_id_utils.make_lead_id() everywhere — deterministic uuid5-based
    suffix, consistent slug normalisation, stable across all scripts.

Usage:
  python pipeline_fixed.py --city sydney --source abn_bulk --limit 500
  python pipeline_fixed.py --all --source yellow_pages
  python pipeline_fixed.py --city melbourne --dry-run
"""
import os, sys, json, re, time, random, logging, argparse, csv, uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("ERROR: pip install psycopg2-binary")
    sys.exit(1)

# ── Import canonical lead_id utility ─────────────────────────────────────────
# lead_id_utils.py must live in the same directory as this script.
sys.path.insert(0, str(Path(__file__).parent))
from lead_id_utils import patch_validate_lead_id  # noqa: E402

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("webbuild")

PROJECT_ROOT = Path(__file__).parent

# ── Cities ────────────────────────────────────────────────────────────────────
CITY_MAP = {
    "sydney":    {"state": "NSW", "city": "Sydney"},
    "melbourne": {"state": "VIC", "city": "Melbourne"},
    "brisbane":  {"state": "QLD", "city": "Brisbane"},
    "perth":     {"state": "WA",  "city": "Perth"},
    "adelaide":  {"state": "SA",  "city": "Adelaide"},
    "hobart":    {"state": "TAS", "city": "Hobart"},
    "darwin":    {"state": "NT",  "city": "Darwin"},
    "canberra":  {"state": "ACT", "city": "Canberra"},
}

# ── Trade keywords ────────────────────────────────────────────────────────────
CATEGORY_MAP = {
    "plumber":          ["plumber", "plumbing", "drain", "hot water", "leak"],
    "electrician":      ["electrician", "electrical", "wiring", "switchboard"],
    "builder":          ["builder", "construction", "renovation", "carpenter"],
    "painter":          ["painter", "painting", "decorator"],
    "roofer":           ["roofer", "roofing", "guttering"],
    "air conditioning": ["air conditioning", "hvac", "ducted", "cooling"],
    "solar":            ["solar", "photovoltaic", "pv", "battery"],
    "flooring":         ["flooring", "tiles", "carpet", "timber floor"],
    "kitchen":          ["kitchen", "bathroom", "joinery", "cabinet"],
    "mechanic":         ["mechanic", "auto repair", "car service", "tyre"],
    "pest control":     ["pest control", "termite", "exterminator"],
}
KEYWORD_TO_CATEGORY = {kw: cat for cat, kws in CATEGORY_MAP.items() for kw in kws}


# ── Config loader ─────────────────────────────────────────────────────────────
def load_config() -> Dict:
    cfg_path = PROJECT_ROOT / "config" / "settings.json"
    if cfg_path.exists():
        cfg = json.loads(cfg_path.read_text())
        log.info(f"Config loaded from {cfg_path}: {cfg}")
        return cfg
    return {
        "postgres": {
            "host": os.getenv("PG_HOST", "127.0.0.1"),
            "port": int(os.getenv("PG_PORT", "6543")),
            "database": os.getenv("PG_DATABASE", "postgres"),
            "user": os.getenv("PG_USER", "supabase_service"),
            "password": os.getenv("PG_PASSWORD", ""),
        },
        "ingestion": {"batch_size": 100}
    }


# ── DB connection ─────────────────────────────────────────────────────────────
def connect_db(cfg: Dict) -> psycopg2.extensions.connection:
    pg = cfg["postgres"]
    url = cfg.get("supabase", {}).get("url", "")
    if url:
        clean = url.replace("http://", "").replace("https://", "")
        if ":" in clean:
            host, port_str = clean.split(":", 1)
            pg = {**pg, "host": host, "port": int(port_str)}
    return psycopg2.connect(
        host=pg["host"], port=pg["port"],
        dbname=pg["database"], user=pg["user"], password=pg["password"]
    )


# ── Phone normalisation ───────────────────────────────────────────────────────
_PLACEHOLDER_PHONE = re.compile(
    r"^(?:13\d{2}|1800|190\d)\s*[-\s]\s*(?:\d{3}\s*[-\s]\s*\d{3}|\d{6})$", re.I
)

def normalise_phone(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    raw = raw.strip()
    if _PLACEHOLDER_PHONE.match(raw):
        return None
    digits = re.sub(r"[^\d]", "", raw)
    if digits.startswith("04") and len(digits) == 10:
        return f"+61{digits[1:]}"
    elif digits.startswith("0") and 9 <= len(digits) <= 10:
        return f"+61{digits[1:]}"
    elif digits.startswith("61") and len(digits) == 11:
        return f"+{digits}"
    return raw


# ── Category detection ────────────────────────────────────────────────────────
def detect_category(text: str) -> Optional[str]:
    text = text.lower()
    for kw, cat in KEYWORD_TO_CATEGORY.items():
        if kw in text:
            return cat
    return None


# ── Lead validation ───────────────────────────────────────────────────────────
AUSTRALIAN_STATES = {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"}

def validate_lead(raw: Dict, city_cfg: Dict) -> Optional[Dict]:
    name = (raw.get("business_name") or "").strip()
    if not name or len(name) < 3:
        return None

    category = (raw.get("category") or detect_category(name) or "trade").strip()
    state = (raw.get("state") or city_cfg.get("state", "")).upper()
    if state not in AUSTRALIAN_STATES:
        return None

    # Website rejection — key filter
    website = (raw.get("website") or "").strip()
    if website and website.lower() not in ("", "n/a", "null", "none"):
        return None

    phone = normalise_phone(raw.get("phone"))
    email = (raw.get("email") or "").strip().lower() or None
    if email and "@" not in email:
        email = None

    score = 40
    if phone: score += 15
    if email: score += 15
    if raw.get("abn"): score += 15
    if raw.get("gst_registered"): score += 10
    score = min(100, score)

    tier = "HIGH" if score >= 75 else "MEDIUM" if score >= 55 else "LOW"

    # ── FIXED: use canonical lead_id (was: random uuid4 suffix + inconsistent slug) ──
    lead_id = patch_validate_lead_id(raw, city_cfg)

    return {
        "lead_id": lead_id,
        "source": raw.get("source", "webbuild"),
        "ingestion_batch_id": str(uuid.uuid4()),
        "business_name": name,
        "abn": raw.get("abn"),
        "category": category,
        "phone": phone,
        "email": email,
        "website": None,
        "country": "Australia",
        "state": state,
        "city": raw.get("city") or city_cfg.get("city", ""),
        "suburb": raw.get("suburb"),
        "postcode": raw.get("postcode"),
        "address_full": raw.get("address_full"),
        "lead_score": score,
        "tier": tier,
        "is_active": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Fetchers ──────────────────────────────────────────────────────────────────
def fetch_manual_csv(city_key: str) -> List[Dict]:
    path = PROJECT_ROOT / "data" / "inputs" / f"{city_key}_leads.csv"
    if not path.exists():
        return []
    leads = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            row["source"] = "manual_csv"
            leads.append(row)
    log.info(f"  CSV: {len(leads)} from {path.name}")
    return leads


def fetch_abn_leads(city_key: str, limit: int = 500) -> List[Dict]:
    leads_dir = Path("/home/thinkpad/data/abn/leads")
    state = CITY_MAP[city_key]["state"]
    results = []
    if not leads_dir.exists():
        log.warning(f"ABN leads dir not found: {leads_dir}")
        return fetch_manual_csv(city_key)

    for jl_file in sorted(leads_dir.glob("trades_part*.jsonl")):
        if len(results) >= limit:
            break
        with open(jl_file) as f:
            for line in f:
                if len(results) >= limit:
                    break
                try:
                    rec = json.loads(line)
                    if rec.get("state") == state and rec.get("city") == CITY_MAP[city_key]["city"]:
                        results.append({**rec, "source": "abn_bulk"})
                except Exception:
                    continue

    log.info(f"  ABN: {len(results)} leads for {city_key}")
    return results or fetch_manual_csv(city_key)


def fetch_yellow_pages(city_key: str, limit: int = 500) -> List[Dict]:
    yp_dir = PROJECT_ROOT / "raw_leads" / "yellow_pages_batch"
    city_name = CITY_MAP[city_key]["city"]
    results = []

    if yp_dir.exists():
        for jl in sorted(yp_dir.glob("yp_batch_*.jsonl"), reverse=True):
            if len(results) >= limit:
                break
            with open(jl) as f:
                for line in f:
                    if len(results) >= limit:
                        break
                    try:
                        rec = json.loads(line)
                        if rec.get("city", "").lower() == city_name.lower():
                            if not rec.get("yp_url"):
                                results.append({**rec, "source": "yellow_pages_batch"})
                    except Exception:
                        continue

    if not results:
        yp_dir2 = PROJECT_ROOT / "raw_leads" / "yellow_pages"
        if yp_dir2.exists():
            for jf in sorted(yp_dir2.glob(f"yp_{city_key.lower()}*.json"), reverse=True):
                try:
                    data = json.loads(jf.read_text())
                    for rec in data:
                        if len(results) >= limit:
                            break
                        if not rec.get("has_website", True):
                            results.append({**rec, "source": "yellow_pages"})
                except Exception:
                    continue

    log.info(f"  YP: {len(results)} leads for {city_key}")
    return results or fetch_manual_csv(city_key)


def fetch_leads(city_key: str, source: str, limit: int) -> List[Dict]:
    if source == "abn_bulk":
        return fetch_abn_leads(city_key, limit)
    elif source == "yellow_pages":
        return fetch_yellow_pages(city_key, limit)
    elif source == "abn_yp_merge":
        a = fetch_abn_leads(city_key, limit // 2)
        b = fetch_yellow_pages(city_key, limit // 2)
        return a + b
    else:
        return fetch_manual_csv(city_key)


# ── Upsert ────────────────────────────────────────────────────────────────────
UPSERT_SQL = """
INSERT INTO leads (
    lead_id, source, ingestion_batch_id,
    business_name, abn, category,
    phone, email, website,
    country, state, city, suburb, postcode, address_full,
    lead_score, tier, is_active,
    created_at, updated_at
) VALUES (
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s
)
ON CONFLICT (lead_id) DO UPDATE SET
    phone      = COALESCE(EXCLUDED.phone, leads.phone),
    email      = COALESCE(EXCLUDED.email, leads.email),
    abn        = COALESCE(EXCLUDED.abn,   leads.abn),
    lead_score = GREATEST(EXCLUDED.lead_score, leads.lead_score),
    updated_at = NOW();
"""

def upsert_leads(conn, leads: List[Dict], batch_size: int = 100) -> Tuple[int, int]:
    rows = [(
        l["lead_id"], l["source"], l["ingestion_batch_id"],
        l["business_name"], l.get("abn"), l["category"],
        l.get("phone"), l.get("email"), None,
        "Australia", l["state"], l["city"], l.get("suburb"), l.get("postcode"), l.get("address_full"),
        l.get("lead_score", 50), l.get("tier", "LOW"), True,
        l["created_at"], l["updated_at"]
    ) for l in leads]

    inserted = 0
    failed = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            try:
                execute_batch(cur, UPSERT_SQL, batch, page_size=batch_size)
                inserted += len(batch)
            except Exception as e:
                log.error(f"Batch {i // batch_size + 1} failed: {e}")
                conn.rollback()
                failed += len(batch)
    conn.commit()
    return inserted, failed


def log_ingestion(conn, source: str, city: str, count: int, status: str = "success"):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ingestion_log
                    (batch_id, source_name, city_target, state_target, record_count, status, started_at, completed_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT DO NOTHING
            """, (str(uuid.uuid4()), source, city, CITY_MAP.get(city.lower(), {}).get("state", ""), count, status))
        conn.commit()
    except Exception as e:
        log.warning(f"Could not write ingestion_log: {e}")


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run_city(city_key: str, source: str, limit: int, dry_run: bool, cfg: Dict) -> Dict:
    city_cfg = CITY_MAP[city_key]
    log.info(f"\n{'=' * 55}")
    log.info(f"City: {city_cfg['city']} ({city_cfg['state']})  source={source}  limit={limit}  dry={dry_run}")
    log.info(f"{'=' * 55}")

    raw = fetch_leads(city_key, source, limit)
    log.info(f"Fetched {len(raw)} raw leads")

    valid = [v for r in raw if (v := validate_lead(r, city_cfg))]
    skipped = len(raw) - len(valid)
    log.info(f"Validated: {len(valid)} OK, {skipped} skipped")

    if not valid:
        log.warning("No valid leads — skipping upload")
        return {"city": city_key, "fetched": len(raw), "valid": 0, "inserted": 0, "failed": 0}

    if dry_run:
        log.info("[DRY-RUN] Would insert/upsert:")
        for l in valid[:5]:
            log.info(f"  {l['business_name']} | {l['lead_id']} | score={l['lead_score']}")
        if len(valid) > 5:
            log.info(f"  … and {len(valid) - 5} more")
        return {"city": city_key, "fetched": len(raw), "valid": len(valid), "inserted": len(valid), "failed": 0}

    try:
        conn = connect_db(cfg)
        inserted, failed = upsert_leads(conn, valid, cfg["ingestion"].get("batch_size", 100))
        log_ingestion(conn, source, city_cfg["city"], inserted)
        conn.close()
        log.info(f"✅ {city_key}: {inserted} inserted, {failed} failed")
        return {"city": city_key, "fetched": len(raw), "valid": len(valid), "inserted": inserted, "failed": failed}
    except Exception as e:
        log.error(f"DB error for {city_key}: {e}")
        return {"city": city_key, "fetched": len(raw), "valid": len(valid), "inserted": 0, "failed": len(valid)}


def print_summary(results: List[Dict]):
    print("\n" + "=" * 60)
    print("WEBBUILD PIPELINE SUMMARY")
    print("=" * 60)
    for r in results:
        icon = "✅" if r.get("inserted", 0) > 0 else "⚠️"
        print(f"{icon} {r['city']:<12} fetched={r.get('fetched', 0):>4}  valid={r.get('valid', 0):>4}  "
              f"inserted={r.get('inserted', 0):>4}  failed={r.get('failed', 0):>3}")
    total_i = sum(r.get("inserted", 0) for r in results)
    total_f = sum(r.get("failed", 0) for r in results)
    total_r = sum(r.get("fetched", 0) for r in results)
    print(f"\nTOTAL  fetched={total_r}  inserted={total_i}  failed={total_f}")
    print("=" * 60)


def main():
    p = argparse.ArgumentParser(description="WEBBUILD Lead Generation & Upload Pipeline")
    p.add_argument("--city", choices=list(CITY_MAP.keys()), help="Single city")
    p.add_argument("--all", action="store_true", help="Run all 8 cities")
    p.add_argument("--source", default="abn_bulk",
                   choices=["abn_bulk", "yellow_pages", "abn_yp_merge", "manual_csv"],
                   help="Lead source")
    p.add_argument("--limit", type=int, default=500, help="Max leads per city")
    p.add_argument("--dry-run", action="store_true", help="Validate only, no DB writes")
    p.add_argument("--config", default="config/settings.json")
    args = p.parse_args()

    if not args.city and not args.all:
        p.error("Specify --city <name> or --all")

    cfg = load_config()
    if args.config and Path(args.config).exists():
        cfg = json.loads(Path(args.config).read_text())

    if not cfg["postgres"].get("password") and not args.dry_run:
        log.error("No DB password set. Use --dry-run or set PG_PASSWORD env var.")
        sys.exit(1)

    cities = list(CITY_MAP.keys()) if args.all else [args.city]
    results = []
    for city in cities:
        try:
            r = run_city(city, args.source, args.limit, args.dry_run, cfg)
            results.append(r)
        except Exception as e:
            log.error(f"City {city} crashed: {e}")
            results.append({"city": city, "fetched": 0, "valid": 0, "inserted": 0, "failed": 0})

    print_summary(results)
    return 0 if all(r.get("failed", 0) == 0 for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
