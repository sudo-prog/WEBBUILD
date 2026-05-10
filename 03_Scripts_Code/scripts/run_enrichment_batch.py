#!/usr/bin/env python3
"""
run_enrichment_batch.py — Drop-in replacement for the broken scraper-based enrichment.

Reads ABN trade lead JSONL files, enriches them with free per-business lookups
(no paid APIs, no bulk category scraping), and upserts results to Supabase.

Fits into existing pipeline:
  abn_trade_filter.py → THIS SCRIPT → Supabase leads table

Usage:
  # Enrich one city's leads and upsert
  python run_enrichment_batch.py --city sydney --limit 200

  # Enrich ALL cities, 100 leads each, resumable
  python run_enrichment_batch.py --all --limit 100 --resume

  # Dry run with debug output
  python run_enrichment_batch.py --city melbourne --limit 10 --dry-run --debug

  # Just enrich to file, skip DB upsert
  python run_enrichment_batch.py --city brisbane --limit 300 --no-db
"""

import sys, os, json, logging, argparse, subprocess, time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional

log = logging.getLogger("batch")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

PROJECT_ROOT = Path(__file__).parent

# Paths — adjust if your layout differs
ABN_LEADS_DIR  = Path("/home/thinkpad/data/abn/leads")
ENRICHED_DIR   = Path("/home/thinkpad/data/abn/enriched")
ENRICHER_SCRIPT = PROJECT_ROOT / "enrich_contacts_free.py"

CITY_STATE_MAP = {
    "sydney":    "NSW", "melbourne": "VIC", "brisbane":  "QLD",
    "perth":     "WA",  "adelaide":  "SA",  "hobart":    "TAS",
    "darwin":    "NT",  "canberra":  "ACT",
}

# ─────────────────────────────────────────────────────────────────────────────
# DB UPSERT  (reuses your existing import_leads.py logic)
# ─────────────────────────────────────────────────────────────────────────────

def upsert_to_supabase(enriched_file: Path, city: str, dry_run: bool = False) -> int:
    """
    Read an enriched JSONL and upsert only leads that now have phone OR email.
    Returns count of rows upserted.
    """
    try:
        import psycopg2
        from psycopg2.extras import execute_batch
    except ImportError:
        log.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        return 0

    # Load config (same cascade as the rest of the project)
    cfg_path = PROJECT_ROOT / "config" / "settings.json"
    if not cfg_path.exists():
        log.warning("config/settings.json not found — using env vars")
        pg_host     = os.getenv("PG_HOST", "127.0.0.1")
        pg_port     = int(os.getenv("PG_PORT", "6543"))
        pg_db       = os.getenv("PG_DATABASE", "postgres")
        pg_user     = os.getenv("PG_USER", "supabase_service")
        pg_password = os.getenv("PG_PASSWORD", "")
    else:
        cfg   = json.loads(cfg_path.read_text()).get("postgres", {})
        pg_host     = cfg.get("host", "127.0.0.1")
        pg_port     = int(cfg.get("port", 6543))
        pg_db       = cfg.get("database", "postgres")
        pg_user     = cfg.get("user", "supabase_service")
        pg_password = cfg.get("password", "")

    if not pg_password and not dry_run:
        log.error("No DB password. Set PG_PASSWORD env var or add to config/settings.json")
        return 0

    # Read enriched leads that actually gained contact data
    leads_to_upsert = []
    with enriched_file.open() as f:
        for line in f:
            try:
                rec = json.loads(line)
                if rec.get("phone") or rec.get("email"):
                    leads_to_upsert.append(rec)
            except Exception:
                continue

    log.info(f"Upserting {len(leads_to_upsert)} enriched leads for {city}")

    if dry_run:
        for l in leads_to_upsert[:5]:
            log.info(f"  [DRY] {l.get('business_name','')} | {l.get('phone','')} | {l.get('email','')}")
        return len(leads_to_upsert)

    if not leads_to_upsert:
        return 0

    SQL = """
    INSERT INTO leads (
        business_name, abn, category, phone, email, website,
        city, state, suburb, postcode, address_full,
        source, lead_score, needs_review,
        created_at, updated_at
    ) VALUES (
        %(business_name)s, %(abn)s, %(category)s, %(phone)s, %(email)s, %(website)s,
        %(city)s, %(state)s, %(suburb)s, %(postcode)s, %(address_full)s,
        %(source)s, %(lead_score)s, %(needs_review)s,
        NOW(), NOW()
    )
    ON CONFLICT (business_name, city) DO UPDATE SET
        phone      = COALESCE(EXCLUDED.phone, leads.phone),
        email      = COALESCE(EXCLUDED.email, leads.email),
        abn        = COALESCE(EXCLUDED.abn,   leads.abn),
        updated_at = NOW()
    WHERE leads.phone IS NULL OR leads.email IS NULL;
    """

    rows = []
    for l in leads_to_upsert:
        name = (l.get("trading_name") or l.get("business_name") or "").strip()
        city_val = (l.get("city") or city).title()
        state_val = (l.get("address_state") or l.get("state") or CITY_STATE_MAP.get(city,"")).upper()
        rows.append({
            "business_name": name,
            "abn":           l.get("abn"),
            "category":      l.get("category", "trade"),
            "phone":         l.get("phone"),
            "email":         l.get("email"),
            "website":       None,  # never set — these are no-website leads
            "city":          city_val,
            "state":         state_val,
            "suburb":        l.get("suburb"),
            "postcode":      l.get("address_postcode") or l.get("postcode"),
            "address_full":  l.get("address_full"),
            "source":        "abn_bulk_enriched_free",
            "lead_score":    _score(l),
            "needs_review":  False,
        })

    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password
        )
        with conn.cursor() as cur:
            execute_batch(cur, SQL, rows, page_size=50)
        conn.commit()
        conn.close()
        log.info(f"✅ Upserted {len(rows)} leads for {city}")
        return len(rows)
    except Exception as e:
        log.error(f"DB upsert failed: {e}")
        return 0


def _score(lead: Dict) -> int:
    score = 40
    if lead.get("phone"):  score += 20
    if lead.get("email"):  score += 20
    if lead.get("abn"):    score += 10
    if lead.get("gst_status") == "ACT": score += 10
    return min(100, score)


# ─────────────────────────────────────────────────────────────────────────────
# PER-CITY RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_city(city: str, limit: int, delay: float, resume: bool,
             dry_run: bool, no_db: bool, debug: bool) -> Dict:
    state = CITY_STATE_MAP.get(city, "")
    city_title = city.title()

    # Find input files for this city
    input_files = sorted(ABN_LEADS_DIR.glob("trades_part*.jsonl"))
    if not input_files:
        log.warning(f"No trade lead files in {ABN_LEADS_DIR}")
        return {"city": city, "processed": 0, "upserted": 0}

    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    out_file = ENRICHED_DIR / f"enriched_{city}_{datetime.now(timezone.utc).strftime('%Y%m%d')}.jsonl"

    log.info(f"\n{'='*55}")
    log.info(f"City: {city_title} ({state})  limit={limit}  delay={delay}s  resume={resume}")
    log.info(f"{'='*55}")

    # Run enricher as subprocess so it can be killed/resumed independently
    cmd = [
        sys.executable, str(ENRICHER_SCRIPT),
        "--input",  str(input_files[0]),    # first (largest) part; extend as needed
        "--output", str(out_file),
        "--limit",  str(limit),
        "--delay",  str(delay),
        "--state",  state,
        "--city",   city_title,
    ]
    if resume:
        cmd.append("--resume")
    if dry_run:
        cmd.append("--dry-run")
    if debug:
        cmd.append("--debug")

    log.info(f"Running: {' '.join(cmd)}")
    start = time.time()
    result = subprocess.run(cmd, timeout=7200)  # 2-hour max per city
    duration = time.time() - start

    if result.returncode != 0:
        log.error(f"Enricher exited {result.returncode} for {city}")
        return {"city": city, "processed": 0, "upserted": 0, "error": True}

    # Count enriched leads
    processed = enriched = 0
    if out_file.exists():
        with out_file.open() as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    processed += 1
                    if rec.get("phone") or rec.get("email"):
                        enriched += 1
                except Exception:
                    pass

    log.info(f"Enriched {enriched}/{processed} leads in {duration:.0f}s")

    # Upsert to Supabase
    upserted = 0
    if not no_db and out_file.exists():
        upserted = upsert_to_supabase(out_file, city, dry_run=dry_run)

    return {
        "city": city_title, "state": state,
        "processed": processed, "enriched": enriched, "upserted": upserted,
        "duration_s": round(duration),
        "output_file": str(out_file),
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Batch enrichment: ABN leads → contacts → Supabase")
    p.add_argument("--city",  choices=list(CITY_STATE_MAP.keys()), help="Single city")
    p.add_argument("--all",   action="store_true",                 help="Run all 8 cities")
    p.add_argument("--limit", type=int,   default=200,             help="Max leads per city (default 200)")
    p.add_argument("--delay", type=float, default=2.5,             help="Seconds between requests (default 2.5)")
    p.add_argument("--resume",  action="store_true",               help="Skip already-enriched ABNs")
    p.add_argument("--dry-run", action="store_true",               help="No HTTP calls, no DB writes")
    p.add_argument("--no-db",   action="store_true",               help="Enrich to file but skip DB upsert")
    p.add_argument("--debug",   action="store_true",               help="Verbose logging")
    args = p.parse_args()

    if not args.city and not args.all:
        p.error("Specify --city <name> or --all")

    if not ENRICHER_SCRIPT.exists():
        log.error(f"enrich_contacts_free.py not found at {ENRICHER_SCRIPT}")
        log.error("Place both scripts in the same directory.")
        sys.exit(1)

    cities = list(CITY_STATE_MAP.keys()) if args.all else [args.city]
    all_results = []

    for city in cities:
        result = run_city(
            city, args.limit, args.delay, args.resume,
            args.dry_run, args.no_db, args.debug
        )
        all_results.append(result)

    # Print summary table
    print(f"\n{'='*65}")
    print(f"{'ENRICHMENT BATCH SUMMARY':^65}")
    print(f"{'='*65}")
    print(f"{'City':<14} {'Processed':>10} {'Enriched':>10} {'Upserted':>10} {'Time':>8}")
    print(f"{'-'*65}")
    for r in all_results:
        flag = "⚠️" if r.get("error") else "✅"
        print(f"{flag} {r['city']:<12} {r.get('processed',0):>10,} {r.get('enriched',0):>10,} "
              f"{r.get('upserted',0):>10,} {r.get('duration_s',0):>6}s")

    total_p = sum(r.get("processed",0) for r in all_results)
    total_e = sum(r.get("enriched",0) for r in all_results)
    total_u = sum(r.get("upserted",0) for r in all_results)
    rate = total_e / max(total_p, 1) * 100
    print(f"{'='*65}")
    print(f"{'TOTAL':<14} {total_p:>10,} {total_e:>10,} ({rate:.0f}%) {total_u:>10,}")
    print(f"{'='*65}\n")

    # Save run log
    log_path = ENRICHED_DIR / f"run_log_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.json"
    ENRICHED_DIR.mkdir(parents=True, exist_ok=True)
    log_path.write_text(json.dumps(all_results, indent=2))
    log.info(f"Run log: {log_path}")


if __name__ == "__main__":
    main()
