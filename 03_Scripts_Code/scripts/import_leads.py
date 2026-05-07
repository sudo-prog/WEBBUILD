#!/usr/bin/env python3
"""
Import consolidated weekly ABN leads into Supabase.

Consumes: data/weekly_leads_YYYYMMDD.json (output of weekly_abn_pipeline.py)
Upserts into: leads table
Audit trail: ingestion_log row with source = 'abn_weekly_bulk'

Idempotent: re-running the same week file is safe (ON CONFLICT DO UPDATE).
"""
import os, sys, json, argparse, logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import psycopg2
from psycopg2.extras import execute_batch

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("import_leads")

# ── Supabase credentials (same cascade as abn_enrichment.py) ────────────────────
SUPABASE_URL   = os.getenv("SUPABASE_URL", "http://localhost:6543")
SUPABASE_DB    = os.getenv("SUPABASE_DB", "postgres")
SUPABASE_USER  = os.getenv("SUPABASE_USER", "postgres")

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


def _supabase_host_port(url: str):
    url_clean = url.replace("http://", "").replace("https://", "")
    if ":" in url_clean:
        host, port = url_clean.split(":", 1)
        return host, int(port)
    return url_clean, 5432


def connect_db():
    host, port = _supabase_host_port(SUPABASE_URL)
    return psycopg2.connect(host=host, port=port, dbname=SUPABASE_DB, user=SUPABASE_USER, password=str(SUPABASE_PASS))


def upsert_leads(leads: List[Dict], source_tag: str) -> int:
    """
    Batch upsert leads into the leads table.

    Conflict key: (business_name, city)
    """
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
    ON CONFLICT (lead_id) DO UPDATE SET
        phone       = COALESCE(EXCLUDED.phone, leads.phone),
        email       = COALESCE(EXCLUDED.email, leads.email),
        abn         = EXCLUDED.abn,
        updated_at  = NOW();
    """
    rows = [(
        l.get("business_name") or "",
        l.get("category") or "trade",
        l.get("phone"),
        l.get("email"),
        l.get("website"),
        l.get("city", "").title(),
        l.get("state", "").upper(),
        l.get("suburb"),
        l.get("postcode"),
        l.get("address_full"),
        source_tag,
        l.get("abn"),
        l.get("lead_score"),
        l.get("needs_review", False),
    ) for l in leads]

    conn = connect_db()
    cur = conn.cursor()
    execute_batch(cur, query, rows, page_size=100)
    inserted = cur.rowcount
    conn.commit()
    cur.close(); conn.close()
    log.info(f"Upserted {inserted} leads (source={source_tag})")
    return inserted


def log_ingestion(source: str, city: str, count: int, status: str = "completed", error: str = None):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ingestion_log (source, city, status, records_ingested, error_message, started_at, completed_at)
        VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
    """, (source, city, status, count, error))
    conn.commit(); cur.close(); conn.close()


def main():
    p = argparse.ArgumentParser(description="Import weekly ABN leads JSON into Supabase")
    p.add_argument("json_file", type=Path, help="Path to weekly_leads_YYYYMMDD.json")
    p.add_argument("--source", default="abn_weekly_bulk", help="Source tag for ingestion_log")
    args = p.parse_args()

    if not args.json_file.exists():
        log.error(f"File not found: {args.json_file}")
        return 1

    log.info(f"Loading {args.json_file}")
    leads = json.loads(args.json_file.read_text())
    log.info(f"Loaded {len(leads):,} lead records")

    if not leads:
        log.warning("No leads to import")
        return 0

    # Group by city for ingestion log
    by_city: Dict[str, List[Dict]] = {}
    for lead in leads:
        city = (lead.get("city") or "unknown").lower()
        by_city.setdefault(city, []).append(lead)

    total = 0
    for city, batch in by_city.items():
        count = upsert_leads(batch, args.source)
        log_ingestion(args.source, city, count, "success")
        total += count

    log.info(f"✅ Total upserted: {total:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
