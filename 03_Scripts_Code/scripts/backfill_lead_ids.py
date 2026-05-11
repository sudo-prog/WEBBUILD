#!/usr/bin/env python3
"""
backfill_lead_ids.py — one-time migration to fix existing leads in Supabase.

Fetches all rows and updates any lead_id that does not match the canonical
format produced by lead_id_utils.make_lead_id(). This ensures exact parity
with the Python code — the uuid5 suffix matches perfectly, so future upserts
from pipeline_fixed.py will find and update the right rows instead of
inserting new duplicates.

Run ONCE after deploying lead_id_utils.py and the patched pipeline scripts.

Usage:
    python backfill_lead_ids.py                     # dry run by default
    python backfill_lead_ids.py --commit            # actually update DB
    python backfill_lead_ids.py --commit --batch 500
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_batch

sys.path.insert(0, str(Path(__file__).parent))
from lead_id_utils import make_lead_id  # noqa: E402

# ── DB connection ─────────────────────────────────────────────────────────────
def connect_db() -> psycopg2.extensions.connection:
    # Mirror the credential cascade from abn_enrichment.py
    password = os.getenv("PG_PASSWORD") or os.getenv("SUPABASE_PASSWORD")
    if not password:
        cred_path = Path.home() / ".config" / "hermes" / "supabase-credentials.json"
        if cred_path.exists():
            password = json.loads(cred_path.read_text()).get("password")
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "6543")),
        dbname=os.getenv("PG_DATABASE", "postgres"),
        user=os.getenv("PG_USER", "supabase_service"),
        password=password or "",
    )


def main():
    p = argparse.ArgumentParser(description="Backfill canonical lead_ids in Supabase leads table")
    p.add_argument(
        "--commit", action="store_true", help="Actually write to DB (default: dry run)"
    )
    p.add_argument(
        "--batch", type=int, default=200, help="Rows per UPDATE batch (default 200)"
    )
    p.add_argument(
        "--limit", type=int, default=0, help="Max rows to process (0 = all)"
    )
    args = p.parse_args()

    print("=" * 60)
    print("WEBBUILD — lead_id backfill migration")
    print(f"Mode: {'LIVE (--commit)' if args.commit else 'DRY RUN (pass --commit to write)'}")
    print("=" * 60)

    conn = connect_db()
    cur = conn.cursor()

    # Fetch all rows (id, lead_id, business_name, state)
    cur.execute("SELECT id, lead_id, business_name, state FROM leads ORDER BY id")
    rows = cur.fetchall()
    print(f"\nTotal leads in DB: {len(rows)}")

    to_update = []
    already_correct = 0
    missing_name = 0

    for row_id, lead_id, name, state in rows:
        if not name or not name.strip():
            missing_name += 1
            continue
        expected_id = make_lead_id(state or "unk", name)
        if not lead_id or lead_id.strip() == "":
            to_update.append((expected_id, row_id))
        elif lead_id != expected_id:
            to_update.append((expected_id, row_id))
        else:
            already_correct += 1

    print(f"Already correct:  {already_correct}")
    print(f"Missing name (skip):{missing_name}")
    print(f"Need backfill:      {len(to_update)}")

    if args.limit:
        to_update = to_update[:args.limit]
        print(f"(Limited to first {args.limit})")

    if not to_update:
        print("\nNothing to do.")
        return 0

    # Show sample of what would change
    print("\nSample changes (first 10):")
    sample_cur = conn.cursor()
    for new_id, row_id in to_update[:10]:
        sample_cur.execute(
            "SELECT lead_id, business_name, state FROM leads WHERE id = %s", (row_id,)
        )
        old_id, bname, state = sample_cur.fetchone()
        print(f"  id={row_id}  {(old_id or 'NULL'):50s}  →  {new_id}")

    if not args.commit:
        print(f"\n[DRY RUN] Would update {len(to_update)} rows. Pass --commit to apply.")
        return 0

    # Detect and skip collisions (another row already has the new_id)
    print(f"\nUpdating {len(to_update)} rows…")
    UPDATE_SQL = "UPDATE leads SET lead_id = %s, updated_at = NOW() WHERE id = %s"

    updated = 0
    skipped_collision = 0

    for i in range(0, len(to_update), args.batch):
        batch = to_update[i:i + args.batch]
        for new_id, row_id in batch:
            try:
                cur.execute(UPDATE_SQL, (new_id, row_id))
                updated += 1
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                skipped_collision += 1
                # The canonical ID is already in use by a different row — 
                # this means a duplicate exists. The SQL migration
                # (fix_duplicate_lead_ids.sql) should be run first to
                # remove duplicates before this backfill.
                print(f"  COLLISION on {new_id!r} — run fix_duplicate_lead_ids.sql first")
            except Exception as e:
                conn.rollback()
                print(f"  ERROR row {row_id}: {e}")
            else:
                conn.commit()

    print(f"\n✅ Updated:  {updated}")
    print(f"   Skipped (collision): {skipped_collision}")
    print(f"   Run fix_duplicate_lead_ids.sql first if collisions > 0")
    return 0


if __name__ == "__main__":
    sys.exit(main())