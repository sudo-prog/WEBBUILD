#!/usr/bin/env python3
"""
scrape_yp_proper.py - Yellow Pages scraper using browser-harness + Chromium CDP.

Prerequisites:
    Chrome must be running with remote debugging:
        /home/thinkpad/chrome-linux/chrome --remote-debugging-port=9222 --no-sandbox \\
            --disable-setuid-sandbox --disable-blink-features=AutomationControlled \\
            --user-data-dir=/tmp/chrome-yp-profile

Usage:
    python3 scrape_yp_proper.py sydney plumbers --pages 3 -o yp_plumbers.csv
"""

import sys
import os
import csv
import json
import re
import subprocess
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

# Add browser-harness to path
sys.path.insert(0, "/home/thinkpad/browser-harness/src")

CDP_URL = "http://127.0.0.1:9222"
BASE_SEARCH_URL = "https://www.yellowpages.com.au/search/{city}/{category}"


def ensure_chrome():
    """Start Chrome with remote debugging if not already running."""
    try:
        import urllib.request
        with urllib.request.urlopen(CDP_URL + "/json/version", timeout=2) as r:
            return True
    except Exception:
        print("Starting Chrome with remote debugging...")
        subprocess.Popen([
            "/home/thinkpad/chrome-linux/chrome",
            "--remote-debugging-port=9222",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--user-data-dir=/tmp/chrome-yp-profile",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(4)
        return True


def ensure_harness_daemon():
    """Ensure browser-harness daemon is connected."""
    try:
        from browser_harness.helpers import page_info
        page_info()
        return True
    except Exception:
        subprocess.run(["browser-harness", "-c", "pass"],
                       capture_output=True, timeout=15)
        time.sleep(2)
        return True


def scrape_page(city: str, category: str, page_num: int = 1):
    """Scrape a single Yellow Pages search result page."""
    from browser_harness.helpers import js, goto_url

    url = BASE_SEARCH_URL.format(city=city, category=category)
    if page_num > 1:
        url += f"?page={page_num}"

    goto_url(url)

    # Wait for listings to render (poll for .srp-listing count)
    for _ in range(15):
        count = js("document.querySelectorAll('.srp-listing').length")
        if int(count) > 0:
            break
        time.sleep(1)
    else:
        print(f"  No listings found on page {page_num}")
        return []

    extract_script = """
    (() => {
        const listings = [];
        document.querySelectorAll('.srp-listing').forEach(node => {
            const nameNode = node.querySelector('.business-name span');
            const name = nameNode ? nameNode.innerText.trim() : null;
            if (!name) return;

            const adrNode = node.querySelector('.adr');
            let address = null;
            if (adrNode) {
                const street = adrNode.querySelector('.street-address');
                const locality = adrNode.querySelector('.locality');
                address = (street ? street.innerText.trim() : '') +
                          (locality ? ', ' + locality.innerText.trim() : '');
            } else {
                const pAdr = node.querySelector('p.adr');
                if (pAdr) address = pAdr.innerText.trim();
            }

            const sloganEl = node.querySelector('.slogan');
            const categories = Array.from(node.querySelectorAll('.categories a'))
                                   .map(a => a.innerText.trim()).join('; ');
            const ratingNode = node.querySelector('.result-avg-rating');
            const rating = ratingNode ? ratingNode.innerText.trim() : null;
            const countNode = node.querySelector('.count');
            const reviews = countNode ? countNode.innerText.replace(/[()]/g,'').trim() : null;
            const phoneNode = node.querySelector('.phone, .phones');
            const phone = phoneNode ? phoneNode.innerText.trim() : null;
            const linkNode = node.querySelector('.business-name');
            const detailUrl = linkNode ? linkNode.getAttribute('href') : null;

            listings.push({
                name, address, categories, rating, reviews, phone, detailUrl,
                slogan: sloganEl ? sloganEl.innerText.trim() : null
            });
        });
        return JSON.stringify(listings);
    })()
    """

    raw = js(extract_script)
    return json.loads(raw)


def parse_address(address_str: str):
    """Parse YP address string into suburb, state, postcode."""
    if not address_str:
        return None, None, None

    # "Street, Suburb, NSW 2000" or "Suburb, NSW 2000"
    m = re.search(r'(?:^|,)\s*([^,]+?),\s*([A-Z]{2,3})\s+(\d{4})\s*$', address_str)
    if m:
        return m.group(1).strip(), m.group(2), m.group(3)

    # "Serving Sydney, NSW"
    m = re.search(r'Serving\s+([^,]+?),\s*([A-Z]{2,3})', address_str)
    if m:
        return m.group(1).strip(), m.group(2), None

    return None, None, None


def map_category(raw_cat: str) -> str:
    """Map YP category string to our simplified category."""
    if not raw_cat:
        return "general"
    first = raw_cat.split(";")[0].strip().lower()
    # Normalise
    first = first.replace(" & ", "_").replace(" ", "_").replace("-", "_")
    # Strip common suffixes
    for suffix in ("_services", "_service", "_contractors", "_contractor"):
        if first.endswith(suffix):
            first = first[: -len(suffix)]
    return first


def calc_tier(rating, reviews) -> tuple[int, str]:
    """Return (score, tier) based on rating and review count."""
    score = 30
    try:
        if rating:
            score += float(rating) * 10
    except ValueError:
        pass
    try:
        if reviews and reviews.isdigit():
            score += min(int(reviews) * 2, 30)
    except ValueError:
        pass
    score = min(int(score), 100)
    tier = "HIGH" if score >= 80 else "MEDIUM" if score >= 50 else "LOW"
    return score, tier


def main():
    parser = argparse.ArgumentParser(description="Scrape Yellow Pages listings via Chromium CDP")
    parser.add_argument("city", help="City slug, e.g. sydney, melbourne, brisbane")
    parser.add_argument("category", help="YP category slug, e.g. plumbers, electricians, builders")
    parser.add_argument("--pages", type=int, default=3, help="Max pages to scrape (default: 3)")
    parser.add_argument("-o", "--output", default="yp_leads.csv", help="Output CSV path")
    parser.add_argument("--append", action="store_true", help="Append to existing CSV instead of overwrite")
    args = parser.parse_args()

    ensure_chrome()
    ensure_harness_daemon()

    all_listings = []
    seen = set()

    for page in range(1, args.pages + 1):
        print(f"Scraping page {page}...")
        listings = scrape_page(args.city, args.category, page)

        if not listings:
            print("  No listings — stopping.")
            break

        new_count = 0
        for listing in listings:
            name = listing["name"]
            if name in seen:
                continue
            seen.add(name)
            all_listings.append(listing)
            new_count += 1

        print(f"  {new_count} new / {len(listings)} raw | total unique: {len(all_listings)}")
        time.sleep(2)

    if not all_listings:
        print("No leads extracted.")
        return

    # Write CSV matching extracted_leads schema
    out_path = Path(args.output)
    mode = "a" if args.append else "w"
    header_needed = not out_path.exists() or not args.append

    with open(out_path, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if header_needed:
            writer.writerow([
                "business_name", "first_seen_at", "category", "lead_score",
                "website", "tier", "phone", "state", "postcode", "suburb", "city", "email"
            ])

        now = datetime.now(timezone.utc).isoformat()
        for listing in all_listings:
            suburb, state, postcode = parse_address(listing.get("address"))
            cat = map_category(listing.get("categories", ""))
            score, tier = calc_tier(listing.get("rating"), listing.get("reviews"))

            writer.writerow([
                listing["name"],
                now,
                cat,
                score,
                "",          # website (requires detail-page scrape)
                tier,
                listing.get("phone", ""),
                state or "",
                postcode or "",
                suburb or "",
                args.city.capitalize(),
                "",          # email
            ])

    print(f"Saved {len(all_listings)} leads to {out_path}")


if __name__ == "__main__":
    main()
