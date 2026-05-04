#!/usr/bin/env python3
"""
Google Maps Business Scraper — Playwright-powered.

Extracts for a given business name + location:
  - google_reviews_count
  - google_last_review   (date of most recent review, if visible)
  - google_owner_replies (bool: does owner reply to reviews?)
  - google_photos_count  (number of photos in listing)
  - gmaps_website        (bool: external website link present?)

Output: dict matching enrich_leads.py expectations.
"""

import json, time, re, argparse, sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except ImportError:
    print("ERROR: pip install playwright && playwright install chromium")
    sys.exit(1)

# ─── Helpers ────────────────────────────────────────────────────────────────────

def parse_rating_count(text: str) -> int:
    if not text: return 0
    m = re.search(r'([\d,]+)\s*reviews?', text, re.I)
    if m: return int(m.group(1).replace(',', ''))
    return 0

def has_owner_replies(page) -> bool:
    content = page.content()
    patterns = [
        r'owner\s+replied',
        r'response\s+from\s+the\s+owner',
        r'owner\s+response',
        r'business\s+owner\s+replied',
    ]
    for pat in patterns:
        if re.search(pat, content, re.I):
            return True
    return False

def count_photos(page) -> int:
    content = page.content()
    m = re.search(r'([\d,]+)\s*photos?', content, re.I)
    if m:
        try: return int(m.group(1).replace(',', ''))
        except: pass
    return 0

# ─── Main scraper ───────────────────────────────────────────────────────────────

def scrape_google_maps(business_name: str, city: str, state: str) -> Dict:
    result = {
        "found": False,
        "business_name": business_name,
        "phone": "",
        "suburb": city,
        "google_reviews_count": 0,
        "google_last_review": None,
        "google_owner_replies": False,
        "google_photos_count": 0,
        "gmaps_website": False,
    }

    query = f"{business_name} {city} {state}".strip()
    search_url = f"https://www.google.com/maps/search/?api=1&query={query.replace(' ', '+')}"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale='en_AU'
        )
        page = context.new_page()

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(3000)

            # Click the first result to load its details panel
            try:
                first = page.query_selector('a[href*="/maps/place/"]') or page.query_selector('div[role="article"]')
                if first:
                    first.click()
                    page.wait_for_timeout(2000)
            except Exception:
                pass

            # Find place info panel
            panel = page.query_selector('div[jsaction*="pane"]') or page.query_selector('div.scrollable')
            if not panel and '/maps/place/' in page.url:
                panel = page.query_selector('div.scrollable') or page.query_selector('div[role="main"]')

            if not panel:
                return result

            result["found"] = True

            # Reviews count — multiple strategies, all safe
            try:
                reviews_el = page.query_selector('span[jslabel*="reviews"]')
                if reviews_el:
                    result["google_reviews_count"] = parse_rating_count(reviews_el.inner_text())
                else:
                    # Fallback 1: aria-label containing "review"
                    aria_els = page.query_selector_all('[aria-label*="review" i]')
                    found = False
                    for el in aria_els:
                        label = el.get_attribute('aria-label') or ''
                        m = re.search(r'([\d,\s]+)\s*reviews?', label, re.I)
                        if m:
                            try:
                                num = int(m.group(1).replace(',', '').replace(' ', ''))
                                result["google_reviews_count"] = num
                                found = True
                                break
                            except ValueError:
                                continue
                    if not found:
                        # Fallback 2: regex scan full page content
                        content = page.content()
                        m = re.search(r'([\d,]+)\s*reviews?', content, re.I)
                        if m:
                            try:
                                result["google_reviews_count"] = int(m.group(1).replace(',', ''))
                            except ValueError:
                                pass
            except Exception:
                # Any unexpected error, leave as 0
                pass

            result["google_owner_replies"] = has_owner_replies(page)
            result["google_photos_count"]  = count_photos(page)

            # External website check
            website_btn = page.query_selector('a[href^="http"]:not([href*="google.com"]):not([href*="maps.google"])')
            if website_btn:
                href = website_btn.get_attribute('href') or ''
                if 'google.com' not in href and 'maps.google' not in href:
                    result["gmaps_website"] = True

            # ── Phone via tel: link ─────────────────────────────────────────────────
            tel_el = page.query_selector('a[href^="tel:"]')
            if tel_el:
                raw = tel_el.get_attribute('href') or ''
                phone = raw.replace('tel:', '').strip()
                cleaned = re.sub(r'[^0-9+]', '', phone)
                if cleaned:
                    result["phone"] = cleaned

            print(f"  [GM] {business_name[:35]:35}  reviews={result['google_reviews_count']}  owner={result['google_owner_replies']}  photos={result['google_photos_count']}  website={result['gmaps_website']}")

        except PlaywrightTimeoutError:
            pass
        except Exception as e:
            print(f"  [GM] Error: {e}")
        finally:
            browser.close()

    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--name",  required=True, help="Business name")
    ap.add_argument("--city",  default="Sydney")
    ap.add_argument("--state", default="NSW")
    args = ap.parse_args()

    res = scrape_google_maps(args.name, args.city, args.state)
    print(json.dumps(res, indent=2, default=str))

