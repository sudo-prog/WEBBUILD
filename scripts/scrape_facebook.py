#!/usr/bin/env python3
"""
Facebook Business Page Scraper — Playwright-powered (simplified).

Extracts:
  - facebook_last_post (most recent post date, if visible)
  - facebook_about     (page excerpt from meta description or og:description)
  - fb_url             (external website link present?)
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

def parse_relative_date(text: str) -> Optional[datetime]:
    text = (text or '').strip().lower()
    if not text: return None
    now = datetime.now()
    m = re.match(r'(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago', text)
    if m:
        num, unit = int(m.group(1)), m.group(2)
        deltas = {'second': {'seconds': num}, 'minute': {'minutes': num}, 'hour': {'hours': num},
                  'day': {'days': num}, 'week': {'days': num*7}, 'month': {'days': num*30}, 'year': {'days': num*365}}
        try: return now - timedelta(**deltas[unit[:3]])
        except: pass
    # Absolute dates: "Jan 15", "15 Jan 2024"
    for fmt in ["%b %d", "%d %b", "%b %d, %Y", "%d %b %Y", "%B %d %Y"]:
        try: return datetime.strptime(text[:20], fmt)
        except: continue
    return None

def scrape_facebook(business_name: str) -> Dict:
    result = {
        "found": False,
        "business_name": business_name,
        "phone": "",
        "suburb": "",
        "facebook_last_post": None,
        "facebook_about": "",
        "fb_url": False,
    }

    query = business_name.replace(' ', '+')
    search_url = f"https://www.facebook.com/search/pages/?q={query}"

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

            # Dismiss login prompt if visible
            for sel in ['button:has-text("Not Now")', 'button:has-text("Skip")']:
                btn = page.query_selector(sel)
                if btn:
                    btn.click()
                    page.wait_for_timeout(1000)

            # Find first page result
            page_link = page.query_selector('a[href*="/pages/"]') or page.query_selector('a[href*="/business/"]')
            if not page_link and '/pages/' in page.url:
                page_link = page  # already on page

            if page_link and page_link != page:
                page_link.click()
                page.wait_for_timeout(3000)

            result["found"] = True

            # ── About: meta description ───────────────────────────────────────────
            desc_el = page.query_selector('meta[name="description"]') or page.query_selector('meta[property="og:description"]')
            if desc_el:
                about = desc_el.get_attribute('content') or ''
                result["facebook_about"] = about.strip()[:500]

            # ── Last post ─────────────────────────────────────────────────────────
            timestamps = page.query_selector_all('abbr[title], span:has-text("hrs"), span:has-text("min"), span:has-text("Just")')
            latest = None
            for ts in timestamps[:5]:
                txt = ts.inner_text().strip()
                dt = parse_relative_date(txt)
                if dt and (latest is None or dt > latest):
                    latest = dt
            result["facebook_last_post"] = latest.isoformat() if latest else None

            # ── Website ────────────────────────────────────────────────────────────
            links = page.query_selector_all('a[href^="http"]:not([href*="facebook.com"]):not([href*="fbcdn.net"])')
            for lnk in links:
                href = lnk.get_attribute('href') or ''
                if 'facebook.com' not in href and 'fbcdn.net' not in href:
                    result["fb_url"] = True
                    break

            print(f"  [FB] {business_name[:35]:35}  last_post={result['facebook_last_post'] is not None}  about_len={len(result['facebook_about'])}  website={result['fb_url']}")

        except PlaywrightTimeoutError:
            pass
        except Exception as e:
            print(f"  [FB] Error: {e}")
        finally:
            browser.close()

    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", required=True, help="Business name")
    args = ap.parse_args()
    res = scrape_facebook(args.name)
    print(json.dumps(res, indent=2, default=str))
