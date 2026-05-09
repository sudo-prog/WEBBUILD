#!/usr/bin/env python3
"""
Yellow Pages AU Scraper — updated for 2026 site structure.

Fetches businesses that:
  • Have a visible email address (mailto link) AND
  • Do NOT have an external website listed

Uses Playwright to bypass Cloudflare. Extracts:
  - business_name
  - phone
  - email
  - category
  - city/state
  - address (optional)

Writes JSON per city to raw_leads/yp_<city>_<timestamp>.json
"""
import json, time, sys, asyncio, re, argparse
from pathlib import Path
from typing import List, Dict

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("ERROR: pip install playwright && playwright install chromium")
    sys.exit(1)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

CATEGORIES = ["plumber", "electrician", "builder", "painter", "carpenter",
              "roofer", "air conditioning", "kitchen", "flooring", "solar"]

# YP-specific selectors (2026)
LISTING_SELECTOR = 'div.v-card, div[class*="listing"]'
NAME_SELECTOR     = 'h2.n a.business-name'
PHONE_SELECTOR    = 'div.phones.phone.primary'
ADDR_SELECTOR     = 'div.adr'
EMAIL_SELECTOR    = 'a[href^="mailto:"]'
WEBSITE_SELECTOR  = 'a[href^="http"]'   # will filter out YP domains later


async def scrape_listing_card(card) -> Dict | None:
    """Extract a single listing card, returning a lead dict or None if filtered out."""
    try:
        # Business name
        name_el = await card.query_selector(NAME_SELECTOR)
        name = (await name_el.inner_text()).strip() if name_el else ""
        if not name:
            return None

        # Phone
        phone_el = await card.query_selector(PHONE_SELECTOR)
        phone = (await phone_el.inner_text()).strip() if phone_el else ""

        # Email (must be present)
        email_el = await card.query_selector(EMAIL_SELECTOR)
        email = (await email_el.get_attribute('href')).replace('mailto:','').strip().lower() if email_el else ""
        if not email:
            return None  # no email → not a lead

        # Website check: look for any external link in the card that is NOT a YP internal link
        # Also exclude generic YP/tracking links
        website = None
        link_els = await card.query_selector_all(WEBSITE_SELECTOR)
        for el in link_els:
            href = (await el.get_attribute('href') or "").strip()
            if not href:
                continue
            # Skip internal YP navigation
            if "yellowpages.com.au" in href or "yellow.com.au" in href:
                continue
            # Skip obvious non-business links (privacy, terms, login)
            if any(skip in href.lower() for skip in ['privacy', 'terms', 'login', 'signup', 'account', 'my.yellow']):
                continue
            # Found external site — they have a website
            website = href
            break

        if website:
            return None  # already has website → not a lead

        # Address (optional)
        addr_el = await card.query_selector(ADDR_SELECTOR)
        address = await addr_el.inner_text() if addr_el else ""
        address = address.strip() if address else None

        return {
            "business_name": name,
            "phone": phone,
            "email": email,
            "website": None,
            "address": address,
        }
    except Exception:
        return None


async def scrape_category(page, category: str, location: str) -> List[Dict]:
    """Scrape one category page for a given city/location."""
    url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={location}&pageNumber=1"
    results = []
    try:
        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(2000)

        # Get all listing cards
        cards = await page.query_selector_all(LISTING_SELECTOR)
        if not cards:
            # Debug: save page for analysis
            html = await page.content()
            open(f"/tmp/yp_debug_{category}.html","w").write(html)
            print(f"  [DEBUG] Saved /tmp/yp_debug_{category}.html ({len(html)} bytes)")

        for card in cards:
            lead = await scrape_listing_card(card)
            if lead:
                lead["category"] = category
                results.append(lead)

    except Exception as e:
        print(f"[{category}] error: {e}")
    return results


async def scrape_yellow_pages(city: str, state: str, max_pages: int = 3) -> List[Dict]:
    """
    Scrape multiple pages for a city.
    max_pages: how many result pages to crawl (default 3)
    """
    location = f"{city.title()} {state}"
    all_leads: List[Dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(user_agent=HEADERS["User-Agent"])
        page = await context.new_page()

        for category in CATEGORIES:
            print(f"[{city}] Scraping '{category}'...")
            cat_leads = await scrape_category(page, category, location)
            print(f"  → {len(cat_leads)} leads")
            all_leads.extend(cat_leads)
            # Rate limiting between categories
            await page.wait_for_timeout(1500)

        await browser.close()
    return all_leads


def scrape_yellow_pages_sync(city: str, state: str = "NSW", max_pages: int = 3) -> List[Dict]:
    return asyncio.run(scrape_yellow_pages(city, state, max_pages))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("city", help="City name (e.g. Sydney)")
    parser.add_argument("state", help="State code (e.g. NSW)")
    parser.add_argument("--pages", type=int, default=3, help="Number of pages to scrape per category")
    args = parser.parse_args()

    leads = scrape_yellow_pages_sync(args.city, args.state, max_pages=args.pages)
    out = Path(f"/home/thinkpad/Projects/active/WEBBUILD/supabase_australia/01_Raw_Data/raw_leads/yellow_pages/yp_{args.city.lower()}_{int(time.time())}.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(leads, indent=2))
    print(f"✓ {len(leads)} leads → {out}")
