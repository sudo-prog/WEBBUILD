#!/usr/bin/env python3
import json, sys
from pathlib import Path
from playwright.sync_api import sync_playwright

CATEGORIES = ["plumber"]
OUT_DIR = Path("/home/thinkpad/Projects/supabase_australia/raw_leads")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def scrape_city(city, state, max_pages=1):
    location = f"{city.title()} {state}"
    all_leads = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36")
        for category in CATEGORIES:
            url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={location}&pageNumber=1"
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2000)
            cards = page.query_selector_all('div.v-card')
            print(f"  {category}: {len(cards)} listing cards")
            for card in cards:
                try:
                    name_el = card.query_selector('h2.n a.business-name')
                    name = name_el.inner_text().strip() if name_el else ""
                    if not name: continue
                    phone_el = card.query_selector('div.phones.phone.primary')
                    phone = phone_el.inner_text().strip() if phone_el else ""
                    has_website = False
                    for el in card.query_selector_all('a[href^="http"]'):
                        href = (el.get_attribute('href') or '').lower()
                        if any(x in href for x in ['yellowpages.com.au','yellow.com.au','thryv.com.au','directoryselect.com']): continue
                        if any(skip in href for skip in ['privacy','terms','login','account','my.yellow','facebook','twitter']): continue
                        has_website = True
                        break
                    all_leads.append({
                        "business_name": name,
                        "phone": phone,
                        "has_website": has_website,
                        "category": category,
                        "city": city,
                        "state": state
                    })
                except Exception:
                    continue
            page.wait_for_timeout(1000)
        browser.close()
    return all_leads

leads = scrape_city("sydney", "NSW", max_pages=1)
no_web = [l for l in leads if not l['has_website']]
print(f"Total scraped: {len(leads)} | No website: {len(no_web)}")
out_path = OUT_DIR / "yp_test_sydney_plumber.json"
out_path.write_text(json.dumps(leads, indent=2))
print(f"Saved to {out_path}")
print("Sample no-website leads:")
for l in no_web[:5]:
    print(f"  {l['business_name']} | {l['phone']}")
