#!/usr/bin/env python3
"""YP scraper — requests-based fallback + Playwright if needed."""
import sys, json, re, requests
from pathlib import Path
from bs4 import BeautifulSoup

def scrape_requests(city, state, category):
    url = f"https://www.yellowpages.com.au/search/listings?clue={category}&locationClue={city}+{state}"
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
    resp = requests.get(url, headers=headers, timeout=30)
    print(f"HTTP {resp.status_code}, len={len(resp.text)}")
    soup = BeautifulSoup(resp.text, "html.parser")
    results = []
    # YP AU uses article.card or div.result-card
    cards = soup.select("article.card, div.result-card, div.search-result, div.listing-item")
    print(f"Found {len(cards)} cards")
    for card in cards:
        try:
            name_el = card.select_one("h3 a, .listing-name a, a[href*='/listing/']")
            name = name_el.get_text(strip=True) if name_el else ""
            if not name or re.search(r'(?i)^(list$|listing)', name): continue
            phone_el = card.select_one(".click-to-call, .phone, a[href^='tel:']")
            phone = phone_el.get_text(strip=True) if phone_el else ""
            if not phone and phone_el:
                href = phone_el.get("href") or ""
                if href.startswith("tel:"): phone = href.replace("tel:","")
            addr_el = card.select_one(".address, [data-testid='address']")
            address = addr_el.get_text(strip=True) if addr_el else ""
            web_el = card.select_one(".website a")
            website = web_el.get("href") if web_el else ""
            if name and (phone or address):
                results.append({"business_name":name,"phone":phone,"address":address,"website":website,"category":category,"city":city,"state":state,"source":"yellowpages.com.au"})
        except Exception as e:
            pass
    return results

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python scrap_yp_fixed.py <city> <state> <category>")
        sys.exit(1)
    leads = scrape_requests(sys.argv[1], sys.argv[2], sys.argv[3])
    out = Path("yp_" + sys.argv[1].lower() + "_" + sys.argv[3] + ".json")
    out.write_text(json.dumps(leads, indent=2))
    print(f"Saved {len(leads)} leads to {out}")
