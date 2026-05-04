#!/usr/bin/env python3
import json, sys
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(user_agent="Mozilla/5.0")
    url = "https://www.yellowpages.com.au/search/listings?clue=plumber&locationClue=Sydney NSW&pageNumber=1"
    page.goto(url, wait_until="networkidle", timeout=30000)
    page.wait_for_timeout(3000)
    # Grab the first v-card and print its innerHTML
    card = page.query_selector('div.v-card')
    if card:
        html = card.inner_html()
        print(html[:5000])
    else:
        print("NO CARD FOUND")
        print(page.content()[:1000])
    browser.close()
