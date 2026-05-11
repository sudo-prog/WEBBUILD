#!/usr/bin/env python3
"""
enrich_contacts_free.py — Per-business contact enrichment using free sources only.

STRATEGY:
  Instead of bulk-scraping a category (e.g. "all plumbers in Sydney"), we do
  targeted lookups PER BUSINESS NAME already known from the ABN bulk extract.
  This is orders of magnitude less likely to be blocked.

FREE SOURCES (in priority order):
  1. ABN Lookup API  — free, official, returns phone for registered businesses
  2. DuckDuckGo HTML search — no API key, returns organic results we parse for
     phone/email in the snippet/URL without loading the business site
  3. White Pages AU  — targeted per-name search, rate-limited politely
  4. True Local AU   — fallback directory, lower traffic = less blocking

USAGE:
  # Enrich a JSONL file of ABN trade leads (output of abn_trade_filter.py)
  python enrich_contacts_free.py \
      --input /home/thinkpad/data/abn/leads/trades_part01.jsonl \
      --output /home/thinkpad/data/abn/leads/enriched_part01.jsonl \
      --limit 500 \
      --delay 2.5

  # Resume interrupted run (skips already-enriched ABNs)
  python enrich_contacts_free.py \
      --input trades_part01.jsonl \
      --output enriched_part01.jsonl \
      --resume

  # Dry run — show what would be looked up
  python enrich_contacts_free.py --input trades_part01.jsonl --dry-run --limit 10
"""

import json
import re
import sys
import time
import random
import argparse
import logging
import os
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timezone
from urllib.parse import quote_plus, urljoin
import urllib.request
import urllib.error
import html
import whois  # for domain email extraction

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("enrich")



def google_search(query: str) -> List[Dict]:
    """Search Google via web scraping (free, no API key)."""
    # Use a simple Google search URL
    url = f"https://www.google.com/search?q={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        results = []
        # Parse Google results (simplified)
        # This is a placeholder - actual implementation would parse Google's HTML
        return results
    except Exception as e:
        log.debug(f"Google search failed: {e}")
        return []

def enrich_via_google(business_name: str, city: str, state: str, search_engines: List[str]) -> Dict:
    query = build_dork_query(business_name, city, state)
    log.debug(f"Google query: {query}")
    results = google_search(query)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "google_snippet"
    if email:
        out["email"] = email
        out["email_source"] = "google_snippet"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out




def bing_search(query: str) -> List[Dict]:
    """Search Bing via web scraping (free, no API key)."""
    url = f"https://www.bing.com/search?q={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36"
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        results = []
        # Parse Bing results (placeholder)
        return results
    except Exception as e:
        log.debug(f"Bing search failed: {e}")
        return []

def enrich_via_bing(business_name: str, city: str, state: str, search_engines: List[str]) -> Dict:
    query = build_dork_query(business_name, city, state)
    log.debug(f"Bing query: {query}")
    results = bing_search(query)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "bing_snippet"
    if email:
        out["email"] = email
        out["email_source"] = "bing_snippet"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out



def google_search(query: str) -> List[Dict]:
    """Search Google via web scraping (free, no API key)."""
    # Use a simple Google search URL
    url = f"https://www.google.com/search?q={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        # Parse Google results
        results = []
        # Simple parsing of Google results - may break if Google changes layout
        # Look for div.g (organic results)
        soup = BeautifulSoup(body, 'html.parser')
        for g in soup.find_all('div', class_='g')[:8]:
            r = {}
            # Title
            title_elem = g.find('h3')
            if title_elem:
                r['title'] = title_elem.get_text()
            else:
                continue
            # Link
            link_elem = g.find('a', href=True)
            if link_elem:
                r['url'] = link_elem['href']
            else:
                continue
            # Snippet
            snippet_elem = g.find('span', {'class': 'st'})
            if snippet_elem:
                r['snippet'] = snippet_elem.get_text()
            else:
                r['snippet'] = ''
            r['domain'] = urllib.parse.urlparse(r['url']).netloc if r['url'] else ''
            results.append(r)
        return results
    except Exception as e:
        log.debug(f"Google search failed: {e}")
        return []

def enrich_via_google(business_name: str, city: str, state: str, search_engines: List[str]) -> Dict:
    query = build_dork_query(business_name, city, state)
    log.debug(f"Google query: {query}")
    results = google_search(query)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "google_snippet"
    if email:
        out["email"] = email
        out["email_source"] = "google_snippet"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out




def bing_search(query: str) -> List[Dict]:
    """Search Bing via web scraping (free, no API key)."""
    url = f"https://www.bing.com/search?q={quote_plus(query)}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        results = []
        # Parse Bing results (simplified)
        soup = BeautifulSoup(body, 'html.parser')
        for g in soup.find_all('li', class_='b_algo')[:8]:
            r = {}
            title_elem = g.find('h2').find('a') if g.find('h2') else None
            if title_elem:
                r['title'] = title_elem.get_text()
            else:
                continue
            link_elem = g.find('a', href=True)
            if link_elem:
                r['url'] = link_elem['href']
            else:
                continue
            snippet_elem = g.find('div', {'class': 'b_caption'})
            if snippet_elem:
                r['snippet'] = snippet_elem.get_text()
            else:
                r['snippet'] = ''
            r['domain'] = urllib.parse.urlparse(r['url']).netloc if r['url'] else ''
            results.append(r)
        return results
    except Exception as e:
        log.debug(f"Bing search failed: {e}")
        return []

def enrich_via_bing(business_name: str, city: str, state: str, search_engines: List[str]) -> Dict:
    query = build_dork_query(business_name, city, state)
    log.debug(f"Bing query: {query}")
    results = bing_search(query)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "bing_snippet"
    if email:
        out["email"] = email
        out["email_source"] = "bing_snippet"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

# Rotate user agents to reduce fingerprinting
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# Regex patterns for contact extraction from raw HTML snippets
PHONE_RE = re.compile(
    r"(?<!\d)"
    r"(?:(?:\+?61|0)[\s\-.]?)"          # country code or leading 0
    r"(?:[23478][\d\s\-.]{7,9}|"        # landline
    r"4[\d\s\-.]{8,10})"                # mobile
    r"(?!\d)",
    re.ASCII,
)
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
)

# Domains that are NOT business websites (filter from dork results)
JUNK_DOMAINS = {
    "yellowpages.com.au", "white-pages.com.au", "truelocal.com.au",
    "localsearch.com.au", "startlocal.com.au", "yelp.com.au",
    "google.com", "facebook.com", "linkedin.com", "instagram.com",
    "twitter.com", "youtube.com", "wikipedia.org", "seek.com.au",
    "abn.business.gov.au", "abr.gov.au", "asic.gov.au",
}

# ABN Lookup public API (no key required, rate-limit: ~10 req/min)
ABN_API_URL = "https://abr.business.gov.au/ABN/View?abn={abn}&format=json"

# DuckDuckGo HTML endpoint (no JS, no login)
DDG_URL = "https://html.duckduckgo.com/html/?q={query}"

# White Pages AU search
WP_URL = "https://www.whitepages.com.au/search/person/?name={name}&state={state}"

# True Local search
TL_URL = "https://www.truelocal.com.au/find/{category}/{city}"


# ─────────────────────────────────────────────────────────────────────────────
# HTTP HELPER
# ─────────────────────────────────────────────────────────────────────────────

def http_get(url: str, timeout: int = 12, retries: int = 2) -> Optional[str]:
    """Fetch URL with rotating UA and basic retry logic. Returns HTML or None."""
    ua = random.choice(USER_AGENTS)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-AU,en;q=0.9",
            "Accept-Encoding": "identity",
            "DNT": "1",
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                charset = "utf-8"
                ct = resp.headers.get("Content-Type", "")
                if "charset=" in ct:
                    charset = ct.split("charset=")[-1].strip()
                return resp.read().decode(charset, errors="replace")
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 30 + random.uniform(5, 20)
                log.warning(f"Rate limited on {url[:60]}… sleeping {wait:.0f}s")
                time.sleep(wait)
            elif e.code in (403, 503):
                log.debug(f"HTTP {e.code} on attempt {attempt+1}: {url[:60]}")
                time.sleep(3 * (attempt + 1))
            else:
                log.debug(f"HTTP error {e.code}: {url[:60]}")
                break
        except Exception as e:
            log.debug(f"Request error: {e}")
            time.sleep(2 * (attempt + 1))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CONTACT EXTRACTION FROM HTML SNIPPETS
# ─────────────────────────────────────────────────────────────────────────────

def clean_phone(raw: str) -> Optional[str]:
    """Normalise to 10-digit Australian number or None if invalid."""
    digits = re.sub(r"[^\d]", "", raw)
    # Remove country code prefix
    if digits.startswith("61") and len(digits) == 11:
        digits = "0" + digits[2:]
    if len(digits) == 10 and digits[0] == "0":
        # Reject obvious placeholders
        if digits in ("0000000000", "1234567890") or digits[1:] == "000000000":
            return None
        return digits
    return None


def extract_contacts(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Pull first valid phone and email from any text blob."""
    phone = None
    email = None

    # Phone
    for m in PHONE_RE.finditer(text):
        candidate = clean_phone(m.group())
        if candidate:
            phone = candidate
            break

    # Email — skip generic/junk addresses
    for m in EMAIL_RE.finditer(text):
        addr = m.group().lower()
        if any(skip in addr for skip in ["example.", "test@", "noreply", "no-reply", "@sentry", "@github"]):
            continue
        email = addr
        break

    return phone, email


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1: ABN LOOKUP API (Free, Official)
# ─────────────────────────────────────────────────────────────────────────────

def lookup_abn_api(abn: str) -> Dict:
    """
    Hit the free ABN Lookup API. Returns phone if the business has listed one.
    The public endpoint doesn't require an API key for basic queries.
    """
    if not abn:
        return {}
    abn_digits = re.sub(r"[^\d]", "", str(abn))
    if len(abn_digits) != 11:
        return {}

    url = ABN_API_URL.format(abn=abn_digits)
    html_body = http_get(url, timeout=10)
    if not html_body:
        return {}

    result = {}
    # The JSON response embeds contact details
    try:
        # Strip any JSONP wrapper
        body = re.sub(r"^[^{]*", "", html_body).strip().rstrip(";)")
        data = json.loads(body)
        entity = data.get("EntityName", "") or data.get("MainName", {}).get("OrganisationName", "")
        if entity:
            result["abn_entity_name"] = entity.strip()
        status = data.get("EntityStatus", {}).get("EntityStatusCode", "")
        result["abn_status_confirmed"] = status == "ACT"
        # Phone sometimes included
        phone_raw = (
            data.get("Telephone", "")
            or data.get("Phone", "")
            or data.get("BusinessPhone", "")
        )
        if phone_raw:
            phone = clean_phone(str(phone_raw))
            if phone:
                result["phone"] = phone
                result["phone_source"] = "abn_api"
    except json.JSONDecodeError:
        # Fall back to regex extraction from raw response
        phone, email = extract_contacts(html_body)
        if phone:
            result["phone"] = phone
            result["phone_source"] = "abn_api_regex"
        if email:
            result["email"] = email
            result["email_source"] = "abn_api_regex"

    log.debug(f"ABN API → {result}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2: DUCKDUCKGO DORK (Free, No API Key)
# ─────────────────────────────────────────────────────────────────────────────

def build_dork_query(business_name: str, city: str, state: str) -> str:
    """
    Construct a targeted dork query that finds contact pages specifically.
    Avoids directory aggregators that are just noise.

    Strategy: search for the business name + city + contact signals.
    Exclude known directory sites so we get the actual business site or
    a snippet containing their real number.
    """
    # Clean the name
    name = re.sub(r"\b(pty|ltd|limited|co|trust|group)\b", "", business_name, flags=re.I).strip()
    name = re.sub(r"\s+", " ", name)

    # Build exclusion list
    excludes = " ".join(f"-site:{d}" for d in [
        "yellowpages.com.au", "truelocal.com.au", "localsearch.com.au",
        "yelp.com.au", "startlocal.com.au",
    ])

    query = f'"{name}" {city} {state} contact phone {excludes}'
    return query


def duckduckgo_search(query: str) -> List[Dict]:
    """
    Search DuckDuckGo's HTML endpoint (no JS required) and return
    structured result snippets. Each result has: title, url, snippet.
    """
    url = DDG_URL.format(query=quote_plus(query))
    body = http_get(url, timeout=15)
    if not body:
        return []

    results = []
    # Parse result blocks — DDG HTML has <div class="result"> wrappers
    # We use simple regex since lxml/bs4 may not be installed
    blocks = re.findall(
        r'<div[^>]+class="[^"]*result[^"]*"[^>]*>(.*?)</div>\s*</div>',
        body,
        re.DOTALL,
    )
    # Also try the simpler result__body pattern
    if not blocks:
        blocks = re.findall(
            r'class="result__body">(.*?)</div>',
            body,
            re.DOTALL,
        )

    for block in blocks[:8]:
        # Extract URL
        url_match = re.search(r'href="(https?://[^"]+)"', block)
        url_val = url_match.group(1) if url_match else ""
        # Skip junk domains
        domain = re.sub(r"https?://([^/]+).*", r"\1", url_val).lower().lstrip("www.")
        if any(jd in domain for jd in JUNK_DOMAINS):
            continue
        # Extract title
        title = re.sub(r"<[^>]+>", "", re.search(r"<a[^>]*>(.*?)</a>", block, re.DOTALL).group(1) if re.search(r"<a[^>]*>(.*?)</a>", block, re.DOTALL) else "")
        # Extract snippet
        snippet_match = re.search(r'class="result__snippet[^"]*"[^>]*>(.*?)</(?:a|div|span)>', block, re.DOTALL)
        snippet = html.unescape(re.sub(r"<[^>]+>", " ", snippet_match.group(1) if snippet_match else "")).strip()

        if snippet or url_val:
            results.append({
                "title": html.unescape(title.strip()),
                "url": url_val,
                "snippet": snippet,
                "domain": domain,
            })

    return results


def enrich_via_duckduckgo(business_name: str, city: str, state: str) -> Dict:
    """
    Run a targeted dork, parse all snippets for phone/email.
    Returns best phone and email found, with source attribution.
    """
    query = build_dork_query(business_name, city, state)
    log.debug(f"DDG query: {query}")

    results = duckduckgo_search(query)
    if not results:
        return {}

    # Combine all snippets into one search blob
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)

    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "duckduckgo_snippet"
    if email:
        out["email"] = email
        out["email_source"] = "duckduckgo_snippet"

    # Record first non-junk URL as potential website
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            # Perform WHOIS lookup to extract email from domain registration
            try:
                import whois
                w = whois.whois(r["url"])
                if w.emails:
                    # Extract the first email that looks like a business email
                    for email in w.emails:
                        email = email.lower().strip()
                        if email and not any(bad in email for bad in ["example", "test", "noreply", "@sentry", "@github"]):
                            out["email"] = email
                            out["email_source"] = "whois"
                            break
            except Exception:
                pass  # WHOIS may fail or not be installed
            break

    return out


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3: WHITE PAGES AU (Targeted per-name)
# ─────────────────────────────────────────────────────────────────────────────

def enrich_via_whitepages(business_name: str, state: str) -> Dict:
    """
    Search White Pages AU by business name. Much less traffic than
    bulk category scraping, so far less likely to get blocked.
    """
    # Clean name for URL
    name_slug = re.sub(r"[^\w\s]", "", business_name).strip().replace(" ", "+")
    url = f"https://www.whitepages.com.au/search/business/?name={name_slug}&state={state}"
    body = http_get(url, timeout=12)
    if not body:
        return {}

    phone, email = extract_contacts(body)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "whitepages"
    if email:
        out["email"] = email
        out["email_source"] = "whitepages"
    return out


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4: TRUE LOCAL (Targeted per-name)
# ─────────────────────────────────────────────────────────────────────────────

def enrich_via_truelocal(business_name: str, city: str) -> Dict:
    """Search True Local by exact business name."""
    name_slug = re.sub(r"[^\w\s]", "", business_name).strip().replace(" ", "-").lower()
    city_slug = city.lower().replace(" ", "-")
    # True Local search URL
    url = f"https://www.truelocal.com.au/find/{quote_plus(business_name)}/{city_slug}"
    body = http_get(url, timeout=12)
    if not body:
        return {}

    phone, email = extract_contacts(body)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "truelocal"
    if email:
        out["email"] = email
        out["email_source"] = "truelocal"
    return out


# ─────────────────────────────────────────────────────────────────────────────


def yellow_pages_search(business_name: str, city: str, state: str) -> List[Dict]:
    """Search Yellow Pages via web scraping (free, no API key)."""
    # Yellow Pages AU search URL
    url = f"https://www.yellowpages.com.au/search/listings?clue={quote_plus(business_name)}&location={city}&state={state}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        results = []
        # Parse Yellow Pages results (simplified)
        soup = BeautifulSoup(body, 'html.parser')
        for item in soup.find_all('div', class_='listing'):  # Adjust class as needed
            r = {}
            title_elem = item.find('h2')
            if title_elem:
                r['title'] = title_elem.get_text().strip()
            else:
                continue
            phone_elem = item.find('button', {'class': 'contact-phone'})
            if phone_elem:
                r['phone'] = phone_elem.get_text().strip()
            else:
                continue
            # Extract snippet if available
            snippet_elem = item.find('div', {'class': 'description'})
            r['snippet'] = snippet_elem.get_text().strip() if snippet_elem else ''
            # Extract URL if available
            link_elem = item.find('a', href=True)
            if link_elem:
                r['url'] = link_elem['href']
                r['domain'] = urllib.parse.urlparse(r['url']).netloc if r['url'] else ''
            results.append(r)
        return results
    except Exception as e:
        log.debug(f"Yellow Pages search failed: {e}")
        return []

def enrich_via_yellowpages(business_name: str, city: str, state: str, search_engines: List[str]) -> Dict:
    query = f'"{business_name}" {city} {state}'
    log.debug(f"Yellow Pages query: {query}")
    results = yellow_pages_search(business_name, city, state)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "yellowpages"
    if email:
        out["email"] = email
        out["email_source"] = "yellowpages"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out



def true_local_search(business_name: str, city: str) -> List[Dict]:
    """Search True Local via web scraping (free, no API key)."""
    url = f"https://www.truelocal.com.au/find/{quote_plus(business_name)}/{city}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-AU,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        body = http_get(url, timeout=15)
        if not body:
            return []
        results = []
        soup = BeautifulSoup(body, 'html.parser')
        for item in soup.find_all('div', class_='listing'):  # Adjust class as needed
            r = {}
            title_elem = item.find('h2')
            if title_elem:
                r['title'] = title_elem.get_text().strip()
            else:
                continue
            phone_elem = item.find('span', {'class': 'phone'})
            if phone_elem:
                r['phone'] = phone_elem.get_text().strip()
            else:
                continue
            snippet_elem = item.find('div', {'class': 'description'})
            r['snippet'] = snippet_elem.get_text().strip() if snippet_elem else ''
            link_elem = item.find('a', href=True)
            if link_elem:
                r['url'] = link_elem['href']
                r['domain'] = urllib.parse.urlparse(r['url']).netloc if r['url'] else ''
            results.append(r)
        return results
    except Exception as e:
        log.debug(f"True Local search failed: {e}")
        return []

def enrich_via_truelocal(business_name: str, city: str, search_engines: List[str]) -> Dict:
    query = f'"{business_name}" {city}'
    log.debug(f"True Local query: {query}")
    results = true_local_search(business_name, city)
    if not results:
        return {}
    all_text = " ".join(r["snippet"] + " " + r["url"] for r in results)
    phone, email = extract_contacts(all_text)
    out = {}
    if phone:
        out["phone"] = phone
        out["phone_source"] = "truelocal"
    if email:
        out["email"] = email
        out["email_source"] = "truelocal"
    for r in results:
        if r["url"] and r["domain"] not in JUNK_DOMAINS:
            out["website_candidate"] = r["url"]
            out["website_domain"] = r["domain"]
            break
    return out

# ORCHESTRATION: ENRICH ONE LEAD
# ─────────────────────────────────────────────────────────────────────────────

def enrich_lead(lead: Dict, delay: float = 2.5) -> Dict:
    """
    Try all free sources in order until we have phone + email.
    Returns the lead dict with enrichment fields added.
    Stops early if both phone and email are found.
    """
    name = (lead.get("trading_name") or lead.get("business_name") or "").strip()
    city = (lead.get("city") or "").strip()
    state = (lead.get("address_state") or lead.get("state") or "").strip().upper()
    abn = lead.get("abn", "")

    found_phone: Optional[str] = lead.get("phone")   # may already have one
    found_email: Optional[str] = lead.get("email")
    sources_tried: List[str] = []

    def _done() -> bool:
        return bool(found_phone and found_email)

    # ── SOURCE 1: ABN Lookup API ──────────────────────────────────────────────
    if not _done() and abn:
        log.debug(f"[ABN API] {name}")
        abn_data = lookup_abn_api(abn)
        sources_tried.append("abn_api")
        if abn_data.get("phone") and not found_phone:
            found_phone = abn_data["phone"]
        if abn_data.get("email") and not found_email:
            found_email = abn_data["email"]
        if abn_data.get("abn_entity_name"):
            lead["abn_entity_name_confirmed"] = abn_data["abn_entity_name"]
        _jitter(delay)

    # ── SOURCE 2: DuckDuckGo dork ─────────────────────────────────────────────
    if not _done() and name and city:
        log.debug(f"[DDG dork] {name}")
        ddg_data = enrich_via_duckduckgo(name, city, state)
        sources_tried.append("duckduckgo")
        if ddg_data.get("phone") and not found_phone:
            found_phone = ddg_data["phone"]
        if ddg_data.get("email") and not found_email:
            found_email = ddg_data["email"]
        if ddg_data.get("website_candidate") and not lead.get("website"):
            lead["website_candidate"] = ddg_data["website_candidate"]
        _jitter(delay)

    # ── SOURCE 3: White Pages ─────────────────────────────────────────────────
    if not _done() and name and state:
        log.debug(f"[White Pages] {name}")
        wp_data = enrich_via_whitepages(name, state)
        sources_tried.append("whitepages")
        if wp_data.get("phone") and not found_phone:
            found_phone = wp_data["phone"]
        if wp_data.get("email") and not found_email:
            found_email = wp_data["email"]
        _jitter(delay)

    # ── SOURCE 4: True Local ──────────────────────────────────────────────────
    if not _done() and name and city:
        log.debug(f"[True Local] {name}")
        tl_data = enrich_via_truelocal(name, city)
        sources_tried.append("truelocal")
        if tl_data.get("phone") and not found_phone:
            found_phone = tl_data["phone"]
        if tl_data.get("email") and not found_email:
            found_email = tl_data["email"]
        _jitter(delay * 0.5)  # True Local is usually fast

    # ── Merge results ─────────────────────────────────────────────────────────
    lead["phone"] = found_phone
    lead["email"] = found_email
    lead["enriched_at"] = datetime.now(timezone.utc).isoformat()
    lead["enrichment_sources"] = sources_tried
    lead["enrichment_complete"] = _done()

    result_str = f"✓ phone={found_phone or '—'}  email={found_email or '—'}"
    log.info(f"  {name[:40]:40}  {result_str}")

    return lead


def _jitter(base: float):
    """Sleep with ±30% jitter to look more human."""
    time.sleep(base * random.uniform(0.7, 1.3))


# ─────────────────────────────────────────────────────────────────────────────
# RESUME SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

def load_already_enriched(output_path: Path) -> set:
    """Return set of ABNs already written to the output file."""
    done = set()
    if not output_path.exists():
        return done
    with output_path.open() as f:
        for line in f:
            try:
                rec = json.loads(line)
                abn = rec.get("abn")
                if abn:
                    done.add(str(abn))
            except Exception:
                pass
    return done


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Enrich ABN trade leads with phone/email from free sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Enrich 500 leads at 2.5s delay between lookups
  python enrich_contacts_free.py \\
      --input trades_part01.jsonl \\
      --output enriched_part01.jsonl \\
      --limit 500 --delay 2.5

  # Resume an interrupted run
  python enrich_contacts_free.py \\
      --input trades_part01.jsonl \\
      --output enriched_part01.jsonl \\
      --resume

  # Dry run to preview queries
  python enrich_contacts_free.py --input trades_part01.jsonl --dry-run --limit 5

  # Enrich all parts in a directory
  for f in /home/thinkpad/data/abn/leads/trades_part*.jsonl; do
      python enrich_contacts_free.py --input "$f" \\
          --output "$(dirname $f)/enriched_$(basename $f)" \\
          --resume --delay 3
  done
        """
    )
    p.add_argument("--input",   required=True,  type=Path, help="Input JSONL of ABN trade leads")
    p.add_argument("--output",  required=True,  type=Path, help="Output JSONL with enriched contacts")
    p.add_argument("--limit",   type=int,        default=0,   help="Max leads to process (0 = all)")
    p.add_argument("--delay",   type=float,      default=2.5, help="Base delay between requests (seconds)")
    p.add_argument("--resume",  action="store_true",          help="Skip ABNs already in output file")
    p.add_argument("--dry-run", action="store_true",          help="Print queries only, no HTTP calls")
    p.add_argument("--city",    default=None,                 help="Filter by city (e.g. Sydney)")
    p.add_argument("--state",   default=None,                 help="Filter by state (e.g. NSW)")
    p.add_argument("--debug",   action="store_true",          help="Verbose HTTP debug logging")
    args = p.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    if not args.input.exists():
        log.error(f"Input not found: {args.input}")
        sys.exit(1)

    # Load already-done ABNs for resume
    done_abns: set = set()
    if args.resume:
        done_abns = load_already_enriched(args.output)
        log.info(f"Resume mode: {len(done_abns)} ABNs already enriched, skipping")

    # Open output in append mode (supports resume)
    out_mode = "a" if args.resume else "w"
    total = processed = enriched_count = 0

    log.info(f"Reading from {args.input}")
    log.info(f"Writing to  {args.output}  (mode={out_mode})")
    if args.delay < 1.5:
        log.warning("Delay < 1.5s risks getting rate-limited. Recommend >= 2.5s")

    with args.input.open() as fin, args.output.open(out_mode) as fout:
        for line in fin:
            if args.limit and total >= args.limit:
                break
            line = line.strip()
            if not line:
                continue

            try:
                lead = json.loads(line)
            except json.JSONDecodeError:
                continue

            total += 1

            # City / state filter
            if args.city and lead.get("city", "").lower() != args.city.lower():
                continue
            if args.state and (lead.get("address_state") or lead.get("state", "")).upper() != args.state.upper():
                continue

            # Resume: skip if already done
            abn = str(lead.get("abn", ""))
            if args.resume and abn and abn in done_abns:
                continue

            name = lead.get("trading_name") or lead.get("business_name") or ""
            log.info(f"[{processed+1}] {name[:45]}")

            if args.dry_run:
                query = build_dork_query(name, lead.get("city",""), lead.get("address_state",""))
                print(f"  DDG query: {query}")
                processed += 1
                continue

            enriched = enrich_lead(lead, delay=args.delay)
            fout.write(json.dumps(enriched, ensure_ascii=False) + "\n")
            fout.flush()  # write immediately so progress isn't lost on crash
            processed += 1

            if enriched.get("phone") or enriched.get("email"):
                enriched_count += 1

    # Summary
    print(f"\n{'='*55}")
    print(f"  Processed : {processed}")
    print(f"  Enriched  : {enriched_count} ({enriched_count/max(processed,1)*100:.1f}% contact rate)")
    print(f"  Output    : {args.output}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
