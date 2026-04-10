#!/usr/bin/env python3
"""
INTEL Scraper — Sedgwick County Lead Intelligence
==================================================
Runs daily via GitHub Actions. Pulls from:
  1. Sedgwick County Treasurer — Delinquent Real Estate Tax List
  2. Sedgwick County Treasurer — Tax Foreclosure Auction List
  3. 18th District Court     — Probate Daily Docket
  4. Kansas DOR              — State Tax Warrants (Sedgwick County)

Outputs: /data/leads.json (auto-committed by the workflow)
"""

import json
import re
import hashlib
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
log = logging.getLogger('intel')

# ── Output path ──────────────────────────────────────────────────────────────
OUTPUT = Path(__file__).parent.parent / 'data' / 'leads.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def make_id(*parts):
    """Deterministic ID from content so duplicates don't pile up across runs."""
    raw = '|'.join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — Sedgwick County Delinquent Real Estate Taxes
# URL: https://ssc.sedgwickcounty.org/TaxInfoWebApp/DelinquentListing.aspx
# This is a public list — no login required.
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_delinquent():
    leads = []
    url = 'https://ssc.sedgwickcounty.org/TaxInfoWebApp/DelinquentListing.aspx'
    log.info(f'Scraping tax delinquent: {url}')

    try:
        # The page may require a POST after accepting a disclaimer
        session = requests.Session()

        # Step 1: Load the intro/disclaimer page
        intro_url = 'https://ssc.sedgwickcounty.org/propertytax/delinquenciesintro.aspx'
        r = session.get(intro_url, headers=HEADERS, timeout=20)
        r.raise_for_status()

        # Step 2: Accept disclaimer (POST)
        soup = BeautifulSoup(r.text, 'html.parser')
        viewstate = soup.find('input', {'id': '__VIEWSTATE'})
        eventval   = soup.find('input', {'id': '__EVENTVALIDATION'})

        post_data = {
            '__VIEWSTATE':       viewstate['value'] if viewstate else '',
            '__EVENTVALIDATION': eventval['value']  if eventval  else '',
            'btnAgree':          'I Agree',
        }

        r2 = session.post(intro_url, data=post_data, headers=HEADERS, timeout=20)
        r2.raise_for_status()

        # Step 3: Fetch the actual listing
        r3 = session.get(url, headers=HEADERS, timeout=20)
        r3.raise_for_status()
        soup3 = BeautifulSoup(r3.text, 'html.parser')

        # Parse table rows — structure: Name | Address | Amount
        table = soup3.find('table', class_=re.compile(r'grid|list|delinquent', re.I))
        if not table:
            table = soup3.find('table')

        if table:
            rows = table.find_all('tr')[1:]  # skip header
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue

                owner   = cells[0].get_text(strip=True) if len(cells) > 0 else ''
                address = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                amount  = cells[2].get_text(strip=True) if len(cells) > 2 else ''

                if not owner or not address:
                    continue

                # Only include residential real estate (skip personal property)
                if 'personal' in owner.lower():
                    continue

                lead = {
                    'id':         make_id('tax-delinquent', owner, address),
                    'type':       'tax-delinquent',
                    'owner':      owner.upper().strip(),
                    'address':    normalize_address(address),
                    'amount':     format_amount(amount),
                    'filingDate': None,  # delinquency list doesn't include exact date
                    'caseNumber': None,
                    'notes':      'Real estate tax delinquent — Sedgwick County Treasurer',
                    'scrapedAt':  now_iso(),
                }
                leads.append(lead)

        log.info(f'  → {len(leads)} tax delinquent leads')

    except Exception as e:
        log.warning(f'  ✗ Tax delinquent scrape failed: {e}')

    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — Sedgwick County Tax Foreclosure Auction
# URL: https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/
# The county posts a map book PDF ~30 days before auction.
# We scrape the page for auction dates and any listed properties.
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_foreclosure():
    leads = []
    url = 'https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/'
    log.info(f'Scraping tax foreclosure: {url}')

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')

        # Look for auction date mentions
        content = soup.get_text(separator=' ')
        date_match = re.search(r'(\w+ \d{1,2},?\s*\d{4})', content)
        auction_date = date_match.group(1) if date_match else None

        # Look for PDF map book link
        pdf_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '.pdf' in href.lower() and ('map' in href.lower() or 'book' in href.lower() or 'exhibit' in href.lower()):
                pdf_links.append(href)

        # Look for property listings in any tables or lists on the page
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                text = ' '.join(c.get_text(strip=True) for c in cells)
                if not text.strip():
                    continue

                # Try to extract an address-like pattern
                addr_match = re.search(r'\d+\s+\w+.*(?:St|Ave|Blvd|Dr|Ct|Pl|Rd|Ln|Way)', text, re.I)
                address = addr_match.group(0) if addr_match else text[:80]

                lead = {
                    'id':         make_id('tax-foreclosure', text),
                    'type':       'tax-foreclosure',
                    'owner':      'SEE COUNTY RECORDS',
                    'address':    address + ', Wichita KS',
                    'amount':     None,
                    'filingDate': auction_date,
                    'caseNumber': None,
                    'notes':      'Tax foreclosure auction — Sedgwick County. ' +
                                  (f'Auction date: {auction_date}' if auction_date else 'Check treasurer site for dates.') +
                                  (f' PDF: {pdf_links[0]}' if pdf_links else ''),
                    'scrapedAt':  now_iso(),
                }
                leads.append(lead)

        # If no table data found but auction is posted, create a placeholder record
        if not leads and (auction_date or pdf_links):
            leads.append({
                'id':         make_id('tax-foreclosure', auction_date or 'pending'),
                'type':       'tax-foreclosure',
                'owner':      'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                'address':    'Sedgwick County, KS',
                'amount':     None,
                'filingDate': auction_date,
                'caseNumber': None,
                'notes':      'Tax foreclosure auction posted. ' +
                              (f'Date: {auction_date}. ' if auction_date else '') +
                              (f'Map book: {pdf_links[0]}' if pdf_links else 'Check sedgwickcounty.org/treasurer'),
                'scrapedAt':  now_iso(),
            })

        log.info(f'  → {len(leads)} tax foreclosure leads (auction date: {auction_date})')

    except Exception as e:
        log.warning(f'  ✗ Tax foreclosure scrape failed: {e}')

    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — 18th District Court Probate Docket
# URL: https://www.dc18.org/courtscheduling/index.shtml
# Returns the next 7 days of probate hearings (case names, not full addresses)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_probate():
    leads = []
    log.info('Scraping probate docket: dc18.org')

    # The DC18 docket URLs follow a date pattern
    from datetime import timedelta
    today = datetime.now()

    for day_offset in range(0, 7):
        check_date = today + timedelta(days=day_offset)
        date_str = check_date.strftime('%m/%d/%Y')
        url = f'https://www.dc18.org/courtscheduling/probate.shtml?date={check_date.strftime("%Y-%m-%d")}'

        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, 'html.parser')
            text = soup.get_text()

            # Look for "Estate of" or "In the matter of" patterns
            estate_matches = re.findall(
                r'(?:Estate of|In the Matter of|In re[:\s]+|Guardianship of)\s+([A-Z][A-Z\s,\.]+?)(?:\n|\r|Case|\d{4})',
                text, re.I
            )

            for match in estate_matches:
                name = match.strip().rstrip(',').strip()
                if len(name) < 4 or len(name) > 80:
                    continue

                # Case number pattern: YYYY-PR-NNNNNN
                cn_match = re.search(r'(\d{4}-PR-\d+|\d{4}[A-Z]{2}\d+)', text)
                case_num = cn_match.group(1) if cn_match else None

                lead = {
                    'id':         make_id('probate', name, date_str),
                    'type':       'probate',
                    'owner':      f'Estate of {name.upper()}',
                    'address':    'Address via BatchData skip trace — see owner name',
                    'amount':     None,
                    'filingDate': check_date.strftime('%Y-%m-%d'),
                    'caseNumber': case_num,
                    'notes':      f'Probate hearing: {date_str}. Run skip trace on owner name to find property address.',
                    'scrapedAt':  now_iso(),
                }
                leads.append(lead)

            time.sleep(0.5)  # be polite

        except Exception as e:
            log.debug(f'  Probate day {date_str}: {e}')
            continue

    # Fallback: try the main docket page
    if not leads:
        try:
            r = requests.get('https://www.dc18.org/courtscheduling/index.shtml', headers=HEADERS, timeout=15)
            if r.ok:
                soup = BeautifulSoup(r.text, 'html.parser')
                probate_link = soup.find('a', string=re.compile(r'probate', re.I))
                if probate_link and probate_link.get('href'):
                    r2 = requests.get('https://www.dc18.org' + probate_link['href'], headers=HEADERS, timeout=15)
                    if r2.ok:
                        soup2 = BeautifulSoup(r2.text, 'html.parser')
                        estate_matches = re.findall(r'Estate of\s+([A-Z][A-Z\s]+)', soup2.get_text(), re.I)
                        for name in estate_matches[:20]:
                            leads.append({
                                'id':         make_id('probate', name, today.date().isoformat()),
                                'type':       'probate',
                                'owner':      f'Estate of {name.strip().upper()}',
                                'address':    'Run skip trace on owner name',
                                'amount':     None,
                                'filingDate': today.strftime('%Y-%m-%d'),
                                'caseNumber': None,
                                'notes':      'Probate filing — DC18 docket. Skip trace owner name for property address.',
                                'scrapedAt':  now_iso(),
                            })
        except Exception as e:
            log.warning(f'  ✗ Probate fallback failed: {e}')

    log.info(f'  → {len(leads)} probate leads')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4 — Kansas Department of Revenue State Tax Warrants
# URL: https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch
# Public search — filter by county
# ─────────────────────────────────────────────────────────────────────────────
def scrape_state_warrants():
    leads = []
    url = 'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch'
    log.info(f'Scraping state tax warrants: {url}')

    try:
        session = requests.Session()

        # Load form to get any tokens
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')

        # Find Sedgwick County option value
        county_select = soup.find('select', {'name': re.compile(r'county', re.I)})
        sedgwick_value = '95'  # Sedgwick County FIPS/code in Kansas

        if county_select:
            for opt in county_select.find_all('option'):
                if 'sedgwick' in opt.get_text(strip=True).lower():
                    sedgwick_value = opt.get('value', '95')
                    break

        # Build POST form
        form_data = {}
        for inp in soup.find_all('input'):
            name = inp.get('name')
            val  = inp.get('value', '')
            if name:
                form_data[name] = val

        # Set county to Sedgwick
        form_data['county'] = sedgwick_value
        form_data['type']   = 'i'  # individual (also try 'b' for business)

        # Also look for the submit button name
        submit_btn = soup.find('input', {'type': 'submit'})
        if submit_btn and submit_btn.get('name'):
            form_data[submit_btn['name']] = submit_btn.get('value', 'Search')

        r2 = session.post(url, data=form_data, headers=HEADERS, timeout=20)
        r2.raise_for_status()
        soup2 = BeautifulSoup(r2.text, 'html.parser')

        # Parse results table
        table = soup2.find('table')
        if table:
            rows = table.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue

                name    = cells[0].get_text(strip=True) if len(cells) > 0 else ''
                address = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                amount  = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                warrant = cells[3].get_text(strip=True) if len(cells) > 3 else ''

                if not name:
                    continue

                lead = {
                    'id':         make_id('state-warrant', name, warrant or amount),
                    'type':       'state-warrant',
                    'owner':      name.upper().strip(),
                    'address':    normalize_address(address) if address else 'Sedgwick County KS',
                    'amount':     format_amount(amount),
                    'filingDate': None,
                    'caseNumber': warrant if warrant else None,
                    'notes':      'Kansas DOR state tax warrant — Sedgwick County',
                    'scrapedAt':  now_iso(),
                }
                leads.append(lead)

        # Also search for businesses
        form_data['type'] = 'b'
        r3 = session.post(url, data=form_data, headers=HEADERS, timeout=20)
        r3.raise_for_status()
        soup3 = BeautifulSoup(r3.text, 'html.parser')
        table3 = soup3.find('table')
        if table3:
            rows = table3.find_all('tr')[1:]
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue
                name    = cells[0].get_text(strip=True)
                address = cells[1].get_text(strip=True) if len(cells) > 1 else ''
                amount  = cells[2].get_text(strip=True) if len(cells) > 2 else ''
                warrant = cells[3].get_text(strip=True) if len(cells) > 3 else ''
                if not name:
                    continue
                lead = {
                    'id':         make_id('state-warrant-biz', name, warrant or amount),
                    'type':       'state-warrant',
                    'owner':      name.upper().strip(),
                    'address':    normalize_address(address) if address else 'Sedgwick County KS',
                    'amount':     format_amount(amount),
                    'filingDate': None,
                    'caseNumber': warrant if warrant else None,
                    'notes':      'Kansas DOR state tax warrant (business) — Sedgwick County',
                    'scrapedAt':  now_iso(),
                }
                leads.append(lead)

        log.info(f'  → {len(leads)} state warrant leads')

    except Exception as e:
        log.warning(f'  ✗ State warrants scrape failed: {e}')

    return leads


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def normalize_address(addr):
    """Clean up address strings."""
    if not addr:
        return ''
    addr = re.sub(r'\s+', ' ', addr).strip()
    # Add Wichita KS if no state present
    if addr and 'KS' not in addr and 'Kansas' not in addr:
        if re.search(r'\d', addr):  # looks like an address with a number
            addr += ', Wichita KS'
    return addr

def format_amount(amount_str):
    """Normalize dollar amount strings."""
    if not amount_str:
        return None
    # Remove non-numeric except . and ,
    clean = re.sub(r'[^\d.,]', '', amount_str)
    if not clean:
        return None
    # Remove commas for float conversion check
    try:
        val = float(clean.replace(',', ''))
        return f'${val:,.2f}'
    except:
        return amount_str.strip() or None

def deduplicate(leads):
    """Remove duplicate IDs, keeping the first occurrence."""
    seen = set()
    unique = []
    for l in leads:
        if l['id'] not in seen:
            seen.add(l['id'])
            unique.append(l)
    return unique


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info('=' * 60)
    log.info('INTEL Scraper — Sedgwick County')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

    all_leads = []

    all_leads += scrape_tax_delinquent()
    all_leads += scrape_tax_foreclosure()
    all_leads += scrape_probate()
    all_leads += scrape_state_warrants()

    # Deduplicate
    all_leads = deduplicate(all_leads)

    # Sort: newest scrape date first, then by type
    type_order = {'tax-foreclosure':0, 'probate':1, 'state-warrant':2, 'tax-delinquent':3}
    all_leads.sort(key=lambda l: (type_order.get(l['type'], 9), l.get('scrapedAt','')), reverse=False)
    all_leads.sort(key=lambda l: l.get('scrapedAt',''), reverse=True)

    output = {
        'lastUpdated': now_iso(),
        'totalLeads':  len(all_leads),
        'sources': {
            'tax_delinquent':  len([l for l in all_leads if l['type'] == 'tax-delinquent']),
            'tax_foreclosure': len([l for l in all_leads if l['type'] == 'tax-foreclosure']),
            'probate':         len([l for l in all_leads if l['type'] == 'probate']),
            'state_warrant':   len([l for l in all_leads if l['type'] == 'state-warrant']),
        },
        'leads': all_leads,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, indent=2)

    log.info('=' * 60)
    log.info(f'✓ Done — {len(all_leads)} total leads written to {OUTPUT}')
    for k, v in output['sources'].items():
        log.info(f'  {k}: {v}')
    log.info('=' * 60)

if __name__ == '__main__':
    main()
