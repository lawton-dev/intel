#!/usr/bin/env python3
"""
INTEL Scraper — Sedgwick County Lead Intelligence
==================================================
Runs daily via GitHub Actions. Pulls from:
  1. Sedgwick County Treasurer — Delinquent Real Estate Tax List
  2. Sedgwick County Treasurer — Tax Foreclosure Auction List
  3. 18th District Court     — Probate Daily Docket
  4. Kansas DOR              — State Tax Warrants (Sedgwick County)

Outputs: data/leads.json (auto-committed by the workflow)
"""

import json
import re
import hashlib
import time
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
log = logging.getLogger('intel')

OUTPUT = Path(__file__).parent.parent / 'data' / 'leads.json'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

def make_id(*parts):
    raw = '|'.join(str(p) for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

def now_iso():
    return datetime.now(timezone.utc).isoformat()


# -----------------------------------------------------------------------------
# SOURCE 1 — Sedgwick County Delinquent Real Estate Taxes
# Flow: GET disclaimer → POST accept → GET listing
# -----------------------------------------------------------------------------
def scrape_tax_delinquent():
    leads = []
    BASE  = 'https://ssc.sedgwickcounty.org'
    INTRO = f'{BASE}/propertytax/delinquenciesintro.aspx'
    LIST  = f'{BASE}/propertytax/delinquencies.aspx'

    log.info(f'Scraping tax delinquent: {LIST}')
    try:
        session = requests.Session()
        session.headers.update(HEADERS)

        # Load disclaimer page
        r1 = session.get(INTRO, timeout=20)
        r1.raise_for_status()
        soup1 = BeautifulSoup(r1.text, 'html.parser')

        # Collect hidden ASP.NET fields + submit button
        form_data = {}
        for inp in soup1.find_all('input'):
            itype = inp.get('type', '').lower()
            name  = inp.get('name', '')
            val   = inp.get('value', '')
            if name and itype in ('hidden', 'submit'):
                form_data[name] = val

        # POST disclaimer acceptance
        r2 = session.post(INTRO, data=form_data, timeout=20)
        r2.raise_for_status()

        # GET the listing page
        r3 = session.get(LIST, timeout=20)
        r3.raise_for_status()
        soup3 = BeautifulSoup(r3.text, 'html.parser')

        # Try <pre> block (common for these older ASP pages)
        pre = soup3.find('pre')
        if pre:
            for line in pre.get_text().splitlines():
                line = line.strip()
                if not line or len(line) < 5:
                    continue
                parts = re.split(r'\s{2,}', line)
                if len(parts) < 2:
                    continue
                owner, address = parts[0].strip(), parts[1].strip()
                amount = parts[2].strip() if len(parts) > 2 else ''
                if not owner or re.match(r'^(name|owner|taxpayer|address)', owner, re.I):
                    continue
                leads.append(_tax_delinquent_lead(owner, address, amount))

        # Try tables
        if not leads:
            for table in soup3.find_all('table'):
                for row in table.find_all('tr')[1:]:
                    cells = [c.get_text(strip=True) for c in row.find_all('td')]
                    if len(cells) < 2 or not cells[0]:
                        continue
                    leads.append(_tax_delinquent_lead(cells[0],
                                                       cells[1] if len(cells)>1 else '',
                                                       cells[2] if len(cells)>2 else ''))
                if leads:
                    break

        # Raw text fallback — find lines with dollar amounts
        if not leads:
            for line in soup3.get_text(separator='\n').splitlines():
                line = line.strip()
                m = re.search(r'\$([\d,]+\.?\d*)', line)
                if m and len(line) > 10:
                    before = line[:m.start()].strip()
                    if len(before) > 3:
                        leads.append(_tax_delinquent_lead(before, '', m.group(0)))

        log.info(f'  -> {len(leads)} tax delinquent leads')

    except Exception as e:
        log.warning(f'  x Tax delinquent scrape failed: {e}')

    return leads

def _tax_delinquent_lead(owner, address, amount):
    return {
        'id':         make_id('tax-delinquent', owner, address),
        'type':       'tax-delinquent',
        'owner':      owner.upper().strip(),
        'address':    normalize_address(address),
        'amount':     format_amount(amount),
        'filingDate': None,
        'caseNumber': None,
        'notes':      'Real estate tax delinquent — Sedgwick County Treasurer',
        'scrapedAt':  now_iso(),
    }


# -----------------------------------------------------------------------------
# SOURCE 2 — Tax Foreclosure Auction
# -----------------------------------------------------------------------------
def scrape_tax_foreclosure():
    leads = []
    url = 'https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/'
    log.info(f'Scraping tax foreclosure: {url}')

    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        full_text = soup.get_text(separator=' ')

        # Find auction date
        auction_date = None
        for pat in [r'(\w+ \d{1,2},?\s*202\d)', r'(202\d-\d{2}-\d{2})', r'(\d{1,2}/\d{1,2}/202\d)']:
            m = re.search(pat, full_text)
            if m:
                auction_date = m.group(1)
                break

        # Find PDF links
        pdf_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '.pdf' in href.lower():
                if not href.startswith('http'):
                    href = 'https://www.sedgwickcounty.org' + href
                pdf_links.append(href)

        # Parse property tables
        for table in soup.find_all('table'):
            for row in table.find_all('tr')[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all('td')]
                if len(cells) < 1 or not cells[0]:
                    continue
                text_joined = ' '.join(cells)
                addr_m = re.search(
                    r'\d+\s+[NSEW]?\.?\s*\w+\s+(?:St|Ave|Blvd|Dr|Ct|Pl|Rd|Ln|Way|Ter)',
                    text_joined, re.I)
                address = (addr_m.group(0) if addr_m else text_joined[:80]) + ', Wichita KS'
                leads.append({
                    'id':         make_id('tax-foreclosure', text_joined),
                    'type':       'tax-foreclosure',
                    'owner':      'SEE COUNTY RECORDS',
                    'address':    address,
                    'amount':     None,
                    'filingDate': auction_date,
                    'caseNumber': cells[0],
                    'notes':      'Tax foreclosure auction — Sedgwick County Treasurer' +
                                  (f'. Auction date: {auction_date}' if auction_date else '') +
                                  (f'. PDF: {pdf_links[0]}' if pdf_links else ''),
                    'scrapedAt':  now_iso(),
                })

        # Auction posted but no table yet
        if not leads and (auction_date or pdf_links):
            leads.append({
                'id':         make_id('tax-foreclosure-notice', auction_date or 'active'),
                'type':       'tax-foreclosure',
                'owner':      'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                'address':    'Sedgwick County, KS',
                'amount':     None,
                'filingDate': auction_date,
                'caseNumber': None,
                'notes':      'Tax foreclosure auction posted.' +
                              (f' Date: {auction_date}.' if auction_date else '') +
                              (f' Map book: {pdf_links[0]}' if pdf_links
                               else ' Visit sedgwickcounty.org/treasurer for full list.'),
                'scrapedAt':  now_iso(),
            })

        log.info(f'  -> {len(leads)} tax foreclosure leads (auction: {auction_date})')

    except Exception as e:
        log.warning(f'  x Tax foreclosure scrape failed: {e}')

    return leads


# -----------------------------------------------------------------------------
# SOURCE 3 — 18th District Court Probate Docket
# -----------------------------------------------------------------------------
def scrape_probate():
    leads = []
    log.info('Scraping probate docket: dc18.org')

    DC18 = 'https://www.dc18.org'
    today = datetime.now()

    # Try main scheduling index to find probate link
    try:
        r = requests.get(f'{DC18}/courtscheduling/index.shtml', headers=HEADERS, timeout=15)
        if r.ok:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                href = a['href'].strip()
                text = a.get_text(strip=True).lower()
                if 'probate' in text or 'probate' in href.lower():
                    full = href if href.startswith('http') else DC18 + ('/' if not href.startswith('/') else '') + href
                    r2 = requests.get(full, headers=HEADERS, timeout=15)
                    if r2.ok:
                        leads += parse_probate_html(r2.text, today)
                    break
    except Exception as e:
        log.debug(f'  Probate main page: {e}')

    # Try direct paths
    if not leads:
        for path in ['/courtscheduling/probate.shtml', '/dockets/probate.shtml']:
            try:
                r = requests.get(DC18 + path, headers=HEADERS, timeout=10)
                if r.ok and ('estate' in r.text.lower() or 'probate' in r.text.lower()):
                    leads += parse_probate_html(r.text, today)
                    if leads:
                        break
            except:
                continue

    log.info(f'  -> {len(leads)} probate leads')
    return leads

def parse_probate_html(html, today):
    leads = []
    text = BeautifulSoup(html, 'html.parser').get_text(separator='\n')
    seen = set()

    patterns = [
        r'Estate of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#|\d{4}-|\r)',
        r'In [Rr]e[:\s]+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#|\d{4}-|\r)',
        r'Guardianship of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#|\d{4}-|\r)',
    ]
    for pat in patterns:
        for m in re.finditer(pat, text):
            name = m.group(1).strip().rstrip(',.').strip()
            if len(name) < 4 or name in seen:
                continue
            seen.add(name)
            nearby = text[max(0, m.start()-50):m.end()+100]
            cn = re.search(r'(\d{4}[- ]?(?:PR|CV|PB)[- ]?\d{4,8})', nearby, re.I)
            leads.append({
                'id':         make_id('probate', name, today.date().isoformat()),
                'type':       'probate',
                'owner':      f'Estate of {name.upper()}',
                'address':    'Run skip trace on owner name for property address',
                'amount':     None,
                'filingDate': today.strftime('%Y-%m-%d'),
                'caseNumber': cn.group(1) if cn else None,
                'notes':      'Probate filing — 18th District Court. Use Trace tool to find property.',
                'scrapedAt':  now_iso(),
            })
    return leads


# -----------------------------------------------------------------------------
# SOURCE 4 — Kansas DOR State Tax Warrants
# -----------------------------------------------------------------------------
def scrape_state_warrants():
    leads = []
    URL = 'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch'
    log.info(f'Scraping state tax warrants: {URL}')

    session = requests.Session()
    session.headers.update(HEADERS)

    for wtype in ['i', 'b']:
        try:
            r1 = session.get(URL, params={'type': wtype}, timeout=20)
            r1.raise_for_status()
            soup1 = BeautifulSoup(r1.text, 'html.parser')

            # Build form from hidden inputs
            form_data = {'type': wtype}
            for inp in soup1.find_all('input'):
                n, v, t = inp.get('name',''), inp.get('value',''), inp.get('type','').lower()
                if n and t == 'hidden':
                    form_data[n] = v
            for btn in soup1.find_all('input', type='submit'):
                n = btn.get('name','')
                if n:
                    form_data[n] = btn.get('value','Search')

            # Find county dropdown + Sedgwick value
            sel = soup1.find('select', {'name': re.compile(r'county', re.I)})
            if sel:
                for opt in sel.find_all('option'):
                    if 'sedgwick' in opt.get_text(strip=True).lower():
                        form_data[sel['name']] = opt.get('value','95')
                        break
            else:
                form_data['county'] = '95'

            r2 = session.post(URL, data=form_data, timeout=20)
            r2.raise_for_status()
            soup2 = BeautifulSoup(r2.text, 'html.parser')

            table = soup2.find('table')
            if table:
                header_skipped = False
                for row in table.find_all('tr'):
                    cells = [c.get_text(strip=True) for c in row.find_all(['td','th'])]
                    if not cells or not cells[0]:
                        continue
                    if not header_skipped:
                        header_skipped = True
                        if any(h.lower() in ('name','taxpayer','county') for h in cells):
                            continue
                    name    = cells[0] if len(cells) > 0 else ''
                    address = cells[1] if len(cells) > 1 else ''
                    county  = cells[2] if len(cells) > 2 else ''
                    amount  = cells[3] if len(cells) > 3 else ''
                    warrant = cells[4] if len(cells) > 4 else ''
                    if not name or len(name) < 2:
                        continue
                    if county and county.strip() and 'sedgwick' not in county.lower():
                        continue
                    leads.append({
                        'id':         make_id('state-warrant', wtype, name, warrant or amount),
                        'type':       'state-warrant',
                        'owner':      name.upper().strip(),
                        'address':    normalize_address(address) or 'Sedgwick County KS',
                        'amount':     format_amount(amount),
                        'filingDate': None,
                        'caseNumber': warrant or None,
                        'notes':      f'Kansas DOR state tax warrant '
                                      f'({"individual" if wtype=="i" else "business"}) — Sedgwick County',
                        'scrapedAt':  now_iso(),
                    })

            time.sleep(1)

        except Exception as e:
            log.warning(f'  x State warrants ({wtype}) failed: {e}')

    log.info(f'  -> {len(leads)} state warrant leads')
    return leads


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def normalize_address(addr):
    if not addr:
        return ''
    addr = re.sub(r'\s+', ' ', addr).strip()
    if addr and 'KS' not in addr and 'Kansas' not in addr and re.search(r'\d', addr):
        addr += ', Wichita KS'
    return addr

def format_amount(s):
    if not s:
        return None
    clean = re.sub(r'[^\d.]', '', str(s).replace(',', ''))
    try:
        v = float(clean)
        return f'${v:,.2f}' if v > 0 else None
    except:
        return s.strip() or None

def deduplicate(leads):
    seen, unique = set(), []
    for l in leads:
        if l['id'] not in seen:
            seen.add(l['id'])
            unique.append(l)
    return unique


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
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
    all_leads  = deduplicate(all_leads)

    order = {'tax-foreclosure': 0, 'probate': 1, 'state-warrant': 2, 'tax-delinquent': 3}
    all_leads.sort(key=lambda l: order.get(l['type'], 9))

    output = {
        'lastUpdated': now_iso(),
        'totalLeads':  len(all_leads),
        'sources': {
            'tax_delinquent':  len([l for l in all_leads if l['type']=='tax-delinquent']),
            'tax_foreclosure': len([l for l in all_leads if l['type']=='tax-foreclosure']),
            'probate':         len([l for l in all_leads if l['type']=='probate']),
            'state_warrant':   len([l for l in all_leads if l['type']=='state-warrant']),
        },
        'leads': all_leads,
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, indent=2)

    log.info('=' * 60)
    log.info(f'Done — {len(all_leads)} total leads')
    for k, v in output['sources'].items():
        log.info(f'  {k}: {v}')
    log.info('=' * 60)

if __name__ == '__main__':
    main()
