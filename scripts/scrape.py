#!/usr/bin/env python3
"""
INTEL Scraper v3 — Sedgwick County Lead Intelligence (Playwright)
=================================================================
Uses a real headless Chromium browser so JavaScript renders,
ASP.NET sessions work, and we actually get data from each page.

Sources:
  1. Sedgwick County Treasurer — Delinquent Real Estate Tax List
  2. Sedgwick County Treasurer — Tax Foreclosure Auction List
  3. 18th District Court       — Probate Daily Calendar
  4. Kansas DOR                — State Tax Warrants

Outputs: data/leads.json  (auto-committed by GitHub Actions)
"""

import json, re, hashlib, time, logging
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
log = logging.getLogger('intel')

OUTPUT = Path(__file__).parent.parent / 'data' / 'leads.json'

def make_id(*parts):
    return hashlib.md5('|'.join(str(p) for p in parts).encode()).hexdigest()[:12]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def fmt_amount(s):
    if not s: return None
    clean = re.sub(r'[^\d.]', '', str(s).replace(',', ''))
    try:
        v = float(clean)
        return f'${v:,.2f}' if v > 0 else None
    except: return None

def norm_addr(a):
    if not a: return ''
    a = re.sub(r'\s+', ' ', a).strip()
    if a and 'KS' not in a and re.search(r'\d', a):
        a += ', Wichita KS'
    return a


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 — Delinquent Real Estate Taxes
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_delinquent(page):
    leads = []
    log.info('Scraping tax delinquent...')
    try:
        page.goto('https://ssc.sedgwickcounty.org/propertytax/delinquenciesintro.aspx',
                  wait_until='networkidle', timeout=30000)

        # Accept the disclaimer
        for sel in ['input[value*="Agree"]', 'input[value*="agree"]',
                    'input[type="submit"]', '#btnAgree', 'button:has-text("Agree")']:
            try:
                page.click(sel, timeout=4000)
                page.wait_for_load_state('networkidle', timeout=15000)
                break
            except: continue

        # If still on intro, go directly to listing
        if 'intro' in page.url.lower():
            page.goto('https://ssc.sedgwickcounty.org/propertytax/delinquencies.aspx',
                      wait_until='networkidle', timeout=20000)

        text = page.inner_text('body')

        # Try table rows
        for table in page.query_selector_all('table'):
            rows = table.query_selector_all('tr')
            for row in rows[1:]:
                cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                if len(cells) < 2 or not cells[0]: continue
                owner, address = cells[0], cells[1]
                amount = cells[2] if len(cells) > 2 else ''
                if re.match(r'^(name|owner|taxpayer|address)', owner, re.I): continue
                if len(owner) < 3: continue
                leads.append({
                    'id': make_id('td', owner, address),
                    'type': 'tax-delinquent',
                    'owner': owner.upper().strip(),
                    'address': norm_addr(address),
                    'amount': fmt_amount(amount),
                    'filingDate': None,
                    'caseNumber': None,
                    'notes': 'Real estate tax delinquent — Sedgwick County Treasurer',
                    'scrapedAt': now_iso(),
                })
            if leads: break

        # Fallback: lines with dollar amounts
        if not leads:
            for line in text.splitlines():
                line = line.strip()
                m = re.search(r'\$([\d,]+\.?\d*)', line)
                if m and len(line) > 8:
                    before = line[:m.start()].strip()
                    if len(before) > 3 and not re.match(
                            r'^(pay|total|amount|note|copyright|interest)', before, re.I):
                        leads.append({
                            'id': make_id('td', before, m.group(0)),
                            'type': 'tax-delinquent',
                            'owner': before.upper()[:80],
                            'address': 'Sedgwick County KS — verify address',
                            'amount': fmt_amount(m.group(0)),
                            'filingDate': None,
                            'caseNumber': None,
                            'notes': 'Real estate tax delinquent — Sedgwick County Treasurer',
                            'scrapedAt': now_iso(),
                        })

        log.info(f'  -> {len(leads)} tax delinquent leads')
    except Exception as e:
        log.warning(f'  x Tax delinquent failed: {e}')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — Tax Foreclosure Auction
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_foreclosure(page):
    leads = []
    log.info('Scraping tax foreclosure...')
    try:
        page.goto('https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/',
                  wait_until='networkidle', timeout=30000)

        text = page.inner_text('body')

        # Find auction date
        auction_date = None
        for pat in [r'(\w+ \d{1,2},?\s*202\d)', r'(202\d-\d{2}-\d{2})', r'(\d{1,2}/\d{1,2}/202\d)']:
            m = re.search(pat, text)
            if m:
                auction_date = m.group(1).strip()
                break

        # PDF links
        pdf_links = []
        for a in page.query_selector_all('a[href*=".pdf"], a[href*=".PDF"]'):
            href = a.get_attribute('href') or ''
            if href and not href.startswith('http'):
                href = 'https://www.sedgwickcounty.org' + href
            if href: pdf_links.append(href)

        # Property table
        for table in page.query_selector_all('table'):
            rows = table.query_selector_all('tr')
            for row in rows[1:]:
                cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                if not cells or not cells[0]: continue
                joined = ' '.join(cells)
                addr_m = re.search(r'\d+\s+\w+.*?(?:St|Ave|Blvd|Dr|Ct|Pl|Rd|Ln|Way)', joined, re.I)
                addr = (addr_m.group(0) if addr_m else joined[:80]) + ', Wichita KS'
                leads.append({
                    'id': make_id('tf', joined),
                    'type': 'tax-foreclosure',
                    'owner': 'SEE COUNTY RECORDS',
                    'address': addr,
                    'amount': None,
                    'filingDate': auction_date,
                    'caseNumber': cells[0],
                    'notes': 'Tax foreclosure auction' +
                             (f' — {auction_date}' if auction_date else '') +
                             (f' — {pdf_links[0]}' if pdf_links else ''),
                    'scrapedAt': now_iso(),
                })

        if not leads and (auction_date or pdf_links):
            leads.append({
                'id': make_id('tf-notice', auction_date or 'active'),
                'type': 'tax-foreclosure',
                'owner': 'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                'address': 'Sedgwick County, KS',
                'amount': None,
                'filingDate': auction_date,
                'caseNumber': None,
                'notes': 'Tax foreclosure auction posted.' +
                         (f' Date: {auction_date}.' if auction_date else '') +
                         (f' Map book: {pdf_links[0]}' if pdf_links
                          else ' Visit sedgwickcounty.org/treasurer'),
                'scrapedAt': now_iso(),
            })

        log.info(f'  -> {len(leads)} tax foreclosure leads (auction: {auction_date})')
    except Exception as e:
        log.warning(f'  x Tax foreclosure failed: {e}')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — 18th District Court Probate Calendar
# ─────────────────────────────────────────────────────────────────────────────
def scrape_probate(page):
    leads = []
    log.info('Scraping probate docket...')
    DC18 = 'https://www.dc18.org'

    try:
        page.goto(f'{DC18}/courtscheduling/index.shtml',
                  wait_until='networkidle', timeout=20000)

        probate_url = None
        for a in page.query_selector_all('a'):
            href = a.get_attribute('href') or ''
            txt  = (a.inner_text() or '').lower()
            if 'probate' in txt or 'probate' in href.lower():
                probate_url = href if href.startswith('http') else DC18 + href
                break

        if probate_url:
            page.goto(probate_url, wait_until='networkidle', timeout=20000)
            leads += _parse_probate(page.inner_text('body'))

    except Exception as e:
        log.warning(f'  x Probate main page: {e}')

    if not leads:
        for path in ['/courtscheduling/probate.shtml', '/dockets/probate.shtml']:
            try:
                page.goto(DC18 + path, wait_until='networkidle', timeout=15000)
                t = page.inner_text('body')
                if 'estate' in t.lower():
                    leads += _parse_probate(t)
                    if leads: break
            except: continue

    log.info(f'  -> {len(leads)} probate leads')
    return leads

def _parse_probate(text):
    leads = []
    today = datetime.now()
    seen  = set()
    for pat in [
        r'Estate of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#?:?|\d{4}-|\r)',
        r'In [Rr]e[:\s]+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#?:?|\d{4}-|\r)',
        r'Guardianship of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#?:?|\d{4}-|\r)',
    ]:
        for m in re.finditer(pat, text):
            name = m.group(1).strip().rstrip(',.').strip()
            if len(name) < 4 or name in seen: continue
            seen.add(name)
            nearby = text[max(0, m.start()-60):m.end()+120]
            cn = re.search(r'(\d{4}[- ]?(?:PR|CV|PB)[- ]?\d{4,8})', nearby, re.I)
            leads.append({
                'id': make_id('pr', name, today.date().isoformat()),
                'type': 'probate',
                'owner': f'Estate of {name.upper()}',
                'address': 'Run skip trace on owner name for property address',
                'amount': None,
                'filingDate': today.strftime('%Y-%m-%d'),
                'caseNumber': cn.group(1) if cn else None,
                'notes': 'Probate filing — 18th District Court. Use Skip Trace to find property.',
                'scrapedAt': now_iso(),
            })
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4 — Kansas DOR State Tax Warrants
# ─────────────────────────────────────────────────────────────────────────────
def scrape_state_warrants(page):
    leads = []
    log.info('Scraping state tax warrants...')
    URL = 'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch'

    for wtype in ['i', 'b']:
        try:
            page.goto(f'{URL}?type={wtype}', wait_until='networkidle', timeout=20000)

            # Select Sedgwick County
            sel = page.query_selector('select[name*="ounty"], select[id*="ounty"]')
            if sel:
                opts = sel.query_selector_all('option')
                for opt in opts:
                    if 'sedgwick' in (opt.inner_text() or '').lower():
                        sel.select_option(value=opt.get_attribute('value'))
                        break
                else:
                    sel.select_option(value='95')

            # Submit
            for s in ['input[type="submit"]', 'button[type="submit"]', '#btnSearch']:
                try:
                    page.click(s, timeout=4000)
                    page.wait_for_load_state('networkidle', timeout=15000)
                    break
                except: continue

            # Parse table
            for table in page.query_selector_all('table'):
                rows = table.query_selector_all('tr')
                hdr  = False
                for row in rows:
                    cells = [c.inner_text().strip() for c in row.query_selector_all('td,th')]
                    if not cells or not cells[0]: continue
                    if not hdr:
                        hdr = True
                        if any(h.lower() in ('name','taxpayer','county') for h in cells): continue
                    name    = cells[0] if len(cells) > 0 else ''
                    address = cells[1] if len(cells) > 1 else ''
                    county  = cells[2] if len(cells) > 2 else ''
                    amount  = cells[3] if len(cells) > 3 else ''
                    warrant = cells[4] if len(cells) > 4 else ''
                    if not name or len(name) < 2: continue
                    if county and county.strip() and 'sedgwick' not in county.lower(): continue
                    leads.append({
                        'id': make_id('sw', wtype, name, warrant or amount),
                        'type': 'state-warrant',
                        'owner': name.upper().strip(),
                        'address': norm_addr(address) or 'Sedgwick County KS',
                        'amount': fmt_amount(amount),
                        'filingDate': None,
                        'caseNumber': warrant or None,
                        'notes': f'Kansas DOR state tax warrant ({"individual" if wtype=="i" else "business"})',
                        'scrapedAt': now_iso(),
                    })
            time.sleep(1)

        except Exception as e:
            log.warning(f'  x State warrants ({wtype}): {e}')

    log.info(f'  -> {len(leads)} state warrant leads')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info('=' * 60)
    log.info('INTEL Scraper v3 — Playwright')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

    all_leads = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=['--no-sandbox','--disable-setuid-sandbox',
                  '--disable-dev-shm-usage','--disable-gpu']
        )
        ctx  = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 900},
        )
        page = ctx.new_page()

        all_leads += scrape_tax_delinquent(page)
        all_leads += scrape_tax_foreclosure(page)
        all_leads += scrape_probate(page)
        all_leads += scrape_state_warrants(page)

        browser.close()

    # Deduplicate
    seen, unique = set(), []
    for l in all_leads:
        if l['id'] not in seen:
            seen.add(l['id'])
            unique.append(l)
    all_leads = unique

    order = {'tax-foreclosure':0,'probate':1,'state-warrant':2,'tax-delinquent':3}
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
    for k,v in output['sources'].items():
        log.info(f'  {k}: {v}')
    log.info('=' * 60)

if __name__ == '__main__':
    main()
