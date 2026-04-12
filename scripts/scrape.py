#!/usr/bin/env python3
"""
INTEL Scraper v3 DEBUG — dumps raw page text so we can fix parsing
"""

import json, re, hashlib, logging
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
log = logging.getLogger('intel')

OUTPUT = Path(__file__).parent.parent / 'data' / 'leads.json'

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def make_id(*parts):
    return hashlib.md5('|'.join(str(p) for p in parts).encode()).hexdigest()[:12]

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


def debug_page(page, label):
    """Dump first 3000 chars of page text to log so we can see the structure."""
    try:
        text = page.inner_text('body')
        log.info(f'\n{"="*40}\nDEBUG [{label}] URL: {page.url}\nFIRST 3000 CHARS:\n{text[:3000]}\n{"="*40}')

        # Also log all table counts and row counts
        tables = page.query_selector_all('table')
        log.info(f'  Tables found: {len(tables)}')
        for i, t in enumerate(tables):
            rows = t.query_selector_all('tr')
            log.info(f'  Table {i}: {len(rows)} rows')
            for j, row in enumerate(rows[:3]):  # first 3 rows
                cells = [c.inner_text().strip()[:40] for c in row.query_selector_all('td,th')]
                log.info(f'    Row {j}: {cells}')

        # Log all forms
        forms = page.query_selector_all('form')
        log.info(f'  Forms found: {len(forms)}')

        # Log all inputs
        inputs = page.query_selector_all('input')
        for inp in inputs:
            itype = inp.get_attribute('type') or 'text'
            iname = inp.get_attribute('name') or ''
            ival  = inp.get_attribute('value') or ''
            log.info(f'  Input: type={itype} name={iname} value={ival[:30]}')

        # Log all select dropdowns
        selects = page.query_selector_all('select')
        for sel in selects:
            sname = sel.get_attribute('name') or sel.get_attribute('id') or ''
            opts  = sel.query_selector_all('option')
            log.info(f'  Select: name={sname}, {len(opts)} options')
            for opt in opts[:5]:
                log.info(f'    Option: value={opt.get_attribute("value")} text={opt.inner_text()[:30]}')

    except Exception as e:
        log.warning(f'  Debug dump failed: {e}')


def scrape_tax_delinquent(page):
    log.info('\n--- TAX DELINQUENT ---')
    page.goto('https://ssc.sedgwickcounty.org/propertytax/delinquenciesintro.aspx',
              wait_until='networkidle', timeout=30000)
    debug_page(page, 'TAX DELINQUENT INTRO')

    # Try clicking agree
    for sel in ['input[value*="Agree"]', 'input[value*="agree"]',
                'input[type="submit"]', '#btnAgree']:
        try:
            log.info(f'  Trying to click: {sel}')
            page.click(sel, timeout=4000)
            page.wait_for_load_state('networkidle', timeout=15000)
            log.info(f'  Clicked! Now at: {page.url}')
            break
        except Exception as e:
            log.info(f'  {sel} not found: {e}')

    debug_page(page, 'TAX DELINQUENT AFTER CLICK')
    return []


def scrape_kdor_warrants(page):
    log.info('\n--- KDOR STATE WARRANTS ---')
    page.goto('https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch?type=i',
              wait_until='networkidle', timeout=20000)
    debug_page(page, 'KDOR WARRANTS')
    return []


def scrape_probate(page):
    log.info('\n--- PROBATE DC18 ---')
    page.goto('https://www.dc18.org/courtscheduling/index.shtml',
              wait_until='networkidle', timeout=20000)
    debug_page(page, 'DC18 SCHEDULING INDEX')
    return []


def scrape_tax_foreclosure(page):
    log.info('\n--- TAX FORECLOSURE ---')
    page.goto('https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/',
              wait_until='networkidle', timeout=30000)
    debug_page(page, 'TAX FORECLOSURE')
    return []


def main():
    log.info('=' * 60)
    log.info('INTEL Scraper DEBUG RUN')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

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

        scrape_tax_delinquent(page)
        scrape_kdor_warrants(page)
        scrape_probate(page)
        scrape_tax_foreclosure(page)

        browser.close()

    # Write empty leads so dashboard stays stable
    output = {
        'lastUpdated': now_iso(),
        'totalLeads': 0,
        'sources': {'tax_delinquent':0,'tax_foreclosure':0,'probate':0,'state_warrant':0},
        'leads': []
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, 'w') as f:
        json.dump(output, f, indent=2)

    log.info('DEBUG RUN COMPLETE — check logs above for page structure')

if __name__ == '__main__':
    main()
