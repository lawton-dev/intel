#!/usr/bin/env python3
"""
INTEL Scraper v4 — Sedgwick County Lead Intelligence
=====================================================
Production scraper based on confirmed page structures.

Sources:
  1. Sedgwick County Treasurer — Delinquent Tax Search (A-Z sweep)
  2. Sedgwick County Treasurer — Tax Foreclosure Auction (seasonal)
  3. 18th District Court       — Probate Daily Calendar (PDF)
  4. Kansas DOR                — State Tax Warrants (full table, filter Sedgwick)

Outputs: data/leads.json
"""

import json, re, hashlib, time, logging, io
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
# The page is a name search. We sweep A-Z to get all records.
# URL: https://ssc.sedgwickcounty.org/propertytax/delinquencies.aspx
# Form field: ctl00$mainContentPlaceHolder$keywordsTextBox_TextBox
# Submit:     ctl00$mainContentPlaceHolder$searchButton
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_delinquent(page):
    leads = []
    seen  = set()
    log.info('Scraping tax delinquent (A-Z sweep)...')

    try:
        # First load the intro and click through
        page.goto('https://ssc.sedgwickcounty.org/propertytax/delinquenciesintro.aspx',
                  wait_until='networkidle', timeout=30000)
        page.click('input[type="submit"]', timeout=5000)
        page.wait_for_load_state('networkidle', timeout=15000)

        # Now sweep A through Z
        letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        for letter in letters:
            try:
                # Type the letter in the search box
                page.fill('input[name="ctl00$mainContentPlaceHolder$keywordsTextBox_TextBox"]',
                          letter)
                page.click('input[name="ctl00$mainContentPlaceHolder$searchButton"]')
                page.wait_for_load_state('networkidle', timeout=15000)

                # Parse results table
                tables = page.query_selector_all('table')
                found_this_letter = 0
                for table in tables:
                    rows = table.query_selector_all('tr')
                    for row in rows[1:]:  # skip header
                        cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                        if len(cells) < 2 or not cells[0]: continue

                        # Typical columns: Name | Address | Amount OR Name | ParcelID | Amount | Address
                        # Try to identify which column is which
                        owner   = cells[0].strip()
                        address = ''
                        amount  = ''

                        # Skip nav/header rows
                        if re.match(r'^(name|owner|taxpayer|parcel|search|skip|home)', owner, re.I):
                            continue
                        if len(owner) < 3 or owner.isdigit():
                            continue

                        # Find address-looking cell (contains digits + street)
                        for cell in cells[1:]:
                            if re.search(r'\d+\s+[NSEW]?\s*\w+\s+(?:St|Ave|Blvd|Dr|Ct|Pl|Rd|Ln|Way|Ter)', cell, re.I):
                                address = cell
                                break

                        # Find amount-looking cell
                        for cell in cells:
                            if re.match(r'^\$[\d,]+\.?\d*$', cell.strip()):
                                amount = cell
                                break

                        uid = make_id('td', owner, address or letter)
                        if uid in seen: continue
                        seen.add(uid)

                        leads.append({
                            'id':         uid,
                            'type':       'tax-delinquent',
                            'owner':      owner.upper(),
                            'address':    norm_addr(address),
                            'amount':     fmt_amount(amount),
                            'filingDate': None,
                            'caseNumber': None,
                            'notes':      'Real estate tax delinquent — Sedgwick County Treasurer',
                            'scrapedAt':  now_iso(),
                        })
                        found_this_letter += 1

                if found_this_letter:
                    log.info(f'  {letter}: {found_this_letter} leads')
                time.sleep(0.5)

            except Exception as e:
                log.debug(f'  Letter {letter} error: {e}')
                continue

    except Exception as e:
        log.warning(f'  x Tax delinquent failed: {e}')

    log.info(f'  -> {len(leads)} tax delinquent leads total')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 — Tax Foreclosure Auction (seasonal — posts ~30 days before auction)
# ─────────────────────────────────────────────────────────────────────────────
def scrape_tax_foreclosure(page):
    leads = []
    log.info('Scraping tax foreclosure auction...')
    try:
        page.goto('https://www.sedgwickcounty.org/treasurer/tax-foreclosure-auctions/',
                  wait_until='networkidle', timeout=30000)
        text = page.inner_text('body')

        # Find future auction date (not concluded)
        auction_date = None
        if 'concluded' not in text.lower():
            for pat in [r'(\w+ \d{1,2},?\s*202\d)', r'(202\d-\d{2}-\d{2})', r'(\d{1,2}/\d{1,2}/202\d)']:
                m = re.search(pat, text)
                if m:
                    auction_date = m.group(1).strip()
                    break

        # Find PDF links
        pdf_links = []
        for a in page.query_selector_all('a[href*=".pdf"], a[href*=".PDF"]'):
            href = a.get_attribute('href') or ''
            if href and not href.startswith('http'):
                href = 'https://www.sedgwickcounty.org' + href
            if href: pdf_links.append(href)

        if auction_date or pdf_links:
            leads.append({
                'id':         make_id('tf-notice', auction_date or 'active'),
                'type':       'tax-foreclosure',
                'owner':      'MULTIPLE PROPERTIES — SEE AUCTION LIST',
                'address':    'Sedgwick County, KS',
                'amount':     None,
                'filingDate': auction_date,
                'caseNumber': None,
                'notes':      'Tax foreclosure auction posted.' +
                              (f' Date: {auction_date}.' if auction_date else '') +
                              (f' Map: {pdf_links[0]}' if pdf_links
                               else ' Visit sedgwickcounty.org/treasurer'),
                'scrapedAt':  now_iso(),
            })
        else:
            log.info('  No active auction currently posted (concluded or not yet scheduled)')

        log.info(f'  -> {len(leads)} tax foreclosure leads')
    except Exception as e:
        log.warning(f'  x Tax foreclosure failed: {e}')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 — 18th District Court Probate Calendar (PDF)
# The dockets are PDFs. We download and parse the text from them.
# ─────────────────────────────────────────────────────────────────────────────
def scrape_probate(page):
    leads = []
    log.info('Scraping probate docket (PDF)...')

    DC18 = 'https://www.dc18.org'

    try:
        page.goto(f'{DC18}/court-schedule', wait_until='networkidle', timeout=20000)

        # Find probate calendar PDF link
        probate_pdf_url = None
        for a in page.query_selector_all('a'):
            href = a.get_attribute('href') or ''
            txt  = (a.inner_text() or '').strip()
            # Look for the "Upcoming Dockets" link near "Probate Daily Calendar"
            if ('probate' in txt.lower() or
                ('upcoming' in txt.lower() and 'probate' in page.inner_text('body').lower())):
                if href:
                    probate_pdf_url = href if href.startswith('http') else DC18 + href
                    break

        # Also scan all links for PDF that might be the probate docket
        if not probate_pdf_url:
            all_links = page.query_selector_all('a[href]')
            for a in all_links:
                href = a.get_attribute('href') or ''
                txt  = (a.inner_text() or '').lower()
                if 'probate' in txt and href:
                    probate_pdf_url = href if href.startswith('http') else DC18 + href
                    break

        if probate_pdf_url:
            log.info(f'  Found probate link: {probate_pdf_url}')

            # Download PDF content
            import urllib.request
            try:
                req = urllib.request.Request(
                    probate_pdf_url,
                    headers={'User-Agent': 'Mozilla/5.0 Chrome/120.0.0.0'}
                )
                with urllib.request.urlopen(req, timeout=20) as resp:
                    pdf_bytes = resp.read()

                # Parse PDF text with pdfminer
                try:
                    from pdfminer.high_level import extract_text_to_fp
                    from pdfminer.layout import LAParams
                    out = io.StringIO()
                    extract_text_to_fp(io.BytesIO(pdf_bytes), out, laparams=LAParams())
                    pdf_text = out.getvalue()
                    leads += _parse_probate_text(pdf_text)
                    log.info(f'  Parsed PDF: {len(leads)} estate names found')
                except ImportError:
                    # pdfminer not available — try raw text extraction
                    text = pdf_bytes.decode('latin-1', errors='ignore')
                    leads += _parse_probate_text(text)

            except Exception as e:
                log.warning(f'  PDF download/parse failed: {e}')
        else:
            log.info('  No probate PDF link found on page')

    except Exception as e:
        log.warning(f'  x Probate scrape failed: {e}')

    log.info(f'  -> {len(leads)} probate leads')
    return leads

def _parse_probate_text(text):
    leads = []
    today = datetime.now()
    seen  = set()

    patterns = [
        r'Estate of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case\s*#?:?|\d{4}-|\r|,\s*Dec)',
        r'In [Rr]e[:\s]+(?:Estate of\s+)?([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case|\d{4}-|\r)',
        r'Guardianship of\s+([A-Z][A-Z\s,\.]{3,60}?)(?:\n|Case|\d{4}-|\r)',
    ]
    for pat in patterns:
        for m in re.finditer(pat, text):
            name = m.group(1).strip().rstrip(',.').strip()
            if len(name) < 4 or name in seen: continue
            seen.add(name)
            nearby = text[max(0, m.start()-80):m.end()+150]
            cn = re.search(r'(\d{4}[- ]?(?:PR|CV|PB)[- ]?\d{4,8})', nearby, re.I)
            leads.append({
                'id':         make_id('pr', name, today.date().isoformat()),
                'type':       'probate',
                'owner':      f'Estate of {name.upper()}',
                'address':    'Run skip trace on owner name for property address',
                'amount':     None,
                'filingDate': today.strftime('%Y-%m-%d'),
                'caseNumber': cn.group(1) if cn else None,
                'notes':      'Probate filing — 18th District Court. Use Skip Trace to find property.',
                'scrapedAt':  now_iso(),
            })
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 4 — Kansas DOR State Tax Warrants
# The full table loads on page load — just filter for SEDGWICK county.
# Structure confirmed: Name+Address | County | Tax Type | Amount | Case#
# ─────────────────────────────────────────────────────────────────────────────
def scrape_state_warrants(page):
    leads = []
    log.info('Scraping KDOR state tax warrants...')

    URL = 'https://www.kdor.ks.gov/Apps/Misc/Miscellaneous/WarrantsOnWebSearch'

    for wtype, label in [('i', 'individual'), ('b', 'business')]:
        try:
            page.goto(f'{URL}?type={wtype}', wait_until='networkidle', timeout=20000)

            table = page.query_selector('table')
            if not table:
                log.info(f'  No table found for type={wtype}')
                continue

            rows = table.query_selector_all('tr')
            log.info(f'  type={wtype}: {len(rows)} rows in table')

            for row in rows[1:]:  # skip header
                cells = [c.inner_text().strip() for c in row.query_selector_all('td')]
                if len(cells) < 4: continue

                # Confirmed structure: Name+Address | County | Tax Type | Amount | Case#
                # Name and address are combined in cell 0, separated by \xa0\xa0 (non-breaking spaces)
                name_addr = cells[0]
                county    = cells[1] if len(cells) > 1 else ''
                tax_type  = cells[2] if len(cells) > 2 else ''
                amount    = cells[3] if len(cells) > 3 else ''
                case_num  = cells[4] if len(cells) > 4 else ''

                # Only Sedgwick County
                if 'SEDGWICK' not in county.upper():
                    continue

                # Split name from address on double non-breaking spaces or double spaces
                parts = re.split(r'\xa0{2,}|\s{3,}', name_addr)
                name    = parts[0].strip() if parts else name_addr
                address = parts[1].strip() if len(parts) > 1 else ''

                # Clean up address — remove zip+4 artifacts
                address = re.sub(r'\s+', ' ', address).strip()

                if not name or len(name) < 2: continue

                leads.append({
                    'id':         make_id('sw', wtype, name, case_num or amount),
                    'type':       'state-warrant',
                    'owner':      name.upper().strip(),
                    'address':    norm_addr(address),
                    'amount':     fmt_amount(amount),
                    'filingDate': None,
                    'caseNumber': case_num or None,
                    'notes':      f'Kansas DOR state tax warrant ({label}) — {tax_type}',
                    'scrapedAt':  now_iso(),
                })

        except Exception as e:
            log.warning(f'  x State warrants ({wtype}) failed: {e}')

    log.info(f'  -> {len(leads)} state warrant leads')
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info('=' * 60)
    log.info('INTEL Scraper v4 — Sedgwick County')
    log.info(f'Run time: {now_iso()}')
    log.info('=' * 60)

    all_leads = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox',
                  '--disable-dev-shm-usage', '--disable-gpu']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 900},
        )
        page = ctx.new_page()

        # Run all sources
        all_leads += scrape_state_warrants(page)   # easiest — do first
        all_leads += scrape_tax_foreclosure(page)
        all_leads += scrape_probate(page)
        all_leads += scrape_tax_delinquent(page)   # slowest — A-Z sweep last

        browser.close()

    # Deduplicate
    seen, unique = set(), []
    for l in all_leads:
        if l['id'] not in seen:
            seen.add(l['id'])
            unique.append(l)
    all_leads = unique

    # Sort by type priority
    order = {'tax-foreclosure': 0, 'probate': 1, 'state-warrant': 2, 'tax-delinquent': 3}
    all_leads.sort(key=lambda l: order.get(l['type'], 9))

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
    log.info(f'Done — {len(all_leads)} total leads')
    for k, v in output['sources'].items():
        log.info(f'  {k}: {v}')
    log.info('=' * 60)

if __name__ == '__main__':
    main()
