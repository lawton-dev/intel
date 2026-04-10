# INTEL — OneRoof Lead Intelligence

Automated daily scraper and dashboard for Sedgwick County distressed property leads.

## Data Sources

| Source | Type | Frequency |
|--------|------|-----------|
| Sedgwick County Treasurer | Tax Delinquent Real Estate | Daily |
| Sedgwick County Treasurer | Tax Foreclosure Auctions | Daily |
| 18th District Court (DC18) | Probate Docket | Daily |
| Kansas DOR | State Tax Warrants | Daily |
| ATTOM API *(pending)* | Lis Pendens / Foreclosures / REOs | Daily |

## Setup

### 1. Deploy to Netlify

Drag the folder to [app.netlify.com](https://app.netlify.com) (drag-and-drop deploy).
Set the site name to `intel` → your URL will be `intel.oneroofre.net`.

### 2. Set Environment Variables in Netlify

Go to **Site → Environment Variables** and add:

```
SLICK_API_KEY    your SlickText API key
SLICK_ACCOUNT    your SlickText account number
SLICK_TEXTWORD   your textword keyword (e.g. ONEROOF)
```

### 3. Configure the Dashboard

Open `index.html` and update the `CONFIG` block at the top of the script:

```js
const CONFIG = {
  password:      'YourPassword',        // access code for the gate
  zapierWebhook: 'https://hooks.zapier.com/...',  // your Zapier webhook
  slickApiKey:   '',                    // handled server-side
  dataUrl:       '/data/leads.json',
};
```

### 4. Connect GitHub Actions (for daily auto-refresh)

1. Push this repo to GitHub
2. Go to **Settings → Actions → General** and enable workflows
3. The scraper runs automatically at **6:00 AM CT every weekday**
4. To trigger manually: **Actions → INTEL Daily Scraper → Run workflow**

### 5. Link Netlify to GitHub

In Netlify: **Site → Deploys → Link to Git**
→ Netlify will auto-deploy every time GitHub Actions commits new lead data.

## Adding ATTOM Pre-Foreclosure (when unlocked)

Once your ATTOM subscription includes the pre-foreclosure endpoint:

1. Add `ATTOM_API_KEY` to Netlify environment variables
2. In `scripts/scrape.py`, uncomment the `scrape_attom()` function
3. ATTOM will add Lis Pendens, Mortgage Foreclosures, and REOs

## Dashboard Actions

- **Send Text** → fires SMS via SlickText Netlify function
- **Push to Monday** → POSTs lead data to your Zapier webhook
- **Mark Contacted** → persists locally via localStorage (per browser)

## Lead Types

| Badge | Color | Source |
|-------|-------|--------|
| TAX DELINQUENT | Amber | Sedgwick County Treasurer |
| TAX FORECLOSURE | Red | Sedgwick County Treasurer |
| PROBATE | Purple | 18th District Court |
| STATE WARRANT | Orange | Kansas DOR |
| ATTOM LEAD | Blue | ATTOM API *(future)* |
