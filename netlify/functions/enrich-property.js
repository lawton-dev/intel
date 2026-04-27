// netlify/functions/enrich-property.js
//
// BatchData property valuation lookup — returns estimatedValue, estimatedEquity,
// mortgageBalance for a single property by address. NO skip trace (cheaper than
// the full skip-trace endpoint, useful for triaging which leads have enough
// equity to be worth pursuing).
//
// Env var required (Netlify dashboard):
//   BATCHDATA_API_KEY  — your BatchData Bearer token
//
// Frontend usage:
//   POST /.netlify/functions/enrich-property
//   Body: { address: "123 Main St, Houston TX", county: "harris" }
//   Returns: { estimatedValue, estimatedEquity, mortgageBalance, success }

const BATCH_API_URL = 'https://api.batchdata.com/api/v1/property/search';

exports.handler = async (event) => {
  const cors = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers: cors, body: '' };
  }
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: cors, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const apiKey = process.env.BATCHDATA_API_KEY;
  if (!apiKey) {
    return {
      statusCode: 500,
      headers: cors,
      body: JSON.stringify({ error: 'BATCHDATA_API_KEY not configured in Netlify env vars' }),
    };
  }

  let body;
  try { body = JSON.parse(event.body || '{}'); }
  catch { return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { address } = body;
  if (!address) {
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'address required' }) };
  }

  // Parse address into BatchData's expected shape: "street, city ST" or "street, city, ST ZIP"
  const parsed = parseAddress(address);
  if (!parsed.street || !parsed.state) {
    return { statusCode: 400, headers: cors, body: JSON.stringify({ error: 'Could not parse address', address }) };
  }

  try {
    const apiBody = {
      requests: [{
        searchCriteria: {
          query: `${parsed.street}, ${parsed.city} ${parsed.state}${parsed.zip ? ' ' + parsed.zip : ''}`,
        },
        options: {
          take: 1,
          skipTrace: false,           // value lookup only — keep cost down
          useYearBuilt: false,
        },
      }],
    };

    const res = await fetch(BATCH_API_URL, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type':  'application/json',
        'Accept':        'application/json',
      },
      body: JSON.stringify(apiBody),
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error('BatchData error', res.status, errText);
      return {
        statusCode: 502,
        headers: cors,
        body: JSON.stringify({ error: 'BatchData request failed', status: res.status }),
      };
    }

    const data = await res.json();

    // Drill into BatchData's response shape — adjust if your account uses a different envelope
    const property = data?.results?.properties?.[0]
                  || data?.results?.[0]
                  || (Array.isArray(data?.properties) && data.properties[0])
                  || null;

    if (!property) {
      return { statusCode: 200, headers: cors, body: JSON.stringify({ success: false, reason: 'no match' }) };
    }

    // BatchData puts valuation in a few possible places depending on account/plan
    const valuation = property.valuation || property.assessment || {};
    const mortgage  = property.mortgage  || property.openLien   || {};

    const estimatedValue =
      num(valuation.estimatedValue)
      || num(valuation.priceRangeMax)
      || num(property.estimatedValue)
      || num(property.assessedValue?.total);

    const mortgageBalance =
      num(mortgage.totalOpenLienBalance)
      || num(mortgage.estimatedBalance)
      || num(mortgage.totalLoanBalance)
      || 0;

    let estimatedEquity = num(valuation.equityCurrentEstimatedBalance) || num(property.equity);
    if (!estimatedEquity && estimatedValue && mortgageBalance) {
      estimatedEquity = estimatedValue - mortgageBalance;
    }

    return {
      statusCode: 200,
      headers: cors,
      body: JSON.stringify({
        success: !!estimatedValue,
        estimatedValue:  estimatedValue  || null,
        estimatedEquity: estimatedEquity || null,
        mortgageBalance: mortgageBalance || null,
      }),
    };
  } catch (e) {
    console.error('enrich-property exception', e);
    return {
      statusCode: 500,
      headers: cors,
      body: JSON.stringify({ error: 'Server error', message: e.message }),
    };
  }
};

// ─── helpers ─────────────────────────────────────────────────────────

function num(v) {
  if (v == null || v === '') return 0;
  const n = typeof v === 'number' ? v : parseFloat(String(v).replace(/[^0-9.-]/g, ''));
  return isFinite(n) ? n : 0;
}

// Parse a casual address like "8519 Windy Thicket Ln, Cypress TX" or
// "8519 Windy Thicket Ln, Cypress, TX 77433" into parts.
function parseAddress(addr) {
  const parts = addr.split(',').map(s => s.trim()).filter(Boolean);
  let street = '', city = '', state = '', zip = '';

  if (parts.length >= 3) {
    // "street, city, ST ZIP"
    street = parts[0];
    city   = parts[1];
    const m = parts[2].match(/^([A-Za-z]{2})\s*(\d{5})?/);
    if (m) { state = m[1].toUpperCase(); zip = m[2] || ''; }
  } else if (parts.length === 2) {
    // "street, city ST" or "street, city ST ZIP"
    street = parts[0];
    const m = parts[1].match(/^(.+?)\s+([A-Za-z]{2})\s*(\d{5})?$/);
    if (m) {
      city  = m[1].trim();
      state = m[2].toUpperCase();
      zip   = m[3] || '';
    } else {
      city = parts[1];
    }
  } else if (parts.length === 1) {
    street = parts[0];
  }

  return { street, city, state, zip };
}
