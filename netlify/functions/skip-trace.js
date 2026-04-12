// ── INTEL · Skip Trace DEBUG — returns raw BatchData response ────────────────
// Temporary debug version — shows exact response so we can fix parsing

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const API_KEY = process.env.BATCHDATA_API_KEY;
  if (!API_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'BatchData not configured' }) };
  }

  let body;
  try { body = JSON.parse(event.body); }
  catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { owner, address } = body;

  // Parse address
  const addrParts = (address || '').split(',');
  const street    = (addrParts[0] || '').trim();
  const cityState = (addrParts[1] || '').trim();
  const csMatch   = cityState.match(/^(.*?)\s+([A-Z]{2})\s*(\d{5})?\d*/);
  const city      = csMatch ? csMatch[1].trim() : 'Wichita';
  const state     = csMatch ? csMatch[2] : 'KS';
  let   zip       = csMatch?.[3] || '';
  if (!zip) {
    const zm = (address || '').match(/(\d{5})\d*/);
    if (zm) zip = zm[1];
  }

  // Parse name
  const cleanName = (owner || '').replace(/^Estate of\s+/i, '').trim();
  const nameParts = cleanName.split(/\s+/);
  const lastName  = nameParts[0] || '';
  const firstName = nameParts.slice(1).join(' ') || '';

  const requestBody = {
    requests: [{
      propertyAddress: { street, city, state, zip },
      person: { firstName, lastName },
    }]
  };

  try {
    // Try v3 endpoint
    const res = await fetch('https://api.batchdata.com/api/v3/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify(requestBody),
    });

    const rawText = await res.text();
    let parsed;
    try { parsed = JSON.parse(rawText); } catch { parsed = rawText; }

    // Return EVERYTHING so we can see the structure
    return {
      statusCode: 200,
      body: JSON.stringify({
        debug: true,
        requestSent: requestBody,
        httpStatus: res.status,
        rawResponse: parsed,
        // Try to pull phone with various paths
        phonePaths: {
          'results[0].PHONE1':           parsed?.results?.[0]?.PHONE1,
          'results[0].phone1':           parsed?.results?.[0]?.phone1,
          'results[0].phones[0]':        parsed?.results?.[0]?.phones?.[0],
          'data[0].PHONE1':              parsed?.data?.[0]?.PHONE1,
          'result.PHONE1':               parsed?.result?.PHONE1,
          'status':                      parsed?.status,
          'results length':              parsed?.results?.length,
          'top-level keys':              Object.keys(parsed || {}),
          'results[0] keys':             Object.keys(parsed?.results?.[0] || {}),
        }
      })
    };

  } catch (err) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: err.message, stack: err.stack })
    };
  }
};
