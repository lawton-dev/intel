// ── INTEL · Skip Trace via BatchData ────────────────────────────────────────
// POSTs owner name + address to BatchData, returns phone number

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
  if (!owner && !address) {
    return { statusCode: 400, body: JSON.stringify({ error: 'owner or address required' }) };
  }

  // Parse address components
  const addrParts = (address || '').split(',');
  const street    = (addrParts[0] || '').trim();
  const cityState = (addrParts[1] || '').trim();
  const zip       = (addrParts[2] || '').trim();

  // Parse city/state
  const csMatch  = cityState.match(/^(.*?)\s+([A-Z]{2})$/);
  const city     = csMatch ? csMatch[1].trim() : cityState;
  const state    = csMatch ? csMatch[2] : 'KS';

  // Parse owner name (Last First or First Last)
  const nameParts   = (owner || '').replace(/^Estate of\s+/i, '').trim().split(/\s+/);
  const firstName   = nameParts.length > 1 ? nameParts.slice(0, -1).join(' ') : '';
  const lastName    = nameParts[nameParts.length - 1] || nameParts[0] || '';

  try {
    const res = await fetch('https://api.batchdata.com/api/v1/property/skip-trace', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify({
        requests: [{
          propertyAddress: {
            street:  street,
            city:    city || 'Wichita',
            state:   state,
            zip:     zip || '',
          },
          person: {
            firstName: firstName,
            lastName:  lastName,
          }
        }]
      })
    });

    const data = await res.json();

    // Navigate BatchData response structure
    const result   = data?.results?.[0] || data?.result || data;
    const persons  = result?.persons || result?.owner?.persons || [];
    const phones   = [];

    persons.forEach(p => {
      (p.phones || p.phoneNumbers || []).forEach(ph => {
        const num = ph.phone || ph.phoneNumber || ph.number || ph;
        if (num && typeof num === 'string' && num.replace(/\D/g,'').length >= 10) {
          phones.push({
            number: formatPhone(num),
            type:   ph.phoneType || ph.type || 'unknown',
            score:  ph.confidenceScore || ph.score || 0,
          });
        }
      });
    });

    // Sort by confidence score, prefer mobile
    phones.sort((a,b) => {
      if (a.type === 'mobile' && b.type !== 'mobile') return -1;
      if (b.type === 'mobile' && a.type !== 'mobile') return 1;
      return (b.score - a.score);
    });

    if (phones.length > 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          success: true,
          phone:   phones[0].number,
          allPhones: phones,
        })
      };
    } else {
      return {
        statusCode: 200,
        body: JSON.stringify({ success: false, phone: null, message: 'No phone found' })
      };
    }

  } catch (err) {
    console.error('Skip trace error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Skip trace failed', detail: err.message })
    };
  }
};

function formatPhone(raw) {
  const digits = raw.replace(/\D/g, '').replace(/^1/, '');
  if (digits.length === 10) {
    return `(${digits.slice(0,3)}) ${digits.slice(3,6)}-${digits.slice(6)}`;
  }
  return raw;
}
