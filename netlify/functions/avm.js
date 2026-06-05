// ──────────────────────────────────────────────────────────
// AVM (Estimated ARV) lookup via RentCast.
// Called by INTEL right before pushing a lead to Monday so the
// Zapier webhook payload can include estimated_arv.
//
// Request: POST { address, propertyType?, bedrooms?, bathrooms?, squareFootage? }
// Response: { success, arv, priceRangeLow, priceRangeHigh, confidence, message? }
// ──────────────────────────────────────────────────────────

const RENTCAST_BASE = 'https://api.rentcast.io/v1/avm/value';
const API_KEY = process.env.RENTCAST_API_KEY;

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  let body;
  try {
    body = JSON.parse(event.body || '{}');
  } catch {
    return {
      statusCode: 400,
      body: JSON.stringify({ success: false, message: 'Invalid JSON body' }),
    };
  }

  const { address, propertyType, bedrooms, bathrooms, squareFootage } = body;
  if (!address || typeof address !== 'string') {
    return {
      statusCode: 400,
      body: JSON.stringify({ success: false, message: 'address required' }),
    };
  }

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        success: false,
        message: 'RENTCAST_API_KEY not configured on Netlify',
      }),
    };
  }

  // Build query — only include optional fields if provided, so RentCast can
  // fall back to its own subject-property lookup when we don't have specifics
  const params = new URLSearchParams({ address });
  if (propertyType)   params.set('propertyType',   propertyType);
  if (bedrooms)       params.set('bedrooms',       String(bedrooms));
  if (bathrooms)      params.set('bathrooms',      String(bathrooms));
  if (squareFootage)  params.set('squareFootage',  String(squareFootage));

  try {
    const res = await fetch(`${RENTCAST_BASE}?${params.toString()}`, {
      method: 'GET',
      headers: {
        'X-Api-Key': API_KEY,
        Accept: 'application/json',
      },
    });

    const text = await res.text();
    if (!res.ok) {
      // 404 = no AVM available, not really an error
      return {
        statusCode: 200,
        body: JSON.stringify({
          success: false,
          arv: null,
          message: `RentCast ${res.status}: ${text.substring(0, 200)}`,
        }),
      };
    }

    const data = JSON.parse(text);
    // RentCast returns: { price, priceRangeLow, priceRangeHigh, latitude, longitude,
    //                     comparables: [...], subjectProperty: {...} }
    return {
      statusCode: 200,
      body: JSON.stringify({
        success: true,
        arv:            data.price          || null,
        priceRangeLow:  data.priceRangeLow  || null,
        priceRangeHigh: data.priceRangeHigh || null,
        confidence:     data.comparables?.length || 0,  // # of comps used
      }),
    };
  } catch (err) {
    return {
      statusCode: 200,
      body: JSON.stringify({
        success: false,
        arv: null,
        message: `Network error: ${err.message || String(err)}`,
      }),
    };
  }
};
