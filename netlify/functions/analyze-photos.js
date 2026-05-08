// ── INTEL · Zillow Photo Analysis ──────────────────────────────────────────
// Pipeline: ScraperAPI fetches Zillow → extract photos from __NEXT_DATA__
// → Claude vision scores them as renovated/partial/dated → cache + return

const ANTHROPIC_VERSION = '2023-06-01';
const CLAUDE_MODEL      = 'claude-sonnet-4-6';
const MAX_PHOTOS        = 6;

exports.handler = async (event) => {
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: JSON.stringify({ error: 'Method not allowed' }) };
  }

  const SCRAPERAPI_KEY = process.env.SCRAPERAPI_KEY;
  const ANTHROPIC_KEY  = process.env.ANTHROPIC_API_KEY;

  if (!SCRAPERAPI_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'ScraperAPI not configured' }) };
  }
  if (!ANTHROPIC_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Anthropic API not configured' }) };
  }

  let body;
  try { body = JSON.parse(event.body); }
  catch { return { statusCode: 400, body: JSON.stringify({ error: 'Invalid JSON' }) }; }

  const { zillowUrl } = body;
  if (!zillowUrl || !/zillow\.com/i.test(zillowUrl)) {
    return { statusCode: 400, body: JSON.stringify({ error: 'Valid zillowUrl required' }) };
  }

  try {
    // ── Step 1: Fetch Zillow listing via ScraperAPI ──
    // premium=true uses residential IPs (datacenter IPs are blocked by Imperva)
    // render=true executes JS so __NEXT_DATA__ is fully populated
    const scraperUrl =
      `https://api.scraperapi.com/?api_key=${SCRAPERAPI_KEY}` +
      `&url=${encodeURIComponent(zillowUrl)}` +
      `&premium=true&render=true&country_code=us`;

    const scraperRes = await fetch(scraperUrl, { signal: AbortSignal.timeout(90000) });
    if (!scraperRes.ok) {
      throw new Error(`ScraperAPI returned ${scraperRes.status}`);
    }
    const html = await scraperRes.text();

    // ── Step 2: Extract photo URLs ──
    const photos = extractPhotos(html);
    if (photos.length === 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          success: false,
          error:   'no_photos',
          message: 'Could not extract photos from listing (Zillow may have changed structure)',
        }),
      };
    }

    // ── Step 3: Pick top N photos. Zillow orders best/most-representative first ──
    const photosToAnalyze = photos.slice(0, MAX_PHOTOS);

    // ── Step 4: Claude vision scoring ──
    const analysis = await analyzeWithClaude(photosToAnalyze, ANTHROPIC_KEY);

    return {
      statusCode: 200,
      body: JSON.stringify({
        success:         true,
        ...analysis,
        photos_analyzed: photosToAnalyze.length,
        photo_urls:      photosToAnalyze,
        timestamp:       new Date().toISOString(),
        _debug: {
          version:      'v1-scraperapi-claude-vision',
          model:        CLAUDE_MODEL,
          total_photos: photos.length,
        },
      }),
    };

  } catch (err) {
    console.error('Photo analysis error:', err);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Analysis failed', detail: err.message }),
    };
  }
};

// ─── Photo extraction ──────────────────────────────────────────────────────
// Walks the entire __NEXT_DATA__ tree looking for Zillow CDN photo URLs.
// Belt-and-suspenders approach — Zillow changes their JSON shape often,
// so we don't rely on a specific path.
function extractPhotos(html) {
  try {
    const match = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (!match) return [];

    const data  = JSON.parse(match[1]);
    const found = new Set();
    const seen  = new WeakSet();

    function walk(obj) {
      if (!obj || typeof obj !== 'object' || seen.has(obj)) return;
      seen.add(obj);

      if (Array.isArray(obj)) { obj.forEach(walk); return; }

      for (const [, val] of Object.entries(obj)) {
        if (typeof val === 'string') {
          // Zillow CDN photo URL pattern
          if (/photos\.zillowstatic\.com.*\.(jpg|jpeg|png|webp)/i.test(val)) {
            // Force highest available resolution (uncropped, 1536px wide)
            const hiRes = val.replace(/-cc_ft_\d+/, '-cc_ft_1536');
            found.add(hiRes);
          }
        } else if (typeof val === 'object') {
          walk(val);
        }
      }
    }

    walk(data);

    // gdpClientCache is sometimes a stringified JSON nested inside __NEXT_DATA__
    try {
      const cacheStr = data?.props?.pageProps?.componentProps?.gdpClientCache;
      if (cacheStr && typeof cacheStr === 'string') {
        walk(JSON.parse(cacheStr));
      }
    } catch { /* ignore */ }

    return Array.from(found);
  } catch (err) {
    console.error('Photo extraction error:', err);
    return [];
  }
}

// ─── Claude vision analysis ────────────────────────────────────────────────
async function analyzeWithClaude(photoUrls, apiKey) {
  const content = [
    {
      type: 'text',
      text: `You are analyzing photos of a residential property for a real estate wholesale investor. The investor is looking for properties that NEED renovation work — homes that have already been flipped or extensively remodeled have NO opportunity for value-add and should be SKIPPED.

Look at all photos provided and assess the overall renovation status. Pay close attention to:
- Kitchen: cabinets (modern shaker/painted vs dated wood), countertops (granite/quartz vs laminate/tile), appliances (stainless vs older), backsplash, hardware
- Bathrooms: vanities, tile work, fixtures, glass shower enclosures vs old tub/tile combos, dated colors (pink/blue/almond)
- Flooring: LVP/hardwood/modern tile vs original carpet, old vinyl, dated patterns
- Paint and walls: fresh modern colors vs wallpaper, popcorn ceilings, wood paneling
- Fixtures and hardware: modern lighting vs brass/dated, ceiling fans
- Overall: staging quality, "Instagram-ready" feel = recent flip indicator

Respond with ONLY a JSON object (no other text, no markdown fences):
{
  "status": "renovated" | "partial" | "dated",
  "confidence": 0-100,
  "reasoning": "2-3 sentence explanation citing specific observations from the photos",
  "key_findings": {
    "kitchen": "brief description of what you see",
    "bathrooms": "brief description",
    "flooring": "brief description",
    "overall": "brief description"
  }
}

Status definitions:
- "renovated" = SKIP. Modern kitchen + modern bathrooms + updated flooring throughout. Recent flip or major remodel. No meaningful value-add for an investor.
- "partial" = MAYBE. Some areas updated but not all (e.g., kitchen redone but bathrooms still dated, or fresh paint over original everything). Could still have meat on the bone.
- "dated" = CHASE. Original or dated finishes throughout. Strong value-add opportunity for an investor doing a flip or BRRRR.`,
    },
  ];

  for (const url of photoUrls) {
    content.push({
      type:   'image',
      source: { type: 'url', url },
    });
  }

  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type':      'application/json',
      'x-api-key':         apiKey,
      'anthropic-version': ANTHROPIC_VERSION,
    },
    body: JSON.stringify({
      model:      CLAUDE_MODEL,
      max_tokens: 1024,
      messages:   [{ role: 'user', content }],
    }),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Claude API ${res.status}: ${errText.substring(0, 200)}`);
  }

  const data         = await res.json();
  const responseText = data.content?.[0]?.text || '';

  // Strip any markdown fences just in case, then extract the JSON object
  const cleaned   = responseText.replace(/```json|```/g, '').trim();
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    throw new Error('Claude did not return valid JSON');
  }

  const parsed = JSON.parse(jsonMatch[0]);

  // Sanity check: status must be one of our three values
  if (!['renovated', 'partial', 'dated'].includes(parsed.status)) {
    parsed.status     = 'partial';
    parsed.confidence = parsed.confidence || 50;
  }

  return parsed;
}
