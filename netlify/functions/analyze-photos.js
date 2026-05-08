// ── INTEL · Zillow Photo Analysis ──────────────────────────────────────────
// Pipeline: ScraperAPI fetches Zillow → extract photos from __NEXT_DATA__
// → download each photo → base64 encode → Claude vision scores them

const ANTHROPIC_VERSION = '2023-06-01';
const CLAUDE_MODEL      = 'claude-sonnet-4-6';
const MAX_PHOTOS        = 10;            // bumped from 6 → 10 to ensure interior coverage
const MAX_PHOTO_BYTES   = 1024 * 1024;   // 1 MB per photo cap (safety)

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
    const scraperUrl =
      `https://api.scraperapi.com/?api_key=${SCRAPERAPI_KEY}` +
      `&url=${encodeURIComponent(zillowUrl)}` +
      `&premium=true&render=true&country_code=us`;

    const scraperRes = await fetch(scraperUrl, { signal: AbortSignal.timeout(90000) });
    if (!scraperRes.ok) {
      throw new Error(`ScraperAPI returned ${scraperRes.status}`);
    }
    const html = await scraperRes.text();

    // ── Step 2: Extract photo URLs (deduped by hash) ──
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

    // ── Step 3: Pick top N unique photos and download them as base64 ──
    const photosToFetch = photos.slice(0, MAX_PHOTOS);
    const downloaded = await downloadPhotos(photosToFetch);

    if (downloaded.length === 0) {
      return {
        statusCode: 200,
        body: JSON.stringify({
          success: false,
          error:   'photo_download_failed',
          message: 'Could not download any photos from Zillow CDN',
          _debug:  { photos_attempted: photosToFetch.length },
        }),
      };
    }

    // ── Step 4: Claude vision scoring with base64 images ──
    const analysis = await analyzeWithClaude(downloaded, ANTHROPIC_KEY);

    return {
      statusCode: 200,
      body: JSON.stringify({
        success:         true,
        ...analysis,
        photos_analyzed: downloaded.length,
        photo_urls:      photosToFetch,
        timestamp:       new Date().toISOString(),
        _debug: {
          version:         'v5-10-photos',
          model:           CLAUDE_MODEL,
          unique_photos:   photos.length,
          downloaded_ok:   downloaded.length,
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

// ─── Photo extraction with hash-based dedupe ──────────────────────────────
//
// Zillow stores each photo at multiple URLs (preview, 768px JPG, 768px WebP,
// 1536px, etc.) — all sharing the same photo hash. Dedupe by hash, prefer
// 768px JPG. Strict regex anchored at start AND end avoids matching JSON
// substrings that contain CDN URLs.
function extractPhotos(html) {
  try {
    const match = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
    if (!match) return [];

    const data        = JSON.parse(match[1]);
    const byHash      = new Map();  // hash → { url, score }
    const seen        = new WeakSet();

    function consider(url) {
      // Strict validation: must be a clean photo URL, not a JSON fragment.
      // Pattern: https://photos.zillowstatic.com/fp/<HASH>-<SUFFIX>.<EXT>
      const m = url.match(/^https:\/\/photos\.zillowstatic\.com\/fp\/([a-f0-9]+)-[a-z_0-9]+\.(jpg|jpeg|png|webp)$/i);
      if (!m) return;

      const hash  = m[1];
      const ext   = m[2].toLowerCase();
      const isJpg = ext === 'jpg' || ext === 'jpeg';
      const is768 = /-cc_ft_768\./.test(url);

      // Prefer 768px JPG. Score each candidate, keep the highest.
      const score = (is768 ? 2 : 0) + (isJpg ? 1 : 0);
      const existing = byHash.get(hash);
      if (!existing || score > existing.score) {
        // Force 768px JPG if we have a different size — gives consistent payload
        const normalized = url.replace(/-cc_ft_\d+\./, '-cc_ft_768.').replace(/\.webp$/i, '.jpg');
        byHash.set(hash, { url: normalized, score });
      }
    }

    function walk(obj) {
      if (!obj || typeof obj !== 'object' || seen.has(obj)) return;
      seen.add(obj);

      if (Array.isArray(obj)) { obj.forEach(walk); return; }

      for (const [, val] of Object.entries(obj)) {
        if (typeof val === 'string') {
          consider(val);
        } else if (typeof val === 'object') {
          walk(val);
        }
      }
    }

    walk(data);

    try {
      const cacheStr = data?.props?.pageProps?.componentProps?.gdpClientCache;
      if (cacheStr && typeof cacheStr === 'string') {
        walk(JSON.parse(cacheStr));
      }
    } catch { /* ignore */ }

    return Array.from(byHash.values()).map(v => v.url);
  } catch (err) {
    console.error('Photo extraction error:', err);
    return [];
  }
}

// ─── Download photos and convert to base64 ────────────────────────────────
async function downloadPhotos(urls) {
  const results = await Promise.all(urls.map(async (url) => {
    try {
      const res = await fetch(url, {
        headers: {
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          'Accept':     'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8',
          'Referer':    'https://www.zillow.com/',
        },
        signal: AbortSignal.timeout(15000),
      });

      if (!res.ok) {
        console.warn(`Photo download failed: ${res.status} ${url}`);
        return null;
      }

      const buffer = Buffer.from(await res.arrayBuffer());

      if (buffer.length > MAX_PHOTO_BYTES) {
        console.warn(`Photo too large (${buffer.length} bytes), skipping: ${url}`);
        return null;
      }

      const ext = url.toLowerCase().match(/\.(jpe?g|png|webp|gif)/);
      const extToMime = { jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', webp: 'image/webp', gif: 'image/gif' };
      const mediaType = extToMime[ext?.[1]] || 'image/jpeg';

      return {
        media_type: mediaType,
        data:       buffer.toString('base64'),
        url,
      };
    } catch (err) {
      console.warn(`Photo download error for ${url}:`, err.message);
      return null;
    }
  }));

  return results.filter(Boolean);
}

// ─── Claude vision analysis ────────────────────────────────────────────────
async function analyzeWithClaude(photos, apiKey) {
  const content = [
    {
      type: 'text',
      text: `You are analyzing photos of a residential property for a real estate wholesale investor. The investor is looking for properties that NEED renovation work — homes that have already been flipped or extensively remodeled have NO opportunity for value-add and should be SKIPPED.

You are receiving up to 10 photos from this listing. Zillow often orders exterior shots first, so don't assume the listing is exterior-only just because the early photos are exteriors — review ALL photos before judging interior coverage. Focus your verdict primarily on interior rooms (kitchen, bathrooms, living areas) when they are visible.

Pay close attention to:
- Kitchen: cabinets (modern shaker/painted vs dated wood), countertops (granite/quartz vs laminate/tile), appliances (stainless vs older), backsplash, hardware
- Bathrooms: vanities, tile work, fixtures, glass shower enclosures vs old tub/tile combos, dated colors (pink/blue/almond)
- Flooring: LVP/hardwood/modern tile vs original carpet, old vinyl, dated patterns
- Paint and walls: fresh modern colors vs wallpaper, popcorn ceilings, wood paneling
- Fixtures and hardware: modern lighting vs brass/dated, ceiling fans
- Overall: staging quality, "Instagram-ready" feel = recent flip indicator

ALSO assess what photo coverage you have. Some Zillow listings (especially expired/withdrawn ones) only have exterior photos because the seller pulled interior shots when delisting. Be honest about coverage but don't conflate "first few photos are exterior" with "no interior photos available."

Respond with ONLY a JSON object (no other text, no markdown fences):
{
  "status": "renovated" | "partial" | "dated",
  "confidence": 0-100,
  "interior_photos": "full" | "limited" | "none",
  "reasoning": "2-3 sentence explanation citing specific observations from the photos",
  "key_findings": {
    "kitchen": "brief description of what you see, or 'not visible' if no interior kitchen photos",
    "bathrooms": "brief description, or 'not visible'",
    "flooring": "brief description, or 'not visible'",
    "overall": "brief description"
  }
}

Status definitions:
- "renovated" = SKIP. Modern kitchen + modern bathrooms + updated flooring throughout. Recent flip or major remodel. No meaningful value-add for an investor.
- "partial" = MAYBE. Some areas updated but not all (e.g., kitchen redone but bathrooms still dated, or fresh paint over original everything). Could still have meat on the bone.
- "dated" = CHASE. Original or dated finishes throughout. Strong value-add opportunity for an investor doing a flip or BRRRR.

Interior photos definitions:
- "full" = good coverage of kitchen + bathrooms + living spaces across the photos provided
- "limited" = some interior shots but key rooms (kitchen or bathrooms) are missing
- "none" = exterior shots only, no interior visible across ALL photos provided

Confidence calibration: when interior_photos is "none" or "limited", your confidence should be lower (typically 40-60%) since you're inferring condition from incomplete information. Full interior coverage with clear verdict warrants 80-95% confidence.`,
    },
  ];

  for (const photo of photos) {
    content.push({
      type: 'image',
      source: {
        type:       'base64',
        media_type: photo.media_type,
        data:       photo.data,
      },
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

  const cleaned   = responseText.replace(/```json|```/g, '').trim();
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    throw new Error('Claude did not return valid JSON');
  }

  const parsed = JSON.parse(jsonMatch[0]);

  if (!['renovated', 'partial', 'dated'].includes(parsed.status)) {
    parsed.status     = 'partial';
    parsed.confidence = parsed.confidence || 50;
  }
  if (!['full', 'limited', 'none'].includes(parsed.interior_photos)) {
    parsed.interior_photos = 'limited';
  }

  return parsed;
}
