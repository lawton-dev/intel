// ── INTEL · Photo Analysis Frontend Module ────────────────────────────────
// Drop into intel/index.html. Exposes:
//   window.analyzePhotos(propertyId, zillowUrl)    — fetch + score (or use cache)
//   window.renderCachedPhotoResult(id, zillowUrl)  — render cache only, no API call
//   window.renderPhotoBadge(zillowUrl)             — small inline pill HTML
//   window.clearPhotoCache(zillowUrl?)             — clear one or all

(function () {
  const CACHE_KEY      = 'intel_photo_analysis_v1';
  const CACHE_TTL_DAYS = 30;  // listing photos rarely change within a month

  // ─── Cache helpers ───────────────────────────────────────────────────────
  function getCache() {
    try { return JSON.parse(localStorage.getItem(CACHE_KEY) || '{}'); }
    catch { return {}; }
  }

  function setCache(cache) {
    try { localStorage.setItem(CACHE_KEY, JSON.stringify(cache)); }
    catch (e) { console.warn('Photo cache write failed:', e); }
  }

  function getCached(zillowUrl) {
    const cache = getCache();
    const hit   = cache[zillowUrl];
    if (!hit) return null;

    const ageMs = Date.now() - new Date(hit.cached_at).getTime();
    if (ageMs > CACHE_TTL_DAYS * 86400 * 1000) {
      delete cache[zillowUrl];
      setCache(cache);
      return null;
    }
    return hit;
  }

  function saveCached(zillowUrl, result) {
    const cache = getCache();
    cache[zillowUrl] = { ...result, cached_at: new Date().toISOString() };
    setCache(cache);
  }

  // ─── Status styling — matches INTEL dark theme ────────────────────────────
  const STATUS_STYLES = {
    renovated: {
      bg:    'rgba(239,68,68,0.08)',
      fg:    '#ef4444',
      border:'rgba(239,68,68,0.4)',
      label: 'RENOVATED',
      emoji: '⛔',
      verb:  'SKIP',
    },
    partial: {
      bg:    'rgba(245,158,11,0.08)',
      fg:    '#f59e0b',
      border:'rgba(245,158,11,0.4)',
      label: 'PARTIAL',
      emoji: '⚠',
      verb:  'MAYBE',
    },
    dated: {
      bg:    'rgba(34,197,94,0.08)',
      fg:    '#22c55e',
      border:'rgba(34,197,94,0.4)',
      label: 'DATED',
      emoji: '✓',
      verb:  'CHASE',
    },
  };

  // ─── Public API ──────────────────────────────────────────────────────────

  // Small inline pill for table rows. Returns '' if not yet analyzed.
  window.renderPhotoBadge = function (zillowUrl) {
    if (!zillowUrl) return '';
    const cached = getCached(zillowUrl);
    if (!cached || !cached.success) return '';
    const s = STATUS_STYLES[cached.status] || STATUS_STYLES.partial;
    return `<span style="
      display:inline-flex;align-items:center;gap:4px;
      background:${s.bg};color:${s.fg};border:1px solid ${s.border};
      padding:2px 6px;border-radius:2px;font-size:8px;font-weight:600;
      letter-spacing:1px;font-family:'JetBrains Mono',monospace;
    " title="${s.label} — ${s.verb} (${cached.confidence}% confident)">
      ${s.emoji} ${s.label}
    </span>`;
  };

  // Cache-only render. Returns true if a cached result was rendered, false otherwise.
  // Use this on panel-open to show prior analysis without firing an API call.
  window.renderCachedPhotoResult = function (propertyId, zillowUrl) {
    if (!zillowUrl) return false;
    const cached = getCached(zillowUrl);
    if (!cached) return false;
    const resultEl = document.querySelector(`#photo-result-${propertyId}`);
    const btnEl    = document.querySelector(`#analyze-btn-${propertyId}`);
    if (resultEl) renderResult(resultEl, cached, true);
    if (btnEl) btnEl.textContent = '📷 RE-ANALYZE PHOTOS';
    return true;
  };

  // Full analyze flow — call from "Analyze Photos" button.
  window.analyzePhotos = async function (propertyId, zillowUrl) {
    if (!zillowUrl) {
      alert('No Zillow URL on this property');
      return;
    }

    const resultEl = document.querySelector(`#photo-result-${propertyId}`);
    const btnEl    = document.querySelector(`#analyze-btn-${propertyId}`);

    // Loading state
    if (btnEl) {
      btnEl.disabled = true;
      btnEl.textContent = '⏳ ANALYZING...';
    }
    if (resultEl) {
      resultEl.innerHTML = `
        <div style="color:#8b96b0;font-size:11px;padding:10px 0;font-family:'JetBrains Mono',monospace;letter-spacing:1px;">
          Fetching listing and scoring photos... (~15-30 seconds)
        </div>
      `;
    }

    try {
      const res = await fetch('/.netlify/functions/analyze-photos', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ zillowUrl }),
      });

      const data = await res.json();
      if (!data.success) {
        throw new Error(data.error || data.message || 'Analysis failed');
      }

      saveCached(zillowUrl, data);
      renderResult(resultEl, data, false);

    } catch (err) {
      if (resultEl) {
        resultEl.innerHTML = `
          <div style="color:#ef4444;font-size:11px;padding:10px 0;font-family:'JetBrains Mono',monospace;">
            Error: ${err.message}
          </div>
        `;
      }
    } finally {
      if (btnEl) {
        btnEl.disabled    = false;
        btnEl.textContent = '📷 RE-ANALYZE PHOTOS';
      }
    }
  };

  // Clear cache — pass a URL to clear one entry, or no args to clear all
  window.clearPhotoCache = function (zillowUrl) {
    if (zillowUrl) {
      const cache = getCache();
      delete cache[zillowUrl];
      setCache(cache);
    } else {
      localStorage.removeItem(CACHE_KEY);
    }
  };

  // ─── Result rendering ────────────────────────────────────────────────────
  function renderResult(el, result, fromCache) {
    if (!el) return;
    const s = STATUS_STYLES[result.status] || STATUS_STYLES.partial;

    const findings   = result.key_findings || {};
    const cachedNote = fromCache
      ? `<span style="color:#5f6473;font-size:9px;margin-left:8px;letter-spacing:1px;">
           CACHED ${formatAge(result.cached_at).toUpperCase()}
         </span>`
      : '';

    el.innerHTML = `
      <div style="
        background:${s.bg};border:1px solid ${s.border};border-radius:4px;
        padding:12px;margin-top:10px;
      ">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;flex-wrap:wrap;">
          <span style="
            background:${s.fg};color:#0a0c0f;
            padding:4px 10px;border-radius:2px;
            font-weight:700;font-size:10px;letter-spacing:1.5px;
            font-family:'JetBrains Mono',monospace;
          ">${s.emoji} ${s.label} — ${s.verb}</span>
          <span style="color:${s.fg};font-size:10px;font-family:'JetBrains Mono',monospace;letter-spacing:1px;">
            ${result.confidence}% CONFIDENT
          </span>
          ${cachedNote}
        </div>

        <div style="
          color:#e8ecf4;font-size:11px;line-height:1.6;
          margin-bottom:10px;font-family:'DM Sans',sans-serif;
        ">
          ${escapeHtml(result.reasoning || '')}
        </div>

        ${findings.kitchen || findings.bathrooms ? `
          <details style="font-size:10px;color:#b0b8cc;font-family:'DM Sans',sans-serif;">
            <summary style="cursor:pointer;color:${s.fg};font-weight:600;letter-spacing:1px;font-family:'JetBrains Mono',monospace;font-size:9px;">
              ▸ DETAILED FINDINGS
            </summary>
            <div style="
              padding:8px 0 4px;display:grid;gap:6px;
              grid-template-columns:1fr;line-height:1.5;
            ">
              ${findings.kitchen   ? `<div><b style="color:#8b96b0;font-weight:500;">Kitchen:</b> ${escapeHtml(findings.kitchen)}</div>`   : ''}
              ${findings.bathrooms ? `<div><b style="color:#8b96b0;font-weight:500;">Bathrooms:</b> ${escapeHtml(findings.bathrooms)}</div>` : ''}
              ${findings.flooring  ? `<div><b style="color:#8b96b0;font-weight:500;">Flooring:</b> ${escapeHtml(findings.flooring)}</div>`  : ''}
              ${findings.overall   ? `<div><b style="color:#8b96b0;font-weight:500;">Overall:</b> ${escapeHtml(findings.overall)}</div>`   : ''}
            </div>
          </details>
        ` : ''}

        ${result.photos_analyzed ? `
          <div style="color:#5f6473;font-size:9px;margin-top:8px;letter-spacing:1px;font-family:'JetBrains Mono',monospace;">
            ANALYZED ${result.photos_analyzed} PHOTOS
          </div>
        ` : ''}
      </div>
    `;
  }

  function formatAge(iso) {
    const ms   = Date.now() - new Date(iso).getTime();
    const days = Math.floor(ms / 86400000);
    if (days === 0) return 'today';
    if (days === 1) return '1 day ago';
    return `${days} days ago`;
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }
})();
