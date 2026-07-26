#!/usr/bin/env node
/**
 * refresh-pricempire.mjs — pull all-market prices from PriceEmpire v4 and store
 * them in Supabase (`skin_market_prices`), one row per item with every market's
 * price in a JSONB blob. The app reads that blob and lets the user switch which
 * market's price is shown WITHOUT extra requests.
 *
 * Why Node (not the Cloudflare Worker)? The PriceEmpire payload is huge (~32 MB
 * base) and the prices endpoint is bulk-only (always ALL ~40k items, max 15
 * sources/request). Parsing that in a Worker dies with error 1102 (CPU/mem).
 * Node has no such limit.
 *
 * Usage:
 *   PRICEMPIRE_API_KEY=...  \
 *   SUPABASE_SERVICE_ROLE_KEY=...  \
 *   node scripts/refresh-pricempire.mjs            # fetch + parse + write
 *   node scripts/refresh-pricempire.mjs --dry-run  # fetch + parse only (no DB write)
 *
 * Env:
 *   PRICEMPIRE_API_KEY         (required) PriceEmpire paid API key (Bearer)
 *   SUPABASE_URL               (optional) defaults to the project URL below
 *   SUPABASE_SERVICE_ROLE_KEY  (required unless --dry-run) bypasses RLS for writes
 */

const API_KEY = process.env.PRICEMPIRE_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://mtpdgnsoxcpgcorjftjt.supabase.co';
const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const DRY_RUN = process.argv.includes('--dry-run');

const HOST = 'https://api.pricempire.com';
const APP_ID = 730;

// By default the script fetches through the Cloudflare Worker proxy (/pe-bulk) so the
// PriceEmpire key stays ONLY in the Worker secret — nothing to paste locally or in CI.
// Set PRICEMPIRE_API_KEY to bypass the proxy and call PriceEmpire directly instead.
// PE_PROXY_TOKEN is required for the proxy (no baked-in default: this file lives in a
// PUBLIC repo for free Actions minutes, so the token must come from a secret).
const PROXY_URL = process.env.PE_PROXY_URL || 'https://push-notifications.deplumaks.workers.dev/pe-bulk';
const PROXY_TOKEN = process.env.PE_PROXY_TOKEN || '';
const USE_PROXY = !API_KEY; // proxy unless an explicit key is provided
// Writes go through the Worker too (/pe-upsert, same token) unless a Supabase
// service key is provided — so CI needs NO Supabase credentials at all.
const UPSERT_URL = process.env.PE_UPSERT_URL || PROXY_URL.replace(/\/pe-bulk$/, '/pe-upsert');

// Curated markets to publish (~45). Each: { pe: PriceEmpire provider_key, key: app allPrices key }.
// PriceEmpire allows max 15 sources per request, so these are fetched in 3 batches.
// Mapping is 1:1 except whitemarket → whitemarekt (the app's historical key spelling).
// Gambling sites (csgoempire/csgoroll/clashgg) are intentionally excluded — Apple bans
// gambling links from iOS apps, so we don't surface their prices.
const MARKETS = [
  // ── batch 1 ──
  { pe: 'steam',      key: 'steam' },
  { pe: 'csfloat',    key: 'csfloat' },
  { pe: 'buff163',    key: 'buff163' },
  { pe: 'skinport',   key: 'skinport' },
  { pe: 'dmarket',    key: 'dmarket' },
  { pe: 'waxpeer',    key: 'waxpeer' },
  { pe: 'tradeit',    key: 'tradeit' },
  { pe: 'csmoney',    key: 'csmoney' },
  { pe: 'gamerpay',   key: 'gamerpay' },
  { pe: 'shadowpay',  key: 'shadowpay' },
  { pe: 'lisskins',   key: 'lisskins' },
  { pe: 'marketcsgo', key: 'marketcsgo' },
  { pe: 'skinbaron',  key: 'skinbaron' },
  // ── batch 2 ──
  { pe: 'haloskins',   key: 'haloskins' },
  { pe: 'whitemarket', key: 'whitemarekt' },
  { pe: 'skinswap',    key: 'skinswap' },
  { pe: 'cstrade',     key: 'cstrade' },
  { pe: 'mannco',      key: 'mannco' },
  { pe: 'rapidskins',  key: 'rapidskins' },
  { pe: 'avanmarket',  key: 'avanmarket' },
  { pe: 'uuskins',     key: 'uuskins' },
  { pe: 'skinplace',   key: 'skinplace' },
  { pe: 'exeskins',    key: 'exeskins' },
  { pe: 'lootfarm',    key: 'lootfarm' },
  { pe: 'c5game',      key: 'c5game' },
  { pe: 'youpin',      key: 'youpin' },
  { pe: 'skinout',     key: 'skinout' },
  { pe: 'skinflow',    key: 'skinflow' },
  // ── batch 3 ──
  { pe: 'skinsmonkey', key: 'skinsmonkey' },
  { pe: 'skindeck',    key: 'skindeck' },
  { pe: 'csdeals',     key: 'csdeals' },
  { pe: 'swapgg',      key: 'swapgg' },
  { pe: 'itrade',      key: 'itrade' },
  { pe: 'buffmarket',  key: 'buffmarket' },
  { pe: 'ecosteam',    key: 'ecosteam' },
  { pe: 'snipeskins',  key: 'snipeskins' },
  { pe: 'skinvault',   key: 'skinvault' },
  { pe: 'gameboost',   key: 'gameboost' },
  { pe: '49skins',     key: '49skins' },
];
// Dropped (negligible coverage, <1k of 37k items): nerf (24), krakatoa (403), skinthunder (895).

const BATCH_SIZE = 15; // PriceEmpire hard limit
const PE_TO_APP = new Map(MARKETS.map(m => [m.pe, m.key]));

function chunk(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

async function fetchPricesBatch(sources, attempt = 1) {
  const csv = sources.join(',');
  const url = USE_PROXY
    ? `${PROXY_URL}?sources=${encodeURIComponent(csv)}`
    : `${HOST}/v4/paid/items/prices?app_id=${APP_ID}&currency=USD&sources=${csv}`;
  // Proxy token travels in a header, never the query string — query strings end up in
  // Cloudflare request logs and in any error text that echoes the URL.
  const headers = USE_PROXY
    ? { Accept: 'application/json', 'Accept-Encoding': 'gzip, br', 'X-PE-Token': PROXY_TOKEN }
    : { Authorization: `Bearer ${API_KEY}`, Accept: 'application/json', 'Accept-Encoding': 'gzip, br' };
  try {
    // Payload is huge (~32 MB) and the Worker proxy can be slow to stream it back;
    // 120s was too tight and tripped false timeouts under load.
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(180000) });
    if (!r.ok) {
      const body = await r.text().catch(() => '');
      throw new Error(`HTTP ${r.status}: ${body.slice(0, 300)}`);
    }
    const data = await r.json();
    if (!Array.isArray(data)) throw new Error('unexpected response shape (not an array)');
    return data;
  } catch (e) {
    // Transient upstream hiccups (Cloudflare 502, timeouts) are common on the big
    // batch — retry more times with longer backoff before giving up.
    if (attempt < 5) {
      console.warn(`  batch retry ${attempt} after error: ${e.message}`);
      await new Promise(res => setTimeout(res, 3000 * attempt));
      return fetchPricesBatch(sources, attempt + 1);
    }
    throw e;
  }
}

// ── CS.Money fallback (inactive by default) ─────────────────────────────────
// Safety net per the CS.Money partnership: if PriceEmpire is ever disabled, run the job
// with PE_FALLBACK=csmoney to populate prices from CS.Money's own API instead.
// NOTE: this yields ONLY CS.Money prices (one market), not the 40-market aggregate, and
// the response shape/units should be verified before relying on it in production.
const CSMONEY_MARKET_API = 'https://cs.money/api/min_price/market/all';
const CSMONEY_TRADE_API  = 'https://cs.money/api/min_price/all';
async function fetchCSMoneyFallback() {
  const now = new Date().toISOString();
  const out = new Map(); // name → usd
  for (const api of [CSMONEY_MARKET_API, CSMONEY_TRADE_API]) {
    try {
      const r = await fetch(api, { headers: { Accept: 'application/json' }, signal: AbortSignal.timeout(60000) });
      if (!r.ok) { console.warn(`csmoney ${api} → HTTP ${r.status}`); continue; }
      const data = await r.json();
      // Expected: a { "<market_hash_name>": <usd price> } map (possibly under .items). Verify on enable.
      const map = (data && typeof data === 'object' && !Array.isArray(data))
        ? (data.items && typeof data.items === 'object' ? data.items : data)
        : {};
      for (const [name, raw] of Object.entries(map)) {
        const usd = Number(raw);
        if (name && usd > 0 && !out.has(name)) out.set(name, Math.round(usd * 100) / 100);
      }
    } catch (e) { console.warn(`csmoney ${api} failed: ${e.message}`); }
  }
  return [...out].map(([name, usd]) => ({
    name, prices: { csmoney: usd }, counts: {}, best_market: 'csmoney', best_price: usd,
    liquidity: null, trades_7d: 0, updated_at: now,
  }));
}

/** master: Map<name, { prices: {appKey:usd}, counts: {appKey:n}, liquidity:number }> */
function mergeBatch(master, items) {
  for (const it of items) {
    const name = it?.market_hash_name;
    if (!name || !Array.isArray(it.prices)) continue;
    let rec = master.get(name);
    if (!rec) { rec = { prices: {}, counts: {}, liquidity: Number(it.liquidity) || 0, trades7d: Number(it.trades_7d) || 0 }; master.set(name, rec); }
    for (const p of it.prices) {
      const appKey = PE_TO_APP.get(p?.provider_key);
      if (!appKey) continue;            // market not in our curated list
      const usd = Number(p?.price) / 100; // PriceEmpire prices are in USD cents
      if (Number.isFinite(usd) && usd > 0) {
        rec.prices[appKey] = Math.round(usd * 100) / 100;
        const c = Number(p?.count) || 0; // listings on that market (volume)
        if (c > 0) rec.counts[appKey] = c;
      }
    }
  }
}

function buildRows(master) {
  const rows = [];
  const now = new Date().toISOString();
  for (const [name, rec] of master) {
    const entries = Object.entries(rec.prices);
    if (!entries.length) continue;
    // best = cheapest market (convenience default; the app computes its own display).
    let best = entries[0];
    for (const e of entries) if (e[1] < best[1]) best = e;
    rows.push({
      name,
      prices: rec.prices,
      counts: rec.counts,
      best_market: best[0],
      best_price: best[1],
      liquidity: rec.liquidity || null,
      trades_7d: rec.trades7d || 0,
      updated_at: now,
    });
  }
  return rows;
}

async function upsertChunkViaWorker(part, attempt = 1) {
  try {
    const r = await fetch(UPSERT_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-PE-Token': PROXY_TOKEN },
      body: JSON.stringify(part),
      signal: AbortSignal.timeout(60000),
    });
    if (!r.ok) {
      const body = await r.text().catch(() => '');
      throw new Error(`pe-upsert HTTP ${r.status}: ${body.slice(0, 300)}`);
    }
  } catch (e) {
    if (attempt < 3) {
      console.warn(`  upsert retry ${attempt} after error: ${e.message}`);
      await new Promise(res => setTimeout(res, 2000 * attempt));
      return upsertChunkViaWorker(part, attempt + 1);
    }
    throw e;
  }
}

async function upsertRows(rows) {
  const CHUNK = 500;
  let written = 0;
  if (SERVICE_KEY) {
    // Direct Supabase path (local runs with the service key).
    const { createClient } = await import('@supabase/supabase-js');
    const supabase = createClient(SUPABASE_URL, SERVICE_KEY, { auth: { persistSession: false } });
    for (const part of chunk(rows, CHUNK)) {
      const { error } = await supabase.from('skin_market_prices').upsert(part, { onConflict: 'name' });
      if (error) throw new Error(`Supabase upsert failed: ${error.message}`);
      written += part.length;
      process.stdout.write(`\r  upserted ${written}/${rows.length}`);
    }
  } else {
    // No service key → write through the Worker (/pe-upsert), same token as /pe-bulk.
    for (const part of chunk(rows, CHUNK)) {
      await upsertChunkViaWorker(part);
      written += part.length;
      process.stdout.write(`\r  upserted ${written}/${rows.length} (via worker)`);
    }
  }
  process.stdout.write('\n');
}

async function main() {
  if (API_KEY && !/^[\x21-\x7e]+$/.test(API_KEY)) {
    console.error('ERROR: PRICEMPIRE_API_KEY contains non-ASCII characters — looks like you pasted the placeholder text (e.g. "<твой ключ>") instead of the real key. Unset it to use the Worker proxy, or set the real key.');
    process.exit(1);
  }
  if (USE_PROXY && !PROXY_TOKEN) { console.error('ERROR: PE_PROXY_TOKEN not set (required to fetch/write via the Worker proxy; or set PRICEMPIRE_API_KEY + SUPABASE_SERVICE_ROLE_KEY for direct access)'); process.exit(1); }
  if (!DRY_RUN && !SERVICE_KEY && !PROXY_TOKEN) { console.error('ERROR: need SUPABASE_SERVICE_ROLE_KEY or PE_PROXY_TOKEN to write (or pass --dry-run)'); process.exit(1); }

  let rows;
  if (process.env.PE_FALLBACK === 'csmoney') {
    // PriceEmpire disabled → fall back to CS.Money's own API.
    console.log('PE_FALLBACK=csmoney → fetching prices from CS.Money API instead of PriceEmpire');
    rows = await fetchCSMoneyFallback();
    console.log(`CS.Money fallback: ${rows.length} items`);
  } else {
    const batches = chunk(MARKETS.map(m => m.pe), BATCH_SIZE);
    console.log(`PriceEmpire refresh — ${MARKETS.length} markets in ${batches.length} batches${DRY_RUN ? ' (dry run)' : ''} via ${USE_PROXY ? 'Worker proxy (no local key)' : 'direct API key'}`);
    const master = new Map();
    for (let i = 0; i < batches.length; i++) {
      const t0 = Date.now();
      const items = await fetchPricesBatch(batches[i]);
      mergeBatch(master, items);
      console.log(`  batch ${i + 1}/${batches.length}: ${items.length} items, ${batches[i].length} sources, ${((Date.now() - t0) / 1000).toFixed(1)}s`);
    }
    rows = buildRows(master);
  }
  console.log(`parsed ${rows.length} items with at least one market price`);

  // Coverage report — how many items priced per market.
  const cov = {};
  for (const r of rows) for (const k of Object.keys(r.prices)) cov[k] = (cov[k] || 0) + 1;
  const covSorted = Object.entries(cov).sort((a, b) => b[1] - a[1]);
  console.log('coverage per market:');
  for (const [k, n] of covSorted) console.log(`  ${k.padEnd(14)} ${n}`);

  if (DRY_RUN) {
    const sample = rows.find(r => r.name.includes('AK-47 | Redline (Field-Tested)')) || rows[0];
    console.log('\nsample row:', JSON.stringify(sample, null, 2));
    console.log('\n--dry-run: nothing written to Supabase');
    return;
  }

  console.log('writing to Supabase…');
  await upsertRows(rows);
  console.log('done.');
}

// ── Loop mode ─────────────────────────────────────────────────────────────
// GitHub throttles frequent crons hard (a '*/20' schedule fires ~8-10×/day, not
// 72×), which left ~98% of the PriceEmpire quota unused. So the workflow now
// fires a SPARSE cron (every 2h — those run reliably) and this script refreshes
// several times within one run: REFRESH_ITERATIONS iterations, sleeping
// REFRESH_INTERVAL_MIN between them. 6 × every 2h × 3 req = ~6,500 req/month.
const ITERATIONS = Math.max(1, Number(process.env.REFRESH_ITERATIONS || '1'));
const INTERVAL_MIN = Math.max(1, Number(process.env.REFRESH_INTERVAL_MIN || '20'));
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

(async () => {
  let anySuccess = false;
  for (let iter = 1; iter <= ITERATIONS; iter++) {
    if (ITERATIONS > 1) console.log(`\n=== refresh iteration ${iter}/${ITERATIONS} ===`);
    try {
      await main();
      anySuccess = true;
    } catch (e) {
      console.error('FATAL:', e.message);
    }
    if (iter < ITERATIONS) {
      console.log(`sleeping ${INTERVAL_MIN} min before next refresh…`);
      await sleep(INTERVAL_MIN * 60 * 1000);
    }
  }
  // Only fail the whole run if EVERY iteration failed. A single transient upstream
  // hiccup (Cloudflare 502/timeout) on one iteration — even the last — should not
  // mark the run failed when other iterations already wrote fresh prices.
  if (!anySuccess) {
    console.error('All refresh iterations failed — marking run as failed.');
    process.exit(1);
  }
})();
