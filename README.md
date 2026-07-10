# skinmanity-prices-refresh

Scheduled job that refreshes CS2 skin prices for the Skinmanity app: pulls all-market
prices from PriceEmpire and stores them in Supabase, every ~20 minutes.

Lives in its own **public** repo because GitHub Actions minutes are free for public
repositories — the refresh loop keeps a runner alive for ~2 h per scheduled run, which
would burn a private repo's entire monthly quota in about a day and a half.

No credentials live here: the script talks only to a Cloudflare Worker proxy
(`/pe-bulk` to read PriceEmpire, `/pe-upsert` to write Supabase), authenticated by the
`PE_PROXY_TOKEN` repo secret. The PriceEmpire and Supabase keys stay in the Worker.

The canonical copy of `scripts/refresh-pricempire.mjs` lives in the main (private)
Skinmanity repo — edit it there and copy changes over.
