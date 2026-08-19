# Elasticsearch Columnar Logs / Metrics — Live Demo

An interactive, customer-ready demo of **Columnar Mode & Columnar Logs**
(Technical Preview in 9.5, **GA in Elasticsearch 9.6**), the **fully columnar
Metrics engine** (TSDS + ES|QL `TS` command), and **native Prometheus / PromQL
support** — all running live against your own Elastic Cloud Serverless project.

**Zero dependencies**: the web app is one Node.js file (built-in modules only)
plus one HTML page; data generators are Python standard library only. Nothing
to `npm install`, no build step.

## What you get

- **Storage footprint comparison** — 150,000 identical wide-schema firewall
  logs ingested into three indices (`standard`, `logsdb`, `logsdb_columnar`),
  with the live sizes charted straight from `_cat/indices`. Expect columnar to
  land **~70–85% smaller than standard**.
- **ES|QL query race** — the same analytics query against all three modes,
  with latency bars: same answers, columnar economics.
- **Metrics engine** — 86,400 TSDS samples with counters/gauges, a live
  `RATE()` throughput chart (including an injected traffic surge to point at),
  and runnable `TS` query presets.
- **Native PromQL** — genuine PromQL (`topk`, `rate`, `sum by`) executed by
  the ES|QL engine, plus the showstopper: a PromQL query **piped into
  `LOOKUP JOIN`** to enrich metrics with a lookup table.
- **Full-text search** over the columnar index with highlighting — the part
  column-store warehouses can't do.
- A keyboard-driven **10-slide HTML deck** (`/slides`) whose storage-numbers
  slides fetch live cluster data *while you present*.

## Quick start

1. Create an [Elastic Cloud Serverless](https://www.elastic.co/cloud/serverless)
   project (Elasticsearch or Observability) on **9.6+** and create an API key.
2. Configure and load data (~3 minutes, ~530k documents):

   ```bash
   export ES_ENDPOINT="https://<your-project>.es.<region>.elastic.cloud"
   export ES_API_KEY="<base64 API key>"

   python3 data/generate_and_ingest.py   # 150k logs × 3 index modes
   python3 data/generate_metrics.py      # TSDS metrics + PromQL info series + lookup table
   ```

3. Run the demo:

   ```bash
   node server/server.js
   ```

   - Web showcase → http://localhost:8787
   - Slides → http://localhost:8787/slides (arrow keys; `#N` deep links)

The Node server proxies Elasticsearch so your API key never reaches the
browser.

## Optional extras

- `data/generate_longterm.py` — a year-ahead dataset: hourly TSDS metrics and
  a firewall logs data stream from now−30d to **now+365d**, with seasonality,
  a growth trend, and incident surges every ~6 weeks (great for alerting/SLO
  demos and for keeping the demo alive without regeneration).
- `data/build_dashboards.py` — imports two Kibana dashboards built entirely
  from ES|QL panels (requires `KIBANA_ENDPOINT` in addition to `ES_API_KEY`).

## Suggested talk track (~10 min)

1. **Slides 1–4**: why analytics at log scale meant running a second system,
   and how Columnar Mode stores each field once (Columnar Logs keeps exactly
   one inverted index — on `message`).
2. **Slide 5**: storage economics, measured live on your cluster mid-pitch.
3. **Web UI**: run a query race → show the metrics `RATE()` chart and its
   surge → run the PromQL `topk()` preset (it finds the surge) → run the
   PromQL + `LOOKUP JOIN` preset → search `DENY ssh` for highlighted hits from
   the columnar index.
4. **Slide 10**: takeaways — pilot one high-volume data stream on
   `logsdb_columnar`; the whole migration is one index setting.

## Serverless notes (learned the hard way)

- Storage sizes come from `_cat/indices` `dataset.size` (the `_stats` API and
  `store.size` aren't available on serverless). Sizes take **1–2 minutes to
  settle** after ingest while segments land in object storage — the UI
  auto-polls, and the server caches last-settled values so tiles never sit
  empty.
- Kibana Index Management shows a *logical* (as-ingested) size — identical for
  all three indices, which conveniently proves the comparison is fair. The
  physical footprint is the `dataset.size` number the demo charts.
- The TSDS metrics index is time-bounded: re-run `generate_metrics.py` if your
  last run is more than ~6 hours old.
- PromQL: the `index=` parameter is required for custom indices; vector
  matching (`group_left`) isn't supported yet — pipe into ES|QL `LOOKUP JOIN`
  instead (it's a better story anyway).

## Query race — what columnar wins (and doesn't)

Columnar Logs drops the inverted index in favour of doc-values-only storage.
That means:

| Query pattern | Standard / LogsDB | Columnar Logs |
|---|---|---|
| `WHERE keyword == "value"` (filter) | ✅ Fast — inverted index lookup | ⚠️ Slower — full column scan |
| `STATS SUM/AVG/PERCENTILE(numeric)` | Fine | ✅ Fastest — sequential column read |
| `STATS COUNT(*) BY keyword` (group-by, no filter) | Fine | ✅ Competitive |
| Full-text search on `message` | ✅ Inverted index | ✅ Inverted index kept |

The race presets are all chosen to play to columnar's strengths (numeric
aggregations, no leading keyword filters). If you swap in a query that starts
with `WHERE some_keyword == "…"`, expect columnar to trail — that trade-off is
by design, and the storage compression numbers are the payoff.

## Layout

```
server/server.js            zero-dependency Node server + ES proxy
web/index.html              single-file showcase UI
slides/deck.html            single-file 10-slide deck
data/generate_and_ingest.py logs → standard / logsdb / logsdb_columnar
data/generate_metrics.py    TSDS metrics + hostinfo series + lookup table
data/generate_longterm.py   optional year-ahead dataset
data/build_dashboards.py    optional Kibana dashboards (ES|QL panels)
```

## License

MIT — see [LICENSE](LICENSE).
