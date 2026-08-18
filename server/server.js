#!/usr/bin/env node
/**
 * Columnar Mode demo server — serves the web UI and proxies Elasticsearch
 * so the API key never reaches the browser.
 */
const http = require("http");
const https = require("https");
const fs = require("fs");
const path = require("path");

const ES = (process.env.ES_ENDPOINT || "").replace(/^https?:\/\//, "").replace(/\/+$/, "");
const API_KEY = process.env.ES_API_KEY || "";
if (!ES || !API_KEY) {
  console.error("Missing configuration. Set both environment variables and retry:");
  console.error('  export ES_ENDPOINT="https://<your-project>.es.<region>.elastic.cloud"');
  console.error('  export ES_API_KEY="<base64 API key>"');
  process.exit(1);
}
const PORT = process.env.PORT || 8787;
const WEB = path.join(__dirname, "..", "web");

const INDICES = ["netops-cmp-standard", "netops-cmp-logsdb", "netops-cmp-columnar"];

function es(method, esPath, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const req = https.request(
      { host: ES, path: esPath, method, headers: {
          Authorization: `ApiKey ${API_KEY}`,
          "Content-Type": "application/json",
          ...(data ? { "Content-Length": Buffer.byteLength(data) } : {}),
        } },
      (res) => {
        let buf = "";
        res.on("data", (c) => (buf += c));
        res.on("end", () => {
          try { resolve({ status: res.statusCode, json: JSON.parse(buf) }); }
          catch { resolve({ status: res.statusCode, json: { raw: buf } }); }
        });
      }
    );
    req.on("error", reject);
    if (data) req.write(data);
    req.end();
  });
}

async function overview() {
  const [cat, info] = await Promise.all([
    es("GET", "/_cat/indices/netops-cmp-*?format=json&bytes=b"),
    es("GET", "/"),
  ]);
  const modes = {};
  for (const name of INDICES) {
    const s = await es("GET", `/${name}/_settings`);
    const st = s.json[name]?.settings?.index || {};
    modes[name] = st.mode || "standard";
  }
  const rows = (cat.json || [])
    .map((r) => ({
      index: r.index,
      mode: modes[r.index],
      docs: Number(r["docs.count"]),
      bytes: Number(r["dataset.size"]),
    }))
    .sort((a, b) => INDICES.indexOf(a.index) - INDICES.indexOf(b.index));
  return { version: info.json.version?.number, endpoint: ES, rows };
}

async function esql(query) {
  const t0 = process.hrtime.bigint();
  const r = await es("POST", "/_query", { query });
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  return { status: r.status, ms: Math.round(ms * 10) / 10, ...r.json };
}

async function race(queryTemplate) {
  // Run the same ES|QL query against each index, twice (2nd run = warm), report warm latency.
  const out = [];
  for (const name of INDICES) {
    const q = queryTemplate.replaceAll("$INDEX", name);
    await esql(q); // warm-up
    const r = await esql(q);
    out.push({ index: name, ms: r.ms, columns: r.columns, values: r.values, error: r.status >= 300 ? r : null });
  }
  return out;
}

const METRICS_PER_DOC = 4; // in_bytes, out_bytes, cpu, memory
const SIZES_CACHE = path.join(__dirname, ".metrics-sizes.json");

function readSizeCache() {
  try { return JSON.parse(fs.readFileSync(SIZES_CACHE, "utf8")); } catch { return {}; }
}

async function metricsOverview() {
  const cat = await es("GET", "/_cat/indices/netops-cmp-metrics*?format=json&bytes=b");
  const cache = readSizeCache();
  let cacheDirty = false;
  const rows = (cat.json || []).map((r) => {
    const live = Number(r["dataset.size"]);
    let bytes = live, stale = false;
    if (live > 10000) {
      if (cache[r.index] !== live) { cache[r.index] = live; cacheDirty = true; }
    } else if (cache[r.index]) {
      bytes = cache[r.index]; // last settled measurement while object-store stats lag
      stale = true;
    }
    return {
      index: r.index,
      docs: Number(r["docs.count"]),
      bytes, stale,
      dataPoints: Number(r["docs.count"]) * METRICS_PER_DOC,
    };
  }).sort((a, b) => a.index.length - b.index.length); // tsds first
  if (cacheDirty) { try { fs.writeFileSync(SIZES_CACHE, JSON.stringify(cache)); } catch {} }
  return { rows };
}

async function search(qtext) {
  const t0 = process.hrtime.bigint();
  const r = await es("POST", "/netops-cmp-columnar/_search", {
    query: { match: { message: { query: qtext, operator: "and" } } },
    highlight: { fields: { message: {} } },
    size: 8,
    track_total_hits: true,
  });
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  return { ms: Math.round(ms * 10) / 10, total: r.json.hits?.total?.value, hits: (r.json.hits?.hits || []).map((h) => ({
    highlight: h.highlight?.message?.[0],
    src: { "@timestamp": h._source["@timestamp"], host: h._source.host?.name,
           action: h._source.event?.action, rule: h._source.rule?.name },
  })) };
}

const MIME = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" };

http.createServer(async (req, res) => {
  const send = (code, obj, type = "application/json") => {
    res.writeHead(code, { "Content-Type": type });
    res.end(type === "application/json" ? JSON.stringify(obj) : obj);
  };
  try {
    const url = new URL(req.url, "http://x");
    if (url.pathname === "/api/overview") return send(200, await overview());
    if (url.pathname === "/api/metrics-overview") return send(200, await metricsOverview());
    if (url.pathname === "/api/esql" && req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", async () => {
        try { send(200, await esql(JSON.parse(body).query)); }
        catch (e) { send(500, { error: String(e) }); }
      });
      return;
    }
    if (url.pathname === "/api/race" && req.method === "POST") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", async () => {
        try { send(200, await race(JSON.parse(body).query)); }
        catch (e) { send(500, { error: String(e) }); }
      });
      return;
    }
    if (url.pathname === "/api/search") return send(200, await search(url.searchParams.get("q") || "DENY ssh"));
    // static
    let p = url.pathname === "/" ? "/index.html" : url.pathname;
    if (p === "/slides" || p === "/slides/") p = "/slides/deck.html";
    const root = p.startsWith("/slides/") ? path.join(__dirname, "..") : WEB;
    const file = path.join(root, path.normalize(p).replace(/^(\.\.[/\\])+/, ""));
    if (file.startsWith(root) && fs.existsSync(file) && fs.statSync(file).isFile()) {
      return send(200, fs.readFileSync(file), MIME[path.extname(file)] || "text/plain");
    }
    send(404, { error: "not found" });
  } catch (e) {
    send(500, { error: String(e) });
  }
}).listen(PORT, () => console.log(`Columnar demo → http://localhost:${PORT}`));
