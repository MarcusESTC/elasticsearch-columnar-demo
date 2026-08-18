#!/usr/bin/env python3
"""
Metrics engine demo: generate 24h of NetOps interface/host metrics and ingest
into a TSDS (time_series) index plus an identical standard-mode index for
footprint comparison.
"""
import gzip
import json
import math
import random
import sys
import time
import urllib.request

import os

EP = os.environ.get("ES_ENDPOINT", "").rstrip("/")
API_KEY = os.environ.get("ES_API_KEY", "")
if not EP or not API_KEY:
    raise SystemExit(
        "Missing configuration. Set both environment variables and retry:\n"
        '  export ES_ENDPOINT="https://<your-project>.es.<region>.elastic.cloud"\n'
        '  export ES_API_KEY="<base64 API key>"'
    )
HEADERS = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}

HOSTS = [f"fw-{dc}-{i:02d}" for dc in ("iad", "sfo", "fra") for i in range(1, 6)]  # 15 hosts
IFACES = ["ethernet1/1", "ethernet1/2"]
INTERVAL_S = 30
HOURS = 24
SAMPLES = HOURS * 3600 // INTERVAL_S  # 2880 per series
METRICS_PER_DOC = 4  # in_bytes, out_bytes, cpu, memory

MAPPINGS = {
    "properties": {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
        "interface": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
        "network": {
            "properties": {
                "in_bytes": {"type": "long", "time_series_metric": "counter"},
                "out_bytes": {"type": "long", "time_series_metric": "counter"},
            }
        },
        "cpu": {"properties": {"utilization": {"type": "double", "time_series_metric": "gauge"}}},
        "memory": {"properties": {"utilization": {"type": "double", "time_series_metric": "gauge"}}},
    }
}


def req(method, path, body=None, ndjson=False):
    headers = dict(HEADERS)
    data = None
    if body is not None:
        if ndjson:
            headers["Content-Type"] = "application/x-ndjson"
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(body.encode())
        else:
            data = json.dumps(body).encode()
    r = urllib.request.Request(EP + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=180) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "body": e.read().decode()[:500]}


def main():
    random.seed(7)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - HOURS * 3600 * 1000

    tsds_settings = {
        "index": {
            "mode": "time_series",
            "routing_path": ["host.name", "interface.name"],
            "time_series": {
                "start_time": start_ms - 3600_000,
                "end_time": now_ms + 6 * 3600_000,
            },
        }
    }
    indices = {
        "netops-cmp-metrics": tsds_settings,
        "netops-cmp-metrics-standard": {"index": {"mode": "standard"}},
    }
    print("Recreating metrics indices...")
    for name, settings in indices.items():
        req("DELETE", f"/{name}")
        out = req("PUT", f"/{name}", {"settings": settings, "mappings": MAPPINGS})
        print(f"  {name}: {out}")
        if "_http_error" in out:
            sys.exit(1)

    # Per-series traffic profile: diurnal curve + noise; one host gets a surge
    # 14:00-16:00 into the window (a story beat for the demo).
    print(f"Generating {len(HOSTS)*len(IFACES)} series x {SAMPLES} samples...")
    docs = []
    for host in HOSTS:
        for iface in IFACES:
            base_bps = random.uniform(2e6, 3e7)  # bytes/sec baseline
            ratio = random.uniform(0.4, 0.9)     # out/in ratio
            cpu0 = random.uniform(0.15, 0.45)
            mem0 = random.uniform(0.35, 0.7)
            in_ctr = random.randint(10**9, 10**11)
            out_ctr = random.randint(10**9, 10**11)
            surge = host == "fw-iad-03" and iface == "ethernet1/2"
            for s in range(SAMPLES):
                ts = start_ms + s * INTERVAL_S * 1000
                frac = s / SAMPLES
                diurnal = 0.6 + 0.4 * math.sin(2 * math.pi * (frac - 0.25))
                mult = diurnal * random.uniform(0.85, 1.15)
                if surge and 14 / 24 <= frac <= 16 / 24:
                    mult *= 6.5  # traffic surge anomaly
                bps = base_bps * mult
                in_ctr += int(bps * INTERVAL_S)
                out_ctr += int(bps * ratio * INTERVAL_S)
                cpu = min(0.98, max(0.02, cpu0 * (0.7 + 0.6 * mult) + random.gauss(0, 0.02)))
                mem = min(0.98, max(0.05, mem0 + 0.05 * math.sin(2 * math.pi * frac * 3) + random.gauss(0, 0.01)))
                docs.append({
                    "@timestamp": ts,
                    "host": {"name": host},
                    "interface": {"name": iface},
                    "network": {"in_bytes": in_ctr, "out_bytes": out_ctr},
                    "cpu": {"utilization": round(cpu, 4)},
                    "memory": {"utilization": round(mem, 4)},
                })
    print(f"  {len(docs)} docs total")

    chunk = 8000
    for name in indices:
        t0 = time.time()
        for i in range(0, len(docs), chunk):
            lines = []
            for d in docs[i:i + chunk]:
                lines.append('{"create":{}}')
                lines.append(json.dumps(d))
            out = req("POST", f"/{name}/_bulk?refresh=false", "\n".join(lines) + "\n", ndjson=True)
            if out.get("errors") or "_http_error" in out:
                print(f"  BULK ERROR {name}@{i}: {str(out)[:400]}")
                sys.exit(1)
            if (i // chunk) % 4 == 0:
                print(f"  {name}: {min(i+chunk, len(docs))}/{len(docs)}", flush=True)
        req("POST", f"/{name}/_refresh")
        print(f"  {name} done in {time.time()-t0:.1f}s")

    # --- host metadata: PromQL info-metric series + ES|QL lookup table ---
    HOST_META = {}
    for h in HOSTS:
        dc = h.split("-")[1]
        HOST_META[h] = {
            "datacenter": {"iad": "us-east-ashburn", "sfo": "us-west-sanjose",
                           "fra": "eu-central-frankfurt"}[dc],
            "environment": "dr" if dc == "fra" else "production",
            "owner": "netops-emea" if dc == "fra" else "netops-us",
        }

    # PromQL-style info metric: host_info{host.name, datacenter, environment} = 1
    print("Recreating netops-cmp-hostinfo (info-metric series for PromQL lookups)...")
    req("DELETE", "/netops-cmp-hostinfo")
    out = req("PUT", "/netops-cmp-hostinfo", {
        "settings": {"index": {"mode": "time_series", "routing_path": ["host.name"],
                               "time_series": tsds_settings["index"]["time_series"]}},
        "mappings": {"properties": {
            "@timestamp": {"type": "date"},
            "host": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
            "datacenter": {"type": "keyword", "time_series_dimension": True},
            "environment": {"type": "keyword", "time_series_dimension": True},
            "host_info": {"type": "double", "time_series_metric": "gauge"},
        }},
    })
    print(f"  {out}")
    lines = []
    for h in HOSTS:
        m = HOST_META[h]
        for s in range(0, HOURS * 3600, 120):  # every 2 min, inside PromQL lookback
            lines.append('{"create":{}}')
            lines.append(json.dumps({"@timestamp": start_ms + s * 1000,
                                     "host": {"name": h}, "datacenter": m["datacenter"],
                                     "environment": m["environment"], "host_info": 1}))
    out = req("POST", "/netops-cmp-hostinfo/_bulk?refresh=true", "\n".join(lines) + "\n", ndjson=True)
    print(f"  hostinfo docs: {len(lines)//2}, errors: {out.get('errors')}")

    # ES|QL lookup table: one row per host (index.mode=lookup for LOOKUP JOIN)
    print("Recreating netops-host-lookup (ES|QL LOOKUP JOIN table)...")
    req("DELETE", "/netops-host-lookup")
    out = req("PUT", "/netops-host-lookup", {
        "settings": {"index": {"mode": "lookup"}},
        "mappings": {"properties": {
            "host.name": {"type": "keyword"},
            "datacenter": {"type": "keyword"},
            "environment": {"type": "keyword"},
            "owner": {"type": "keyword"},
        }},
    })
    print(f"  {out}")
    lines = []
    for h, m in HOST_META.items():
        lines.append('{"create":{}}')
        lines.append(json.dumps({"host.name": h, **m}))
    out = req("POST", "/netops-host-lookup/_bulk?refresh=true", "\n".join(lines) + "\n", ndjson=True)
    print(f"  lookup rows: {len(lines)//2}, errors: {out.get('errors')}")

    print("Sizes (may take 1-2 min to settle):")
    out = req("GET", "/_cat/indices/netops-cmp-metrics*?format=json&bytes=b")
    for row in sorted(out, key=lambda r: r["index"]):
        print(f"  {row['index']}: docs={row['docs.count']} dataset.size={row.get('dataset.size')}")


if __name__ == "__main__":
    main()
