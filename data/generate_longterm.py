#!/usr/bin/env python3
"""
Net-new long-horizon o11y data (does NOT touch netops-cmp-* demo indices):

1. netops-metrics-longterm — custom TSDS, hourly interface metrics from
   now-30d to now+365d (30 series). Diurnal + weekly seasonality, slow growth
   trend, and scheduled "incident" windows (~every 6 weeks) for alert demos.
2. logs-netops.firewall-default — a logs data stream (serverless logsdb by
   default): dense last 48h, steady past 30d, and 365d of future logs with
   error bursts aligned to the metric incidents.
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

DAY_MS = 24 * 3600 * 1000
HOSTS = [f"fw-{dc}-{i:02d}" for dc in ("iad", "sfo", "fra") for i in range(1, 6)]
IFACES = ["ethernet1/1", "ethernet1/2"]


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
        return {"_http_error": e.code, "body": e.read().decode()[:400]}


def bulk(target, docs, label):
    chunk = 8000
    for i in range(0, len(docs), chunk):
        lines = []
        for d in docs[i:i + chunk]:
            lines.append('{"create":{}}')
            lines.append(json.dumps(d))
        out = req("POST", f"/{target}/_bulk?refresh=false", "\n".join(lines) + "\n", ndjson=True)
        if out.get("errors") or "_http_error" in out:
            errs = [it for it in out.get("items", []) if it["create"].get("error")][:1]
            print(f"  BULK ERROR {label}@{i}: {str(errs or out)[:300]}")
            sys.exit(1)
    req("POST", f"/{target}/_refresh")
    print(f"  {label}: {len(docs)} docs ingested")


def main():
    random.seed(2027)
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 30 * DAY_MS
    end_ms = now_ms + 365 * DAY_MS

    # Incident windows: every ~6 weeks, a 3h traffic/error surge on one host
    incidents = []
    t = now_ms - 20 * DAY_MS
    i = 0
    while t < end_ms:
        incidents.append({"start": t, "end": t + 3 * 3600 * 1000,
                          "host": HOSTS[i % len(HOSTS)]})
        t += int(6 * 7 * DAY_MS * random.uniform(0.85, 1.15))
        i += 1
    print(f"Scheduled {len(incidents)} incident windows (first: "
          f"{time.strftime('%Y-%m-%d', time.gmtime(incidents[0]['start']/1000))}, "
          f"last: {time.strftime('%Y-%m-%d', time.gmtime(incidents[-1]['start']/1000))})")

    def incident_mult(ts, host):
        for inc in incidents:
            if inc["host"] == host and inc["start"] <= ts <= inc["end"]:
                return 5.0
        return 1.0

    # ---------------- 1. long-term TSDS metrics ----------------
    print("Recreating netops-metrics-longterm (TSDS, now-30d .. now+365d)...")
    req("DELETE", "/netops-metrics-longterm")
    out = req("PUT", "/netops-metrics-longterm", {
        "settings": {"index": {"mode": "time_series",
                               "routing_path": ["host.name", "interface.name"],
                               "time_series": {"start_time": start_ms - DAY_MS,
                                               "end_time": end_ms + 5 * DAY_MS}}},
        "mappings": {"properties": {
            "@timestamp": {"type": "date"},
            "host": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
            "interface": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
            "network": {"properties": {
                "in_bytes": {"type": "long", "time_series_metric": "counter"},
                "out_bytes": {"type": "long", "time_series_metric": "counter"}}},
            "cpu": {"properties": {"utilization": {"type": "double", "time_series_metric": "gauge"}}},
            "memory": {"properties": {"utilization": {"type": "double", "time_series_metric": "gauge"}}},
        }},
    })
    if "_http_error" in out:
        print(out); sys.exit(1)

    docs = []
    n_hours = int((end_ms - start_ms) / 3600000)
    for host in HOSTS:
        for iface in IFACES:
            base_bps = random.uniform(4e6, 2.5e7)
            ratio = random.uniform(0.4, 0.9)
            cpu0 = random.uniform(0.15, 0.4)
            in_ctr = random.randint(10**10, 10**12)
            out_ctr = random.randint(10**10, 10**12)
            for h in range(n_hours):
                ts = start_ms + h * 3600000
                day_frac = (ts % DAY_MS) / DAY_MS
                dow = int(ts / DAY_MS + 4) % 7          # weekday seasonality
                weekly = 0.55 if dow >= 5 else 1.0
                diurnal = 0.6 + 0.4 * math.sin(2 * math.pi * (day_frac - 0.25))
                growth = 1.0 + 0.35 * (ts - start_ms) / (end_ms - start_ms)  # traffic grows over the year
                mult = diurnal * weekly * growth * random.uniform(0.9, 1.1) * incident_mult(ts, host)
                bps = base_bps * mult
                in_ctr += int(bps * 3600)
                out_ctr += int(bps * ratio * 3600)
                cpu = min(0.98, max(0.02, cpu0 * (0.6 + 0.7 * mult) + random.gauss(0, 0.02)))
                docs.append({"@timestamp": ts,
                             "host": {"name": host}, "interface": {"name": iface},
                             "network": {"in_bytes": in_ctr, "out_bytes": out_ctr},
                             "cpu": {"utilization": round(cpu, 4)},
                             "memory": {"utilization": round(min(0.95, 0.4 + 0.3 * mult + random.gauss(0, 0.01)), 4)}})
    print(f"  generated {len(docs)} hourly samples ({len(HOSTS)*len(IFACES)} series x {n_hours}h)")
    bulk("netops-metrics-longterm", docs, "netops-metrics-longterm")

    # ---------------- 2. firewall logs data stream ----------------
    print("Recreating logs-netops.firewall-default data stream...")
    req("DELETE", "/_data_stream/logs-netops.firewall-default")

    ACTIONS = ["allow", "deny", "drop", "alert"]
    ACTION_W = [70, 18, 8, 4]
    PROTOS = ["https", "dns", "http", "ssh", "smtp"]
    RULES = ["allow-corp-egress", "deny-inbound-default", "geo-block-highrisk",
             "ids-suspicious-tls", "rate-limit-ssh"]

    def log_doc(ts, host, error_burst=False):
        action = random.choices(ACTIONS, weights=ACTION_W)[0]
        if error_burst and random.random() < 0.7:
            action = random.choice(["deny", "drop", "alert"])
        proto = random.choice(PROTOS)
        rule = random.choice(RULES)
        sip = f"10.{random.randint(0,63)}.{random.randint(0,255)}.{random.randint(1,254)}"
        dip = f"{random.randint(11,203)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
        dport = {"https": 443, "dns": 53, "http": 80, "ssh": 22, "smtp": 25}[proto]
        nbytes = int(random.lognormvariate(7.0, 1.6))
        level = "error" if action in ("drop", "alert") else ("warning" if action == "deny" else "info")
        msg = (f"PA-5450 session {action.upper()} {proto} {sip}:{random.randint(1024,65535)} "
               f"-> {dip}:{dport} rule={rule} bytes={nbytes}")
        d = {"@timestamp": ts, "message": msg,
             "host": {"name": host},
             "observer": {"vendor": "Palo Alto Networks", "product": "PA-5450", "type": "firewall"},
             "log": {"level": level},
             "event": {"action": action, "category": "network",
                       "outcome": "success" if action == "allow" else "failure",
                       "original_size": 0},
             "source": {"ip": sip}, "destination": {"ip": dip, "port": dport},
             "network": {"protocol": proto, "bytes": nbytes},
             "rule": {"name": rule}}
        d["event"]["original_size"] = len(json.dumps(d, separators=(",", ":")).encode())
        return d

    logs = []
    # dense last 48h: every ~10s across the fleet
    for ts in range(now_ms - 2 * DAY_MS, now_ms, 10_000):
        logs.append(log_doc(ts + random.randint(0, 9000), random.choice(HOSTS)))
    # steady past 30d..48h: ~300/day
    for day in range(30, 2, -1):
        d0 = now_ms - day * DAY_MS
        for _ in range(300):
            logs.append(log_doc(d0 + random.randint(0, DAY_MS - 1), random.choice(HOSTS)))
    # future 365d: ~240/day + error bursts during incidents
    for day in range(0, 365):
        d0 = now_ms + day * DAY_MS
        for _ in range(240):
            ts = d0 + random.randint(0, DAY_MS - 1)
            host = random.choice(HOSTS)
            logs.append(log_doc(ts, host, error_burst=incident_mult(ts, host) > 1))
        for inc in incidents:  # concentrated burst logs inside incident windows
            if d0 <= inc["start"] < d0 + DAY_MS:
                for _ in range(400):
                    logs.append(log_doc(random.randint(inc["start"], inc["end"]), inc["host"], True))
    logs.sort(key=lambda d: d["@timestamp"])
    print(f"  generated {len(logs)} log docs")
    bulk("logs-netops.firewall-default", logs, "logs-netops.firewall-default")

    print("Done. Verify:")
    out = req("GET", "/_cat/indices/netops-metrics-longterm?format=json&bytes=b")
    print(" ", [(r["index"], r["docs.count"]) for r in out])
    out = req("POST", "/logs-netops.firewall-default/_search",
              {"size": 0, "aggs": {"range": {"stats": {"field": "@timestamp"}}}})
    agg = out.get("aggregations", {}).get("range", {})
    print("  logs range:", agg.get("min_as_string"), "→", agg.get("max_as_string"))


if __name__ == "__main__":
    main()
