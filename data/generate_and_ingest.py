#!/usr/bin/env python3
"""
Columnar Mode demo: generate realistic NetOps logs and ingest identical copies
into three indices with different index.mode settings for storage/query comparison.

Wide, varied schema on purpose: high-cardinality IDs (trace/session/community),
sparse per-protocol field groups (dns/tls/http/nat), numerics, geo points, dates.
Standard and logsdb still build inverted indices / BKD trees for all of these;
columnar stores them once as doc values — that's where the gap opens up.
"""
import gzip
import json
import os
import random
import sys
import time
import urllib.request
import uuid


EP = os.environ.get("ES_ENDPOINT", "").rstrip("/")
API_KEY = os.environ.get("ES_API_KEY", "")
if not EP or not API_KEY:
    raise SystemExit(
        "Missing configuration. Set both environment variables and retry:\n"
        '  export ES_ENDPOINT="https://<your-project>.es.<region>.elastic.cloud"\n'
        '  export ES_API_KEY="<base64 API key>"'
    )
HEADERS = {
    "Authorization": f"ApiKey {API_KEY}",
    "Content-Type": "application/json",
}

N_DOCS = 150000
INDICES = {
    "netops-cmp-standard": {"index": {"mode": "standard"}},
    "netops-cmp-logsdb": {"index": {"mode": "logsdb"}},
    "netops-cmp-columnar": {"index": {"mode": "logsdb_columnar"}},
}

KW = {"type": "keyword"}
MAPPING = {
    "properties": {
        "@timestamp": {"type": "date"},
        "message": {"type": "text"},
        "tags": KW,
        "host": {"properties": {"name": KW}},
        "observer": {"properties": {
            "vendor": KW, "product": KW, "type": KW, "name": KW,
            "serial_number": KW, "version": KW,
        }},
        "log": {"properties": {
            "level": KW,
            "syslog": {"properties": {"priority": {"type": "integer"}, "facility": {"properties": {"code": {"type": "integer"}}}}},
        }},
        "event": {"properties": {
            "id": KW, "action": KW, "outcome": KW, "category": KW,
            "severity": {"type": "integer"}, "duration": {"type": "long"},
            "risk_score": {"type": "double"}, "ingested": {"type": "date"},
            "original_size": {"type": "long"},  # serverless-friendly stand-in for mapper-size _size
        }},
        "trace": {"properties": {"id": KW}},
        "session": {"properties": {"id": KW}},
        "source": {"properties": {
            "ip": {"type": "ip"}, "port": {"type": "integer"},
            "bytes": {"type": "long"}, "packets": {"type": "long"},
            "geo": {"properties": {"country_iso_code": KW, "city_name": KW, "location": {"type": "geo_point"}}},
            "nat": {"properties": {"ip": {"type": "ip"}, "port": {"type": "integer"}}},
        }},
        "destination": {"properties": {
            "ip": {"type": "ip"}, "port": {"type": "integer"},
            "bytes": {"type": "long"}, "packets": {"type": "long"},
            "geo": {"properties": {"country_iso_code": KW}},
        }},
        "network": {"properties": {
            "protocol": KW, "transport": KW, "direction": KW,
            "bytes": {"type": "long"}, "packets": {"type": "long"},
            "community_id": KW, "iana_number": {"type": "integer"},
            "vlan": {"properties": {"id": {"type": "integer"}}},
        }},
        "rule": {"properties": {"name": KW, "id": KW, "uuid": KW}},
        "interface": {"properties": {"name": KW, "alias": KW}},
        "user": {"properties": {"name": KW, "domain": KW}},
        "user_agent": {"properties": {"original": KW}},
        "http": {"properties": {
            "request": {"properties": {"method": KW, "bytes": {"type": "long"}}},
            "response": {"properties": {"status_code": {"type": "integer"}, "bytes": {"type": "long"}, "mime_type": KW}},
            "version": KW,
        }},
        "url": {"properties": {"domain": KW, "path": KW, "query": KW}},
        "tls": {"properties": {"version": KW, "cipher": KW, "server_name": KW,
                                "established": {"type": "boolean"}}},
        "dns": {"properties": {
            "question": {"properties": {"name": KW, "type": KW}},
            "response_code": KW,
            "resolved_ip": {"type": "ip"},
        }},
    }
}

random.seed(42)

HOSTS = [f"fw-{dc}-{i:02d}" for dc in ("iad", "sfo", "fra", "sin") for i in range(1, 9)] + [
    f"core-rtr-{dc}-{i:02d}" for dc in ("iad", "sfo") for i in range(1, 5)
]
SERIALS = {h: f"SN{random.randint(10**9, 10**10-1)}" for h in HOSTS}
VENDORS = {
    "fw": ("Palo Alto Networks", "PA-5450", "firewall", "11.2.3"),
    "core": ("Cisco", "Nexus 9500", "router", "10.4(2)"),
}
ACTIONS = ["allow", "deny", "drop", "reset", "alert", "nat"]
ACTION_W = [55, 18, 10, 4, 8, 5]
LEVELS = ["info", "info", "info", "warning", "error", "critical"]
PROTOS = ["https", "dns", "http", "ssh", "smtp", "ntp", "bgp", "snmp"]
IANA = {"https": 6, "http": 6, "ssh": 6, "smtp": 6, "bgp": 6, "dns": 17, "ntp": 17, "snmp": 17}
RULES = [
    ("R-1001", "allow-corp-egress"), ("R-1002", "deny-inbound-default"),
    ("R-1044", "allow-dns-resolvers"), ("R-2001", "geo-block-highrisk"),
    ("R-2107", "ids-suspicious-tls"), ("R-3050", "rate-limit-ssh"),
]
RULE_UUIDS = {r[0]: str(uuid.UUID(int=random.getrandbits(128))) for r in RULES}
GEO = [("US", "Ashburn", 39.04, -77.49), ("US", "San Jose", 37.34, -121.89),
       ("DE", "Frankfurt", 50.11, 8.68), ("SG", "Singapore", 1.35, 103.82),
       ("GB", "London", 51.51, -0.13), ("BR", "Sao Paulo", -23.55, -46.63),
       ("IN", "Mumbai", 19.08, 72.88), ("JP", "Tokyo", 35.68, 139.69),
       ("NL", "Amsterdam", 52.37, 4.90), ("RU", "Moscow", 55.76, 37.62)]
DOMAINS = ["api.internal.corp", "auth.corp.example", "cdn.example.net",
           "updates.vendor.io", "telemetry.saas.app", "mail.corp.example"]
DNS_TLDS = ["corp.example", "example.net", "vendor.io", "saas.app", "cdn-edge.net"]
PATH_SEGS = ["api", "v1", "v2", "auth", "assets", "orders", "users", "health",
             "metrics", "sync", "batch", "export", "reports", "search"]
USERS = ["svc-backup", "svc-monitor", "jsmith", "mgarcia", "achen", "-"]
IFACES = ["ethernet1/1", "ethernet1/2", "ae0", "ae1", "vlan100", "vlan200"]
METHODS = ["GET", "GET", "GET", "POST", "PUT", "HEAD", "DELETE"]
STATUS = [200, 200, 200, 204, 301, 403, 404, 500, 502]
MIME = ["application/json", "text/html", "application/octet-stream", "image/png"]
TLSV = ["1.2", "1.3", "1.3", "1.3"]
CIPHERS = ["TLS_AES_128_GCM_SHA256", "TLS_AES_256_GCM_SHA384",
           "TLS_CHACHA20_POLY1305_SHA256", "ECDHE-RSA-AES128-GCM-SHA256"]
UAS = ["curl/8.4.0", "python-requests/2.32", "Go-http-client/2.0",
       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
       "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) Safari/605.1.15",
       "okhttp/4.12.0", "Java/17.0.9", "apache-httpclient/5.2"]
TAG_POOL = ["egress", "ingress", "pci", "prod", "staging", "vpn", "guest-net", "reviewed"]


def hexid(n):
    return "".join(random.choices("0123456789abcdef", k=n))


def gen_doc(ts_ms):
    host = random.choice(HOSTS)
    vend = VENDORS["fw" if host.startswith("fw") else "core"]
    action = random.choices(ACTIONS, weights=ACTION_W)[0]
    outcome = "success" if action in ("allow", "nat") else "failure"
    level = random.choice(LEVELS) if action != "allow" else "info"
    proto = random.choice(PROTOS)
    rule = random.choice(RULES)
    geo = random.choices(GEO, weights=[25, 15, 8, 8, 8, 6, 6, 6, 6, 6])[0]
    sip = f"10.{random.randint(0,63)}.{random.randint(0,255)}.{random.randint(1,254)}"
    dip = f"{random.randint(11,203)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
    sport = random.randint(1024, 65535)
    dport = {"https": 443, "dns": 53, "http": 80, "ssh": 22, "smtp": 25,
             "ntp": 123, "bgp": 179, "snmp": 161}[proto]
    sbytes = int(random.lognormvariate(6.8, 1.7))
    dbytes = int(random.lognormvariate(7.4, 1.9))
    nbytes = sbytes + dbytes
    spkts = max(1, sbytes // random.randint(400, 1400))
    dpkts = max(1, dbytes // random.randint(400, 1400))
    dur = random.randint(120, 90_000_000)
    sev = {"info": 6, "warning": 4, "error": 3, "critical": 2}[level]
    iface = random.choice(IFACES)
    user = random.choice(USERS)
    msg = (
        f"{vend[0]} {vend[1]} session {action.upper()} {proto} "
        f"{sip}:{sport} -> {dip}:{dport} rule={rule[1]} zone=untrust->trust "
        f"if={iface} bytes={nbytes} pkts={spkts+dpkts} user={user} "
        f"reason={'policy-match' if outcome == 'success' else 'policy-violation'}"
    )
    doc = {
        "@timestamp": ts_ms,
        "message": msg,
        "tags": random.sample(TAG_POOL, k=random.randint(1, 3)),
        "host": {"name": host},
        "observer": {"vendor": vend[0], "product": vend[1], "type": vend[2],
                     "name": host, "serial_number": SERIALS[host], "version": vend[3]},
        "log": {"level": level,
                "syslog": {"priority": 8 * 16 + sev, "facility": {"code": 16}}},
        "event": {"id": hexid(16), "action": action, "outcome": outcome,
                  "category": "network", "severity": sev, "duration": dur,
                  "risk_score": round(random.uniform(0, 100), 2),
                  "ingested": ts_ms + random.randint(400, 9000)},
        "session": {"id": hexid(12)},
        "source": {"ip": sip, "port": sport, "bytes": sbytes, "packets": spkts,
                   "geo": {"country_iso_code": "US", "city_name": "Ashburn",
                           "location": {"lat": 39.04 + random.uniform(-0.5, 0.5),
                                        "lon": -77.49 + random.uniform(-0.5, 0.5)}}},
        "destination": {"ip": dip, "port": dport, "bytes": dbytes, "packets": dpkts,
                        "geo": {"country_iso_code": geo[0]}},
        "network": {"protocol": proto,
                    "transport": "udp" if IANA[proto] == 17 else "tcp",
                    "bytes": nbytes, "packets": spkts + dpkts,
                    "direction": random.choice(["inbound", "outbound", "outbound"]),
                    "community_id": "1:" + hexid(27),
                    "iana_number": IANA[proto],
                    "vlan": {"id": random.choice([100, 200, 300, 410, 520])}},
        "rule": {"id": rule[0], "name": rule[1], "uuid": RULE_UUIDS[rule[0]]},
        "interface": {"name": iface, "alias": f"{host}-{iface.replace('/', '-')}"},
        "user": {"name": user, "domain": "CORP" if user != "-" else "-"},
    }
    if random.random() < 0.4:
        doc["trace"] = {"id": hexid(32)}
    if proto in ("http", "https"):
        reqb = int(random.lognormvariate(5.5, 1.2))
        doc["http"] = {"request": {"method": random.choice(METHODS), "bytes": reqb},
                       "response": {"status_code": random.choice(STATUS),
                                    "bytes": dbytes, "mime_type": random.choice(MIME)},
                       "version": random.choice(["1.1", "2", "2", "3"])}
        doc["url"] = {"domain": random.choice(DOMAINS),
                      "path": "/" + "/".join(random.sample(PATH_SEGS, k=random.randint(2, 4))),
                      "query": f"req={hexid(8)}" if random.random() < 0.3 else None}
        doc["user_agent"] = {"original": random.choice(UAS)}
        if doc["url"]["query"] is None:
            del doc["url"]["query"]
    if proto == "https":
        doc["tls"] = {"version": random.choice(TLSV), "cipher": random.choice(CIPHERS),
                      "server_name": random.choice(DOMAINS),
                      "established": random.random() < 0.97}
    if proto == "dns":
        doc["dns"] = {"question": {"name": f"{hexid(6)}.{random.choice(DNS_TLDS)}",
                                   "type": random.choice(["A", "A", "AAAA", "CNAME", "TXT"])},
                      "response_code": random.choices(["NOERROR", "NXDOMAIN", "SERVFAIL"], weights=[90, 8, 2])[0],
                      "resolved_ip": f"{random.randint(11,203)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"}
    if action == "nat":
        doc["source"]["nat"] = {"ip": f"198.51.100.{random.randint(1,254)}",
                                "port": random.randint(1024, 65535)}
    # size of the original event, measured before this field is added
    doc["event"]["original_size"] = len(json.dumps(doc, separators=(",", ":")).encode())
    return doc


def req(method, path, body=None, ndjson=False):
    url = EP + path
    data = None
    headers = dict(HEADERS)
    if body is not None:
        if ndjson:
            headers["Content-Type"] = "application/x-ndjson"
            headers["Content-Encoding"] = "gzip"
            data = gzip.compress(body.encode())
        else:
            data = json.dumps(body).encode()
    r = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(r, timeout=180) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "body": e.read().decode()[:500]}


def main():
    print("Recreating indices...")
    for name, settings in INDICES.items():
        req("DELETE", f"/{name}")
        out = req("PUT", f"/{name}", {"settings": settings, "mappings": MAPPING})
        print(f"  {name}: {out}")
        if "_http_error" in out:
            sys.exit(1)

    print(f"Generating {N_DOCS} docs...")
    start_ts = int(time.time() * 1000) - 24 * 3600 * 1000  # last 24h
    step = (24 * 3600 * 1000) // N_DOCS
    docs = [gen_doc(start_ts + i * step) for i in range(N_DOCS)]

    chunk = 5000
    for name in INDICES:
        t0 = time.time()
        for i in range(0, N_DOCS, chunk):
            lines = []
            for d in docs[i:i + chunk]:
                lines.append('{"create":{}}')
                lines.append(json.dumps(d))
            out = req("POST", f"/{name}/_bulk?refresh=false", "\n".join(lines) + "\n", ndjson=True)
            if out.get("errors") or "_http_error" in out:
                print(f"  BULK ERROR {name}@{i}: {str(out)[:400]}")
                sys.exit(1)
            if (i // chunk) % 6 == 0:
                print(f"  {name}: {min(i+chunk, N_DOCS)}/{N_DOCS}", flush=True)
        req("POST", f"/{name}/_refresh")
        print(f"  {name} done in {time.time()-t0:.1f}s")

    print("Sizes (may take 1-2 min to settle):")
    out = req("GET", "/_cat/indices/netops-cmp-standard,netops-cmp-logsdb,netops-cmp-columnar?format=json&bytes=b")
    for row in sorted(out, key=lambda r: r["index"]):
        print(f"  {row['index']}: docs={row['docs.count']} dataset.size={row.get('dataset.size')}")


if __name__ == "__main__":
    main()
