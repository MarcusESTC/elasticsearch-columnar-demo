#!/usr/bin/env python3
"""Build and import Kibana dashboards using the ES|QL 'vis' panel schema
(modeled on this project's existing working dashboards)."""
import json
import subprocess
import uuid

import os

KB = os.environ.get("KIBANA_ENDPOINT", "").rstrip("/")
API_KEY = os.environ.get("ES_API_KEY", "")
if not KB or not API_KEY:
    raise SystemExit(
        "Missing configuration. Set both environment variables and retry:\n"
        '  export KIBANA_ENDPOINT="https://<your-project>.kb.<region>.elastic.cloud"\n'
        '  export ES_API_KEY="<base64 API key>"'
    )


def esql_panel(index, esql, columns, viz_type, viz_state, grid, title=""):
    """columns: list of (columnId, fieldName, metaType)."""
    dv_id = f"{index}-@timestamp"
    pid = str(uuid.uuid4())
    attrs = {
        "visualizationType": viz_type,
        "title": title,
        "references": [],
        "version": 2,
        "state": {
            "datasourceStates": {"textBased": {"layers": {"layer_0": {
                "index": dv_id,
                "query": {"esql": esql},
                "timeField": "@timestamp",
                "columns": [{"columnId": c, "fieldName": f, "label": f,
                             "customLabel": True, "meta": {"type": t}}
                            for c, f, t in columns],
                "ignoreGlobalFilters": False}}}},
            "internalReferences": [{"type": "index-pattern", "id": dv_id,
                                    "name": "indexpattern-datasource-layer-layer_0"}],
            "visualization": viz_state,
            "adHocDataViews": {dv_id: {"id": dv_id, "title": index, "name": index,
                                       "timeFieldName": "@timestamp", "sourceFilters": [],
                                       "allowNoIndex": False, "type": "esql"}},
            "query": {"esql": esql},
            "filters": [],
        },
    }
    return {"type": "vis", "panelIndex": pid,
            "gridData": {**grid, "i": pid},
            "embeddableConfig": {"attributes": attrs, "drilldowns": []},
            **({"title": title} if title else {})}


def metric(index, esql, field, grid, title):
    return esql_panel(index, esql, [("m", field, "number")], "lnsMetric",
                      {"layerId": "layer_0", "layerType": "data",
                       "metricAccessor": "m", "showBar": False, "density": "default"},
                      grid, title)


def xy(index, esql, cols, series, grid, title, x="t", val="c", split=None):
    layer = {"layerId": "layer_0", "seriesType": series, "xAccessor": x,
             "accessors": [val], "layerType": "data"}
    if split:
        layer["splitAccessor"] = split
    return esql_panel(index, esql, cols, "lnsXY",
                      {"legend": {"isVisible": True, "position": "right"},
                       "preferredSeriesType": series, "valueLabels": "hide",
                       "layers": [layer]},
                      grid, title)


def pie(index, esql, cols, grid, title):
    return esql_panel(index, esql, cols, "lnsPie",
                      {"shape": "donut",
                       "layers": [{"layerId": "layer_0", "primaryGroups": [cols[0][0]],
                                   "metrics": [cols[1][0]], "numberDisplay": "percent",
                                   "categoryDisplay": "default", "legendDisplay": "default",
                                   "nestedLegend": False, "layerType": "data"}]},
                      grid, title)


def markdown(content, grid):
    pid = str(uuid.uuid4())
    return {"type": "markdown", "panelIndex": pid, "gridData": {**grid, "i": pid},
            "embeddableConfig": {"content": content}}


def dashboard(dash_id, title, description, panels, time_from, time_to):
    return {"type": "dashboard", "id": dash_id,
            "typeMigrationVersion": "10.3.0", "coreMigrationVersion": "8.8.0",
            "attributes": {
                "title": title, "description": description,
                "panelsJSON": json.dumps(panels),
                "optionsJSON": json.dumps({"useMargins": True, "syncColors": False,
                                           "syncCursor": True, "syncTooltips": False,
                                           "hidePanelTitles": False}),
                "timeRestore": True, "timeFrom": time_from, "timeTo": time_to,
                "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
                    {"query": {"query": "", "language": "kuery"}, "filter": []})},
            },
            "references": []}


FW = "logs-netops.firewall-default"
LT = "netops-metrics-longterm"
TR = '| WHERE @timestamp >= ?_tstart AND @timestamp < ?_tend'


def main():
    d1 = dashboard(
        "netops-fw-overview", "NetOps — Firewall Overview (24h)",
        "Firewall log analytics on logs-netops.firewall-default — every panel is an ES|QL query.",
        [
            markdown("# 🔥 Firewall Overview\nLive ES|QL analytics over `logs-netops.firewall-default`. "
                     "Every panel is an ES|QL query — open a panel to show it.",
                     {"x": 0, "y": 0, "w": 48, "h": 3}),
            metric(FW, f"FROM {FW}\n{TR}\n| STATS `Total events` = COUNT(*)",
                   "Total events", {"x": 0, "y": 3, "w": 8, "h": 6}, "Total events"),
            metric(FW, f'FROM {FW}\n{TR}\n| WHERE event.action == "deny"\n| STATS `Denied` = COUNT(*)',
                   "Denied", {"x": 8, "y": 3, "w": 8, "h": 6}, "Denied sessions"),
            metric(FW, f'FROM {FW}\n{TR}\n| STATS `Ingest MB` = ROUND(SUM(event.original_size) / 1048576.0, 1)',
                   "Ingest MB", {"x": 16, "y": 3, "w": 8, "h": 6}, "Raw ingest (MB)"),
            pie(FW, f"FROM {FW}\n{TR}\n| STATS c = COUNT(*) BY rule.name",
                [("g", "rule.name", "string"), ("c", "c", "number")],
                {"x": 24, "y": 3, "w": 24, "h": 12}, "Events by firewall rule"),
            xy(FW, f"FROM {FW}\n{TR}\n| STATS c = COUNT(*) BY level = log.level, t = BUCKET(@timestamp, 1 hour)\n| SORT t",
               [("t", "t", "date"), ("level", "level", "string"), ("c", "c", "number")],
               "bar_stacked", {"x": 0, "y": 9, "w": 24, "h": 12}, "Log volume by severity",
               split="level"),
            xy(FW, f"FROM {FW}\n{TR}\n| STATS mb = ROUND(SUM(network.bytes) / 1048576.0, 1) BY t = BUCKET(@timestamp, 1 hour)\n| SORT t",
               [("t", "t", "date"), ("mb", "mb", "number")],
               "line", {"x": 0, "y": 21, "w": 24, "h": 10}, "Traffic volume (MB)", val="mb"),
            xy(FW, f"FROM {FW}\n{TR}\n| STATS c = COUNT(*) BY action = event.action, t = BUCKET(@timestamp, 1 hour)\n| SORT t",
               [("t", "t", "date"), ("action", "action", "string"), ("c", "c", "number")],
               "area_stacked", {"x": 24, "y": 21, "w": 24, "h": 10}, "Actions over time",
               split="action"),
        ],
        "now-24h", "now")

    d2 = dashboard(
        "netops-year-ahead", "NetOps — Year Ahead (capacity & incidents)",
        "13 months of hourly metrics + logs (through Aug 2027): growth trend, seasonality, incident surges every ~6 weeks.",
        [
            markdown("# 📅 Year Ahead — capacity & incidents\nTime range intentionally extends to **now+365d**. "
                     "Traffic grows ~35% over the year; incident surges hit a rotating firewall every ~6 weeks.",
                     {"x": 0, "y": 0, "w": 48, "h": 3}),
            xy(LT, f"FROM {LT}\n{TR}\n| STATS cpu = ROUND(AVG(cpu.utilization) * 100, 1) BY host = host.name, t = BUCKET(@timestamp, 1 week)\n| SORT t",
               [("t", "t", "date"), ("host", "host", "string"), ("cpu", "cpu", "number")],
               "line", {"x": 0, "y": 3, "w": 48, "h": 13}, "CPU % by firewall (weekly avg, 13 months)",
               val="cpu", split="host"),
            xy(LT, f"FROM {LT}\n{TR}\n| STATS mem = ROUND(AVG(memory.utilization) * 100, 1) BY t = BUCKET(@timestamp, 1 week)\n| SORT t",
               [("t", "t", "date"), ("mem", "mem", "number")],
               "area", {"x": 0, "y": 16, "w": 24, "h": 11}, "Fleet memory pressure (%)", val="mem"),
            xy(FW, f'FROM {FW}\n{TR}\n| WHERE log.level == "error"\n| STATS errors = COUNT(*) BY host = host.name, t = BUCKET(@timestamp, 1 week)\n| SORT t',
               [("t", "t", "date"), ("host", "host", "string"), ("errors", "errors", "number")],
               "bar_stacked", {"x": 24, "y": 16, "w": 24, "h": 11}, "Error bursts — incident windows",
               val="errors", split="host"),
        ],
        "now-30d", "now+365d")

    path = "/tmp/netops-dashboards.ndjson"
    with open(path, "w") as f:
        f.write("\n".join(json.dumps(d) for d in (d1, d2)) + "\n")
    out = subprocess.run(
        ["curl", "-s", "-X", "POST",
         "-H", f"Authorization: ApiKey {API_KEY}", "-H", "kbn-xsrf: true",
         KB + "/api/saved_objects/_import?overwrite=true",
         "-F", f"file=@{path};type=application/ndjson"],
        capture_output=True, text=True)
    print(out.stdout[:1200])


if __name__ == "__main__":
    main()
