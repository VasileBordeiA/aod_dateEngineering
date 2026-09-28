"""The provisioned Grafana dashboards follow spec 05 (no Docker needed; queries are checked in embedded chDB)."""

import json
import re
from pathlib import Path

import pytest
import yaml
from test_clickhouse_ddl import statements

ROOT = Path(__file__).resolve().parents[2]
GRAFANA = ROOT / "infra" / "grafana"
PUBLIC = GRAFANA / "provisioning"
OPS = GRAFANA / "provisioning-ops"
LIVE = PUBLIC / "dashboards" / "nyc-taxi-live.json"
HEALTH = OPS / "dashboards" / "stand-health.json"
SPEC = ROOT / "docs" / "specs" / "05-grafana-dashboards.md"
SIM_COLUMNS = r"(?:pickup_ts|dropoff_ts|window_start)"
WALL_COLUMNS = r"(?:\w+_at|event_time)"


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def queries(dashboard: dict) -> list[tuple[str, str]]:
    return [(p["title"] or f"panel {p['id']}", t["rawSql"]) for p in dashboard["panels"] for t in p.get("targets", [])]


def all_queries() -> list[tuple[str, str]]:
    return [(f"{path.stem}: {title}", sql) for path in (LIVE, HEALTH) for title, sql in queries(load(path))]


def borough_palette() -> dict[str, str]:
    return dict(re.findall(r"^\| (Manhattan|Brooklyn|Queens|Bronx|Staten Island|EWR) \| `(#[0-9a-f]{6})` \|",
                           SPEC.read_text(), re.M))


@pytest.mark.parametrize("path, uid", [(LIVE, "nyc-taxi-live"), (HEALTH, "stand-health")])
def test_dashboard_settings(path, uid):
    d = load(path)
    assert (d["uid"], d["timezone"], d["refresh"], d["liveNow"], d["editable"]) == (uid, "utc", "5s", False, False)
    assert len({p["id"] for p in d["panels"]}) == len(d["panels"])


def test_big_screen_panel_budget():
    """Spec 05: no more than 11 panels. Panel 9 (rejected) is a stat + a bar gauge, so 12 query panels."""
    panels = load(LIVE)["panels"]
    assert len([p for p in panels if p.get("targets")]) <= 12
    assert all(p["gridPos"]["x"] + p["gridPos"]["w"] <= 24 for p in panels)
    assert max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in panels) <= 27, "must fit 1080 px in kiosk mode"


@pytest.mark.parametrize("name, sql", all_queries())
def test_no_grafana_time_macros(name, sql):
    """Sim data lives in 2025: the dashboard's wall-clock range must never filter it (spec 05)."""
    assert "$__" not in sql, name


@pytest.mark.parametrize("name, sql", all_queries())
def test_two_clocks_never_mixed(name, sql):
    """Sim columns compare against taxi.sim_now; wall columns (*_at) against now() / now64() (CLAUDE.md invariant 3)."""
    for rhs in re.findall(rf"\b{SIM_COLUMNS}\s*>=\s*([^\n]+)", sql):
        assert rhs.startswith("(SELECT now_ts FROM taxi.sim_now)"), f"{name}: sim column vs {rhs!r}"
    for rhs in re.findall(rf"\b{WALL_COLUMNS}\s*>=\s*([^\n]+)", sql):
        assert re.match(r"(now64\(3\)|now\(\)|toStartOfDay\(now64\(3\)\))", rhs), f"{name}: wall column vs {rhs!r}"


def test_public_dashboard_uses_only_the_public_datasource():
    text = "\n".join(p.read_text() for p in PUBLIC.rglob("*") if p.is_file())
    uids = set(re.findall(r'"uid": "([^"]+)"', LIVE.read_text())) - {"nyc-taxi-live"}
    assert uids == {"clickhouse"}
    for secret in ("agent_log", "query_log", "grafana_ops", "clickhouse-ops", "CLICKHOUSE_OPS"):
        assert secret not in text, f"{secret} must never reach the anonymous Grafana (ADR-017)"


def test_stand_health_uses_only_the_ops_datasource():
    uids = set(re.findall(r'"uid": "([^"]+)"', HEALTH.read_text())) - {"stand-health"}
    assert uids == {"clickhouse-ops"}
    ds = yaml.safe_load((OPS / "datasources" / "clickhouse-ops.yaml").read_text())["datasources"][0]
    assert (ds["uid"], ds["jsonData"]["username"]) == ("clickhouse-ops", "grafana_ops")


def borough_colours_used(dashboard: dict) -> list[tuple[str, str]]:
    """(borough, hex) for every series override and value mapping that names a borough."""
    used = []
    for p in dashboard["panels"]:
        for o in p.get("fieldConfig", {}).get("overrides", []):
            for prop in o["properties"]:
                if prop["id"] == "color" and o["matcher"]["id"] == "byName":
                    used.append((o["matcher"]["options"], prop["value"]["fixedColor"]))
                if prop["id"] == "mappings":
                    used += [(k, v["color"]) for m in prop["value"] for k, v in m["options"].items()]
    return used


def test_borough_colours_match_the_spec_palette():
    """One fixed colour per borough across all panels, shared with the web UI (spec 05 §Borough palette)."""
    palette = borough_palette()
    assert list(palette) == ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island", "EWR"]
    used = [(b, c) for b, c in borough_colours_used(load(LIVE)) if b in palette]
    assert {b for b, _ in used} >= {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"}
    assert all(palette[b] == c for b, c in used), used


def test_offline_no_external_urls():
    """No internet at the venue: no tile servers, no remote images (spec 05 acceptance 2)."""
    for path in (LIVE, HEALTH):
        assert not re.search(r"https?://", path.read_text()), path.name
    geomap = next(p for p in load(LIVE)["panels"] if p["type"] == "geomap")
    assert "basemap" not in geomap["options"], "the basemap comes from GF_GEOMAP_DEFAULT_BASELAYER_CONFIG"


def test_compose_keeps_stand_health_off_the_network():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    public, ops = compose["grafana"], compose["grafana-ops"]
    assert not any("provisioning-ops" in v for v in public["volumes"])
    assert ops["ports"] == ["127.0.0.1:3002:3000"]
    assert ops["environment"]["GF_AUTH_ANONYMOUS_ENABLED"] == "false"
    assert "CLICKHOUSE_OPS_PASSWORD" not in public["environment"]
    assert "grafana-ops" not in (ROOT / "infra" / "caddy" / "Caddyfile").read_text()
    baselayer = json.loads(public["environment"]["GF_GEOMAP_DEFAULT_BASELAYER_CONFIG"])
    assert baselayer["type"] == "geojson" and public["environment"]["GF_GEOMAP_ENABLE_CUSTOM_BASELAYERS"] == "false"


@pytest.fixture(scope="module")
def ch():
    from chdb import session
    s = session.Session()
    for stmt in statements():
        if "ENGINE = Kafka" in stmt or "trace_events_mv" in stmt:
            continue
        s.query(stmt)
    yield s
    s.close()


@pytest.mark.parametrize("name, sql", [(n, q) for n, q in all_queries() if "system.query_log" not in n + q])
def test_query_runs_against_the_schema(ch, name, sql):
    """Every panel query parses and binds against the real DDL (system.query_log: see test_stack.py)."""
    ch.query(sql, "TSV")
